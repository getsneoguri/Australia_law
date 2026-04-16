"""적재 후 Turso DB의 무결성과 쿼리 동작을 검증한다.

실행 항목:
    1. 행 수 확인 (documents, documents_fts가 동기화되었는지)
    2. jurisdiction별 분포 집계
    3. FTS5 검색 스모크 테스트 ('patent', 'negligence', 'privacy')
    4. 벡터 유사도 검색 스모크 테스트 (임의 문서의 벡터로 top-5 조회)
    5. 스토리지 추정치 출력

이 스크립트는 읽기만 하므로 writes 예산에 영향 없음.
"""

from __future__ import annotations

import sys
from typing import Any

from au_law_mcp.db.client import get_client, vector_to_sqlite_literal


def header(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print('=' * 70)


def run_query(client, sql: str, args: list[Any] | None = None) -> list[tuple]:
    """쿼리 실행 후 rows를 파이썬 튜플 리스트로 반환."""
    rs = client.execute(sql, args or [])
    return [tuple(row) for row in rs.rows]


def check_row_counts(client) -> bool:
    """documents와 documents_fts의 행 수가 동기화되었는지."""
    header("1. 행 수 확인")
    doc_count = run_query(client, "SELECT count(*) FROM documents")[0][0]
    fts_count = run_query(client, "SELECT count(*) FROM documents_fts")[0][0]
    print(f"  documents     : {doc_count:,}")
    print(f"  documents_fts : {fts_count:,}")

    if doc_count == 0:
        print("  ✗ documents 테이블이 비어 있습니다. 적재가 완료되지 않았습니다.")
        return False

    if doc_count != fts_count:
        print(f"  ✗ FTS5 동기화 불일치 (차이 {doc_count - fts_count:,}).")
        print("    트리거가 누락되었거나 일부 행이 FTS에 반영되지 않았습니다.")
        return False

    print("  ✓ 동기화 정상")
    return True


def check_jurisdiction_distribution(client) -> None:
    """관할별 문서 수. 예상: Commonwealth 비중이 가장 높음."""
    header("2. Jurisdiction 분포")
    rows = run_query(client, """
        SELECT jurisdiction, count(*) AS n
        FROM documents
        GROUP BY jurisdiction
        ORDER BY n DESC
    """)
    for jur, n in rows:
        print(f"  {jur:30s} {n:>10,}")


def check_doc_type_distribution(client) -> None:
    """문서 유형별 분포."""
    header("3. 문서 유형 분포")
    rows = run_query(client, """
        SELECT doc_type, count(*) AS n
        FROM documents
        GROUP BY doc_type
        ORDER BY n DESC
    """)
    for dt, n in rows:
        print(f"  {dt:30s} {n:>10,}")


def smoke_test_fts5(client) -> bool:
    """FTS5 MATCH 쿼리 동작 확인."""
    header("4. FTS5 검색 스모크 테스트")
    queries = ["patent", "negligence", "privacy"]
    all_ok = True
    for q in queries:
        rows = run_query(client, """
            SELECT d.title, d.jurisdiction, d.doc_type
            FROM documents_fts
            JOIN documents d ON d.rowid = documents_fts.rowid
            WHERE documents_fts MATCH ?
            LIMIT 3
        """, [q])
        print(f"\n  쿼리: {q!r} → {len(rows)}건 (상위 3건 표시)")
        if not rows:
            print("    ✗ 결과 없음 — FTS5 인덱싱이 제대로 되지 않았을 수 있습니다.")
            all_ok = False
        for title, jur, dt in rows:
            title_trim = (title or "")[:60]
            print(f"    - [{jur}/{dt}] {title_trim}")
    return all_ok


def smoke_test_vector_search(client) -> bool:
    """벡터 유사도 검색 동작 확인.

    임의의 문서 벡터를 꺼내어 그 벡터로 top-5 쿼리 → 자기 자신이 1등이어야 함.
    """
    header("5. 벡터 유사도 검색 스모크 테스트")

    # 첫 문서의 version_id와 embedding 확보.
    # libSQL은 F32_BLOB을 JSON 배열 문자열로 반환하지 않으므로,
    # vector_extract 함수를 사용해 텍스트로 꺼낸다.
    rows = run_query(client, """
        SELECT version_id, title, vector_extract(embedding)
        FROM documents
        WHERE embedding IS NOT NULL
        LIMIT 1
    """)
    if not rows:
        print("  ✗ 벡터가 적재된 문서가 없습니다.")
        return False

    seed_vid, seed_title, seed_vec_text = rows[0]
    print(f"  시드 문서: {seed_vid}")
    print(f"  제목     : {(seed_title or '')[:70]}")

    # vector_extract는 "[x, y, z, ...]" 형식 문자열을 반환한다.
    # 그대로 vector() 입력으로 재사용 가능.
    top_rows = run_query(client, """
        SELECT d.version_id, d.title, vector_distance_cos(d.embedding, vector(?))
        FROM vector_top_k('documents_embedding_idx', vector(?), 5) AS vt
        JOIN documents d ON d.rowid = vt.id
        ORDER BY vector_distance_cos(d.embedding, vector(?)) ASC
    """, [seed_vec_text, seed_vec_text, seed_vec_text])

    print(f"\n  Top 5 (거리 오름차순):")
    for vid, title, dist in top_rows:
        marker = " ← 시드" if vid == seed_vid else ""
        print(f"    dist={dist:.4f}  {(title or '')[:50]}{marker}")

    if not top_rows:
        print("  ✗ 벡터 인덱스 쿼리가 결과를 반환하지 않음.")
        return False

    # 자기 자신(또는 거의 0인 거리)이 맨 위에 있어야 함.
    top_vid, _, top_dist = top_rows[0]
    if top_vid != seed_vid and top_dist > 0.01:
        print(f"  ✗ 시드 문서가 top-1이 아님. 인덱스 이상 가능.")
        return False

    print("  ✓ 벡터 인덱스 정상 동작")
    return True


def print_storage_hint(client) -> None:
    """실제 스토리지 사용량은 Turso 대시보드에서 확인. 여기서는 행 수 기반 추정치 출력."""
    header("6. 스토리지 추정")
    doc_count = run_query(client, "SELECT count(*) FROM documents")[0][0]
    avg_row_bytes = 500 + 2000 + 1536  # 메타 + 스니펫 + F32 벡터
    estimated_mb = (doc_count * avg_row_bytes) / 1_048_576
    print(f"  documents 추정 크기: 약 {estimated_mb:.0f}MB")
    print(f"  실제 사용량은 Turso 대시보드에서 확인: https://app.turso.tech")


def main() -> int:
    all_ok = True
    with get_client() as client:
        if not check_row_counts(client):
            all_ok = False
        check_jurisdiction_distribution(client)
        check_doc_type_distribution(client)
        if not smoke_test_fts5(client):
            all_ok = False
        if not smoke_test_vector_search(client):
            all_ok = False
        print_storage_hint(client)

    header("결과")
    if all_ok:
        print("  ✓ 모든 검증 통과. Phase 1 완료.")
        print("  다음 단계: Phase 2 (MCP 서버 도구 구현).")
        return 0
    else:
        print("  ✗ 일부 검증 실패. 위 로그를 확인하세요.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
