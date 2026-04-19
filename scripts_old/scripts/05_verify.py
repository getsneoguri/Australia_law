"""적재 후 Turso DB의 무결성과 FTS5 쿼리 동작을 검증한다.

Phase 1 검증 항목:
    1. 행 수 확인 (documents ↔ documents_fts 동기화)
    2. jurisdiction 분포
    3. doc_type 분포
    4. FTS5 검색 스모크 테스트
"""

from __future__ import annotations

import sys

from au_law_mcp.db.client import get_client


def header(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def main() -> int:
    all_ok = True

    with get_client() as client:
        # 1. 행 수
        header("1. 행 수 확인")
        doc_count = client.execute("SELECT count(*) FROM documents").rows[0][0]
        fts_count = client.execute("SELECT count(*) FROM documents_fts").rows[0][0]
        print(f"  documents     : {doc_count:,}")
        print(f"  documents_fts : {fts_count:,}")

        if doc_count == 0:
            print("  ✗ documents가 비어 있음!")
            return 1
        if doc_count != fts_count:
            print(f"  ✗ FTS5 동기화 불일치 (차이 {doc_count - fts_count:,})")
            all_ok = False
        else:
            print("  ✓ 동기화 정상")

        # 2. jurisdiction 분포
        header("2. Jurisdiction 분포")
        rows = client.execute("""
            SELECT jurisdiction, count(*) AS n
            FROM documents GROUP BY jurisdiction ORDER BY n DESC
        """).rows
        for row in rows:
            print(f"  {str(row[0]):30s} {row[1]:>10,}")

        # 3. doc_type 분포
        header("3. 문서 유형 분포")
        rows = client.execute("""
            SELECT doc_type, count(*) AS n
            FROM documents GROUP BY doc_type ORDER BY n DESC
        """).rows
        for row in rows:
            print(f"  {str(row[0]):30s} {row[1]:>10,}")

        # 4. FTS5 검색 스모크 테스트
        header("4. FTS5 검색 테스트")
        queries = ["patent", "negligence", "privacy", "contract", "criminal"]

        for q in queries:
            rs = client.execute("""
                SELECT d.title, d.jurisdiction
                FROM documents_fts
                JOIN documents d ON d.rowid = documents_fts.rowid
                WHERE documents_fts MATCH ?
                LIMIT 3
            """, [q])
            print(f"\n  쿼리: '{q}' → {len(rs.rows)}건")
            if not rs.rows:
                print("    ✗ 결과 없음")
                all_ok = False
            for row in rs.rows:
                title = (str(row[0]) or "")[:55]
                jur = row[1]
                print(f"    - [{jur}] {title}")

    header("결과")
    if all_ok:
        print("  ✓ 모든 검증 통과. Phase 1 완료!")
        print("  다음 단계: Phase 2 (MCP 서버 도구 구현)")
        return 0
    else:
        print("  ✗ 일부 실패. 위 로그를 확인하세요.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
