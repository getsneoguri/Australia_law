"""Turso libSQL 데이터베이스에 스키마(테이블, FTS5, 벡터 인덱스)를 생성한다.

src/au_law_mcp/db/schema.sql 파일의 내용을 세미콜론 기준으로 분리하여
순차 실행한다. schema.sql은 idempotent하게 작성되어 있으므로 재실행 안전.

이 스크립트는 **기존 데이터를 전부 제거**한다. 이미 적재된 상태에서
실행하면 documents 테이블이 DROP되므로 주의.
"""

from __future__ import annotations

import sys
from pathlib import Path

from au_law_mcp.db.client import get_client

_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "src" / "au_law_mcp" / "db" / "schema.sql"


def split_statements(sql: str) -> list[str]:
    """간단한 세미콜론 기반 statement 분리.

    schema.sql에는 문자열 리터럴이 없으므로 세미콜론 분리만으로 충분하다.
    주석(--) 줄은 각 statement 안에 섞여 있어도 sqlite가 무시하므로 그대로 둔다.
    """
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def main() -> int:
    if not _SCHEMA_PATH.exists():
        print(f"[04] 오류: 스키마 파일을 찾을 수 없습니다: {_SCHEMA_PATH}", file=sys.stderr)
        return 1

    sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    statements = split_statements(sql)

    print(f"[04] {len(statements)}개 statement를 실행합니다. 대상: Turso 원격 DB.")
    print("[04] 주의: 기존 documents/documents_fts 테이블이 있으면 제거됩니다.")

    with get_client() as client:
        for idx, stmt in enumerate(statements, 1):
            # DDL은 CREATE/DROP이 대부분이므로 간단 로깅만.
            head = stmt.splitlines()[0][:80]
            print(f"[04] [{idx}/{len(statements)}] {head}...")
            try:
                client.execute(stmt)
            except Exception as e:
                print(f"[04] 오류 발생 on statement {idx}: {e}", file=sys.stderr)
                print(f"[04] 문제의 SQL:\n{stmt}\n", file=sys.stderr)
                return 2

    print("[04] 스키마 생성 완료.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
