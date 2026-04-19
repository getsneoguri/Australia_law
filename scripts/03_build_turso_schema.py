"""Turso libSQL 데이터베이스에 스키마를 생성한다.

libsql-experimental의 sqlite3 호환 API 사용.
conn.execute() + conn.commit() 패턴.
"""

from __future__ import annotations

import sys

from au_law_mcp.db.client import get_connection

_STATEMENTS = [
    "DROP TRIGGER IF EXISTS documents_ai",
    "DROP TRIGGER IF EXISTS documents_ad",
    "DROP TRIGGER IF EXISTS documents_au",
    "DROP INDEX IF EXISTS documents_jurisdiction_idx",
    "DROP INDEX IF EXISTS documents_type_idx",
    "DROP INDEX IF EXISTS documents_date_idx",
    "DROP TABLE IF EXISTS documents_fts",
    "DROP TABLE IF EXISTS documents",

    """CREATE TABLE documents (
        version_id    TEXT PRIMARY KEY,
        title         TEXT NOT NULL,
        jurisdiction  TEXT NOT NULL,
        doc_type      TEXT NOT NULL,
        date          TEXT,
        citation      TEXT,
        url           TEXT NOT NULL,
        source        TEXT,
        mime          TEXT,
        when_scraped  TEXT,
        text_snippet  TEXT
    )""",

    """CREATE VIRTUAL TABLE documents_fts USING fts5(
        title,
        citation,
        text_snippet,
        content='documents',
        content_rowid='rowid',
        tokenize='porter unicode61 remove_diacritics 2'
    )""",

    """CREATE TRIGGER documents_ai AFTER INSERT ON documents BEGIN
        INSERT INTO documents_fts(rowid, title, citation, text_snippet)
        VALUES (new.rowid, new.title, new.citation, new.text_snippet);
    END""",

    """CREATE TRIGGER documents_ad AFTER DELETE ON documents BEGIN
        INSERT INTO documents_fts(documents_fts, rowid, title, citation, text_snippet)
        VALUES ('delete', old.rowid, old.title, old.citation, old.text_snippet);
    END""",

    """CREATE TRIGGER documents_au AFTER UPDATE ON documents BEGIN
        INSERT INTO documents_fts(documents_fts, rowid, title, citation, text_snippet)
        VALUES ('delete', old.rowid, old.title, old.citation, old.text_snippet);
        INSERT INTO documents_fts(rowid, title, citation, text_snippet)
        VALUES (new.rowid, new.title, new.citation, new.text_snippet);
    END""",

    "CREATE INDEX documents_jurisdiction_idx ON documents(jurisdiction)",
    "CREATE INDEX documents_type_idx ON documents(doc_type)",
    "CREATE INDEX documents_date_idx ON documents(date)",
]


def main() -> int:
    print(f"[03] {len(_STATEMENTS)}개 SQL statement 실행. 대상: Turso 원격 DB.")

    with get_connection() as conn:
        for idx, stmt in enumerate(_STATEMENTS, 1):
            head = stmt.strip().split("\n")[0][:70]
            print(f"[03] [{idx}/{len(_STATEMENTS)}] {head}...")
            try:
                conn.execute(stmt)
                conn.commit()
            except Exception as e:
                print(f"[03] 오류: {e}", file=sys.stderr)
                return 2

    print("[03] 스키마 생성 완료.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
