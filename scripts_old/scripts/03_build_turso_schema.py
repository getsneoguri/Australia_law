"""Turso libSQL 데이터베이스에 스키마를 생성한다.

Phase 1에서는 벡터 인덱스 없이 FTS5 전문검색만 제공한다.
벡터 의미 검색은 Phase 2에서 embedding 컬럼과 인덱스를 추가한다.
"""

from __future__ import annotations

import sys

from au_law_mcp.db.client import get_client

# Phase 1 스키마: 벡터 컬럼 없는 간소화 버전
_STATEMENTS = [
    # 기존 객체 제거 (재실행 안전)
    "DROP TRIGGER IF EXISTS documents_ai",
    "DROP TRIGGER IF EXISTS documents_ad",
    "DROP TRIGGER IF EXISTS documents_au",
    "DROP INDEX IF EXISTS documents_jurisdiction_idx",
    "DROP INDEX IF EXISTS documents_type_idx",
    "DROP INDEX IF EXISTS documents_date_idx",
    "DROP TABLE IF EXISTS documents_fts",
    "DROP TABLE IF EXISTS documents",

    # 문서 테이블
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

    # FTS5 전문검색 가상 테이블
    """CREATE VIRTUAL TABLE documents_fts USING fts5(
        title,
        citation,
        text_snippet,
        content='documents',
        content_rowid='rowid',
        tokenize='porter unicode61 remove_diacritics 2'
    )""",

    # FTS5 동기화 트리거: INSERT
    """CREATE TRIGGER documents_ai AFTER INSERT ON documents BEGIN
        INSERT INTO documents_fts(rowid, title, citation, text_snippet)
        VALUES (new.rowid, new.title, new.citation, new.text_snippet);
    END""",

    # FTS5 동기화 트리거: DELETE
    """CREATE TRIGGER documents_ad AFTER DELETE ON documents BEGIN
        INSERT INTO documents_fts(documents_fts, rowid, title, citation, text_snippet)
        VALUES ('delete', old.rowid, old.title, old.citation, old.text_snippet);
    END""",

    # FTS5 동기화 트리거: UPDATE
    """CREATE TRIGGER documents_au AFTER UPDATE ON documents BEGIN
        INSERT INTO documents_fts(documents_fts, rowid, title, citation, text_snippet)
        VALUES ('delete', old.rowid, old.title, old.citation, old.text_snippet);
        INSERT INTO documents_fts(rowid, title, citation, text_snippet)
        VALUES (new.rowid, new.title, new.citation, new.text_snippet);
    END""",

    # 필터 인덱스
    "CREATE INDEX documents_jurisdiction_idx ON documents(jurisdiction)",
    "CREATE INDEX documents_type_idx ON documents(doc_type)",
    "CREATE INDEX documents_date_idx ON documents(date)",
]


def main() -> int:
    print(f"[03] {len(_STATEMENTS)}개 SQL statement 실행. 대상: Turso 원격 DB.")
    print("[03] 주의: 기존 documents/documents_fts 테이블이 제거됩니다.")

    with get_client() as client:
        for idx, stmt in enumerate(_STATEMENTS, 1):
            head = stmt.strip().split("\n")[0][:70]
            print(f"[03] [{idx}/{len(_STATEMENTS)}] {head}...")
            try:
                client.execute(stmt)
            except Exception as e:
                print(f"[03] 오류: {e}", file=sys.stderr)
                print(f"[03] SQL:\n{stmt}\n", file=sys.stderr)
                return 2

    print("[03] 스키마 생성 완료.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
