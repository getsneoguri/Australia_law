-- ============================================================================
-- Australia Law MCP — Turso libSQL 스키마 (Phase 1: FTS5 전용)
-- ============================================================================
-- Phase 1: 벡터 인덱스 없이 FTS5 전문검색만 제공
-- Phase 2에서 embedding F32_BLOB(384) 컬럼 + DiskANN 인덱스 추가 예정
-- ============================================================================

DROP TRIGGER IF EXISTS documents_ai;
DROP TRIGGER IF EXISTS documents_ad;
DROP TRIGGER IF EXISTS documents_au;
DROP INDEX IF EXISTS documents_jurisdiction_idx;
DROP INDEX IF EXISTS documents_type_idx;
DROP INDEX IF EXISTS documents_date_idx;
DROP TABLE IF EXISTS documents_fts;
DROP TABLE IF EXISTS documents;

CREATE TABLE documents (
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
);

CREATE VIRTUAL TABLE documents_fts USING fts5(
    title,
    citation,
    text_snippet,
    content='documents',
    content_rowid='rowid',
    tokenize='porter unicode61 remove_diacritics 2'
);

CREATE TRIGGER documents_ai AFTER INSERT ON documents BEGIN
    INSERT INTO documents_fts(rowid, title, citation, text_snippet)
    VALUES (new.rowid, new.title, new.citation, new.text_snippet);
END;

CREATE TRIGGER documents_ad AFTER DELETE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, title, citation, text_snippet)
    VALUES ('delete', old.rowid, old.title, old.citation, old.text_snippet);
END;

CREATE TRIGGER documents_au AFTER UPDATE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, title, citation, text_snippet)
    VALUES ('delete', old.rowid, old.title, old.citation, old.text_snippet);
    INSERT INTO documents_fts(rowid, title, citation, text_snippet)
    VALUES (new.rowid, new.title, new.citation, new.text_snippet);
END;

CREATE INDEX documents_jurisdiction_idx ON documents(jurisdiction);
CREATE INDEX documents_type_idx ON documents(doc_type);
CREATE INDEX documents_date_idx ON documents(date);
