-- ============================================================================
-- Australia Law MCP — Turso libSQL 스키마
-- ============================================================================
-- 이 파일은 scripts/04_build_turso_schema.py에서 순서대로 실행된다.
-- 각 statement는 세미콜론으로 종결. DROP은 idempotent 재실행을 위해 포함.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 기존 객체 제거 (재실행 안전)
-- ----------------------------------------------------------------------------
DROP TRIGGER IF EXISTS documents_ai;
DROP TRIGGER IF EXISTS documents_ad;
DROP TRIGGER IF EXISTS documents_au;
DROP INDEX IF EXISTS documents_embedding_idx;
DROP INDEX IF EXISTS documents_jurisdiction_idx;
DROP INDEX IF EXISTS documents_type_idx;
DROP INDEX IF EXISTS documents_date_idx;
DROP TABLE IF EXISTS documents_fts;
DROP TABLE IF EXISTS documents;

-- ----------------------------------------------------------------------------
-- documents: 주 문서 테이블
-- ----------------------------------------------------------------------------
-- version_id: HuggingFace 데이터셋의 각 문서 고유 키. URL-like 문자열.
-- jurisdiction: commonwealth, new_south_wales, queensland, western_australia,
--               south_australia, tasmania, norfolk_island 등 소문자 snake_case.
-- doc_type: primary_legislation, secondary_legislation, decision, bill 등.
-- text_snippet: 원문의 첫 2000자. FTS5 인덱싱용. 풀텍스트는 별도 fetch.
-- embedding: bge-small-en-v1.5 (384-dim F32) — 문서 내 모든 chunk의 평균 풀링.
-- ----------------------------------------------------------------------------
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
    text_snippet  TEXT,
    embedding     F32_BLOB(384)
);

-- ----------------------------------------------------------------------------
-- FTS5 가상 테이블: 전문 검색용
-- ----------------------------------------------------------------------------
-- external content 방식: 실 데이터는 documents에 두고, FTS5는 rowid로 참조.
-- tokenize: porter stemmer + unicode61 + 악센트 제거. 영어 법률 텍스트에 적합.
-- ----------------------------------------------------------------------------
CREATE VIRTUAL TABLE documents_fts USING fts5(
    title,
    citation,
    text_snippet,
    content='documents',
    content_rowid='rowid',
    tokenize='porter unicode61 remove_diacritics 2'
);

-- ----------------------------------------------------------------------------
-- FTS5 동기화 트리거
-- ----------------------------------------------------------------------------
-- documents 테이블의 INSERT/DELETE/UPDATE에 맞춰 FTS5를 자동 갱신.
-- content_rowid='rowid' 방식에서 반드시 필요.
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 벡터 인덱스 (DiskANN ANN)
-- ----------------------------------------------------------------------------
-- metric=cosine: 정규화되지 않은 벡터도 안전하게 비교.
-- 쿼리는 vector_top_k('documents_embedding_idx', vector('[...]'), K) 형태.
-- ----------------------------------------------------------------------------
CREATE INDEX documents_embedding_idx
ON documents(libsql_vector_idx(embedding, 'metric=cosine'));

-- ----------------------------------------------------------------------------
-- 필터 조건 가속용 일반 인덱스
-- ----------------------------------------------------------------------------
-- jurisdiction + type으로 필터하는 filter_by_jurisdiction 도구에서 활용.
-- date 인덱스는 판례 검색의 날짜 범위 필터에 활용.
-- ----------------------------------------------------------------------------
CREATE INDEX documents_jurisdiction_idx ON documents(jurisdiction);
CREATE INDEX documents_type_idx ON documents(doc_type);
CREATE INDEX documents_date_idx ON documents(date);
