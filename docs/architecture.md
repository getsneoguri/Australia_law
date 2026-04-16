# Architecture

## 설계 원칙

1. **Render Free 티어 제약에 맞춘다**: RAM 512MB, 디스크 ephemeral. 대용량 데이터는 서버에 두지 않는다.
2. **Turso Free 티어 제약에 맞춘다**: 5GB 스토리지, 월 10M writes, 월 500M reads.
3. **풀텍스트는 HuggingFace에서 on-demand**: Turso에는 메타데이터 + 검색 인덱스만.
4. **기존 리포 패턴 재사용**: `mcp_kipris`, `korean-law-mcp`, `epo-ops-mcp-server`의 구조를 기반으로 확장.

## 데이터 흐름

### 빌드 타임 (로컬, 1회 + 월간 증분)

```
HuggingFace Hub
  ├─ isaacus/open-australian-legal-embeddings (~8GB, 5.2M chunks)
  └─ isaacus/open-australian-legal-corpus (~6GB, 232K docs)
        │
        ▼
scripts/01_download_embeddings.py     → data/embeddings.parquet
scripts/02_aggregate_doc_vectors.py   → data/doc_vectors.npy  (232K × 384 F32)
scripts/03_download_corpus_snippets.py → data/doc_snippets.parquet
scripts/04_build_turso_schema.py      → Turso에 테이블·인덱스 생성
scripts/05_bulk_load.py               → Turso에 232K 문서 upsert
scripts/06_verify_counts.py           → 행 수 및 쿼리 검증
```

### 런타임 (Render에서 실행, 상시)

```
Claude client
  │  (MCP stdio 또는 HTTP)
  ▼
MCP Server (FastAPI on Render)
  ├─ /health                           → UptimeRobot ping
  ├─ /mcp  (HTTP MCP transport)
  │     ├─ tool: search_legislation    → Turso FTS5 MATCH
  │     ├─ tool: semantic_search       → Turso vector_top_k
  │     ├─ tool: get_document          → Turso meta + HF fetch
  │     └─ ...
  ▼
Turso libSQL (메타데이터 + FTS5 + 벡터 인덱스)
  │
  (get_document 호출 시)
  ▼
HuggingFace Datasets API (풀텍스트 on-demand)
  │
  └─ 세션 캐시: Render /tmp (ephemeral)
```

## Turso 스키마 요약

자세한 DDL은 `src/au_law_mcp/db/schema.sql` 참조.

```sql
-- 주 문서 테이블
CREATE TABLE documents (
    version_id       TEXT PRIMARY KEY,  -- HF 데이터셋의 고유 ID
    title            TEXT NOT NULL,
    jurisdiction     TEXT NOT NULL,     -- commonwealth | new_south_wales | queensland | ...
    doc_type         TEXT NOT NULL,     -- primary_legislation | secondary_legislation | decision | bill
    date             TEXT,              -- ISO 8601
    citation         TEXT,
    url              TEXT NOT NULL,     -- 원본 URL
    source           TEXT,              -- 데이터 원 제공기관
    mime             TEXT,
    when_scraped     TEXT,
    text_snippet     TEXT,              -- 원문의 첫 2000자
    embedding        F32_BLOB(384)      -- bge-small-en-v1.5 평균 풀링된 문서 벡터
);

-- FTS5 가상 테이블: title, citation, text_snippet에 대한 전문 검색
CREATE VIRTUAL TABLE documents_fts USING fts5(
    title, citation, text_snippet,
    content='documents',
    content_rowid='rowid',
    tokenize='porter unicode61 remove_diacritics 2'
);

-- 벡터 인덱스 (DiskANN)
CREATE INDEX documents_embedding_idx
ON documents(libsql_vector_idx(embedding, 'metric=cosine'));

-- 자주 쓰는 필터 조건용 일반 인덱스
CREATE INDEX documents_jurisdiction_idx ON documents(jurisdiction);
CREATE INDEX documents_type_idx ON documents(doc_type);
CREATE INDEX documents_date_idx ON documents(date);
```

## 용량 예산 (Turso Free 5GB)

| 항목 | 추정 크기 |
|---|---|
| `documents` 메타데이터 (232K × ~500B) | 120MB |
| `documents.text_snippet` (232K × ~2000B) | 450MB |
| `documents.embedding` F32 (232K × 1536B) | 350MB |
| `documents_fts` FTS5 인덱스 | 250MB |
| 벡터 인덱스 shadow tables | 300MB |
| 기타 일반 인덱스 | 50MB |
| **소계** | **약 1.5GB** |

Free 티어 5GB 대비 약 30% 사용. Phase 2에서 chunk-level 벡터 5.2M행(F1BIT 250MB, 인덱스 ~500MB) 추가하더라도 여유.

## Writes 예산 (Turso Free 10M/월)

| 작업 | writes |
|---|---|
| `documents` 일괄 insert | 232K |
| FTS5 trigger로 인한 `documents_fts` 동기화 | 232K |
| 벡터 인덱스 내부 insert | ~232K |
| **초기 적재 합계** | **~700K** |

월 한도의 7% 사용. Developer 플랜 업그레이드 불필요.

## Reads 예산 (Turso Free 500M/월)

- FTS5 `MATCH` 쿼리: 스캔 크기는 인덱스에 의해 압축됨. 10K 행 스캔 가정 시 1쿼리당 ~10K reads.
- `vector_top_k`: 후보 K개만 스캔. 1쿼리당 ~K×2 reads.
- 1000 쿼리/일 기준 월 ~30M reads. 한도의 6%.

## MCP 서버 메모리 예산 (Render Free 512MB)

| 항목 | 메모리 |
|---|---|
| Python 런타임 + FastAPI | ~80MB |
| libsql-client | ~20MB |
| sentence-transformers (bge-small-en-v1.5, 쿼리 임베딩용) | ~130MB |
| HuggingFace datasets streaming 버퍼 | ~50MB |
| 여유 | ~232MB |

`sentence-transformers` 모델을 lazy load하여 첫 `semantic_search` 호출 시에만 메모리 점유.

## 콜드 스타트 대응

- Render Web Service는 15분 무요청 시 sleep. 재기동 ~30초 (Python + 모델 로드 포함).
- UptimeRobot에서 5분 간격으로 `/health` GET 핑. 무한 sleep 방지.
- `/health` 핸들러는 DB나 모델을 건드리지 않는 경량 200 OK 응답.

## 라이선스 준수

- `get_document` 응답에는 반드시 `license` 및 `source_url` 포함.
- Corpus의 개별 문서 라이선스는 `source` 필드로 구분되므로 향후 문서별 라이선스 필드 추가 고려 (Phase 2).

## 향후 확장 (Phase 2+)

1. **chunk-level 벡터 추가**: 5.2M chunks × F1BIT 벡터, 조문 단위 정밀 의미 검색.
2. **Queensland API 실시간 조회**: 최신성이 중요한 쿼리에만 fallback.
3. **한-호 법령 비교 도구**: `korean-law-mcp`와 연동하는 크로스-MCP 워크플로우.
4. **증분 업데이트 자동화**: 월 1회 cron으로 HF 최신 버전 diff 반영.
