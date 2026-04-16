# Australia Law MCP

오스트레일리아 연방·주·특별지역 법령 및 판례를 MCP(Model Context Protocol) 도구로 노출하는 서버.

## 데이터 소스

- **Open Australian Legal Corpus** (`isaacus/open-australian-legal-corpus`, HuggingFace) — 232,560 documents, 1.47B tokens. Commonwealth, NSW, QLD, WA, SA, TAS, Norfolk Island의 현행 법령 + 법안 + 법원·심판 결정.
- **Open Australian Legal Embeddings** (`isaacus/open-australian-legal-embeddings`) — BAAI/bge-small-en-v1.5로 계산된 5.2M 384-dim 벡터.
- 라이선스: Open Australian Legal Corpus 라이선스(대부분 상업 이용 허용). 문서별 `licence` 필드 별도 확인.

## 아키텍처 개요

```
HuggingFace 데이터셋 (corpus + embeddings)
        ↓   (빌드 파이프라인, 로컬 1회 실행)
Turso libSQL (메타데이터 + text_snippet + FTS5 + 벡터 인덱스, 약 900MB)
        ↓
MCP 서버 (Python, Render Free 티어)  ←  UptimeRobot ping (/health)
        ↓
Claude Desktop / VS Code / 웹
```

풀텍스트는 Turso에 저장하지 않고 HuggingFace Hub에서 on-demand fetch. 상세: `docs/architecture.md`.

## 사전 준비

1. **Turso 계정** (Free 티어 5GB로 충분)
   ```bash
   curl -sSfL https://get.tur.so/install.sh | bash
   turso auth signup
   turso db create au-law-mcp --group default
   turso db show au-law-mcp --url         # → TURSO_DATABASE_URL
   turso db tokens create au-law-mcp      # → TURSO_AUTH_TOKEN
   ```

2. **Python 3.11+**, `uv` 권장
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **HuggingFace 토큰** (datasets 다운로드 속도 향상용, 선택)
   - https://huggingface.co/settings/tokens

## 설치 및 빌드

```bash
git clone https://github.com/getsneoguri/Australia_law.git
cd Australia_law

# 의존성 설치
uv sync  # 또는: pip install -r requirements.txt

# 환경변수 설정
cp .env.example .env
# .env 파일을 열어 TURSO_DATABASE_URL, TURSO_AUTH_TOKEN, HF_TOKEN 입력
```

### 초기 데이터 적재 (1회, 약 2~4시간 소요)

```bash
# 로컬 작업 디렉터리에 embeddings 다운로드 (약 8GB)
uv run scripts/01_download_embeddings.py

# 청크 벡터들을 문서 단위로 평균 풀링 (232K 문서 × F32 384-dim)
uv run scripts/02_aggregate_doc_vectors.py

# 원문 corpus에서 text_snippet (첫 2000자) 추출
uv run scripts/03_download_corpus_snippets.py

# Turso 스키마 생성
uv run scripts/04_build_turso_schema.py

# 232K 문서 일괄 적재 (진행률 표시)
uv run scripts/05_bulk_load.py

# 적재 결과 검증
uv run scripts/06_verify_counts.py
```

## MCP 서버 실행 (Phase 2 이후)

Phase 2에서 구현 예정. `src/au_law_mcp/server.py`를 엔트리포인트로 사용하며, Render에 배포합니다.

## 제공 도구 (예정)

| 도구 | 설명 |
|---|---|
| `search_legislation` | FTS5 기반 키워드·Boolean 검색 (title, citation, snippet) |
| `search_case_law` | 판례 검색 (법원·날짜 필터) |
| `semantic_search` | 문서 단위 의미 검색 (cosine similarity) |
| `get_document` | Turso에서 메타데이터 + HuggingFace에서 풀텍스트 on-demand fetch |
| `extract_section` | 특정 Act의 특정 조문 추출 |
| `filter_by_jurisdiction` | 관할·문서 유형별 브라우징 |

## 제한사항

- **데이터 신선도**: Corpus는 주기적 스냅샷. 직전 주 판결 등 초최신 조회에는 부적합.
- **정밀도**: Phase 1은 문서 단위 의미 검색. 조문 단위 정밀 검색은 Phase 2에서 chunk-level 벡터 추가 예정.
- **법률 자문 불가**: 모든 응답에는 원문 `url`을 첨부하며, 최종 법률 판단은 반드시 원문과 유자격 전문가 검토 후에 수행할 것.

## 라이선스

코드: MIT. 데이터: Open Australian Legal Corpus Licence 준수.

## 기여 및 참고

- 본 프로젝트는 Umar Butler / Isaacus의 Open Australian Legal Corpus에 의존.
- MCP 서버 구조는 동일 저자의 `mcp_kipris`, `korean-law-mcp`, `epo-ops-mcp-server` 패턴을 참고.
