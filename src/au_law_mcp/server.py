"""Australia Law REST API Server.

FastAPI 기반. Copilot Studio에서 OpenAPI 스펙으로 커스텀 커넥터 생성 가능.
Render Free 티어에 배포.

DB 접속: libsql-client (순수 Python, 빌드 도구 불필요) HTTP 모드 사용.
TURSO_DATABASE_URL을 https:// 형식으로 변환하여 HTTP 프로토콜로 접속.
"""

from __future__ import annotations

import os
from typing import Optional

import libsql_client
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn


# ── 환경변수 ──
def _env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f"환경변수 {key} 미설정")
    return val


TURSO_URL = _env("TURSO_DATABASE_URL")
TURSO_TOKEN = _env("TURSO_AUTH_TOKEN")

# libsql-client는 HTTP URL(https://)을 사용해야 함.
# Turso가 제공하는 libsql:// URL을 https://로 변환.
HTTP_URL = TURSO_URL.replace("libsql://", "https://")


def get_db():
    """동기 libsql-client 생성. 각 요청마다 새로 생성."""
    return libsql_client.create_client_sync(
        url=HTTP_URL,
        auth_token=TURSO_TOKEN,
    )


def query(sql: str, args=None):
    """SQL 실행 후 rows 반환하는 헬퍼."""
    client = get_db()
    try:
        if args:
            rs = client.execute(sql, args)
        else:
            rs = client.execute(sql)
        return rs.rows
    finally:
        client.close()


def query_one(sql: str, args=None):
    """단일 행 반환."""
    rows = query(sql, args)
    return rows[0] if rows else None


# ── FastAPI 앱 생성 ──
app = FastAPI(
    title="Australia Law API",
    description=(
        "호주 연방·주 법령 및 판례 검색 API. "
        "232,560개의 법률 문서 (법률, 시행령, 판례, 법안) 포함. "
        "관할: Commonwealth, NSW, QLD, WA, SA, Tasmania, Norfolk Island. "
        "이 API는 법률 자문을 제공하지 않습니다."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic 모델 ──

class SearchLegislationRequest(BaseModel):
    query: str = Field(description="검색어 (예: 'patent disclosure', 'privacy act')")
    jurisdiction: Optional[str] = Field(
        default="",
        description="관할 필터 (commonwealth, new_south_wales, queensland 등). 비워두면 전체.",
    )
    limit: Optional[int] = Field(default=10, description="최대 결과 수 (기본 10, 최대 50)")


class SearchCaseLawRequest(BaseModel):
    query: str = Field(description="검색어 (예: 'negligence duty of care')")
    jurisdiction: Optional[str] = Field(default="", description="관할 필터. 비워두면 전체.")
    date_from: Optional[str] = Field(default="", description="시작 날짜 (ISO: 2020-01-01)")
    date_to: Optional[str] = Field(default="", description="종료 날짜")
    limit: Optional[int] = Field(default=10, description="최대 결과 수 (기본 10, 최대 50)")


class GetDocumentRequest(BaseModel):
    version_id: str = Field(description="문서 고유 ID (search 결과에서 얻은 값)")


class FilterRequest(BaseModel):
    jurisdiction: str = Field(
        description="관할 (commonwealth, new_south_wales, queensland, western_australia, "
        "south_australia, tasmania, norfolk_island)"
    )
    doc_type: Optional[str] = Field(
        default="",
        description="문서 유형 (primary_legislation, secondary_legislation, decision, bill). 비워두면 전체.",
    )
    limit: Optional[int] = Field(default=20, description="최대 결과 수 (기본 20, 최대 100)")


class DocumentResult(BaseModel):
    version_id: str
    title: str
    citation: Optional[str] = None
    jurisdiction: str
    doc_type: Optional[str] = None
    date: Optional[str] = None
    url: str
    snippet: Optional[str] = None


class DocumentDetail(BaseModel):
    version_id: str
    title: str
    jurisdiction: str
    doc_type: str
    date: Optional[str] = None
    citation: Optional[str] = None
    url: str
    source: Optional[str] = None
    text_snippet: Optional[str] = None
    note: str = "text_snippet은 원문의 첫 2000자입니다. 전체 원문은 url에서 확인하세요."


class StatisticsResponse(BaseModel):
    total_documents: int
    jurisdictions: dict[str, int]
    document_types: dict[str, int]


# ── 엔드포인트 ──

@app.get("/health", tags=["System"])
def health_check():
    """서버 상태 확인 (UptimeRobot keep-alive용)."""
    return {"status": "ok"}


@app.post(
    "/api/search_legislation",
    response_model=list[DocumentResult],
    tags=["Search"],
    summary="법령 키워드 검색",
    description="호주 법령(Act, Regulation)을 FTS5 키워드로 검색합니다.",
)
def search_legislation(req: SearchLegislationRequest):
    limit = min(req.limit or 10, 50)

    if req.jurisdiction:
        rows = query("""
            SELECT d.version_id, d.title, d.citation, d.jurisdiction,
                   d.doc_type, d.date, d.url,
                   snippet(documents_fts, 2, '<b>', '</b>', '...', 40) AS snippet
            FROM documents_fts
            JOIN documents d ON d.rowid = documents_fts.rowid
            WHERE documents_fts MATCH ?
              AND d.doc_type IN ('primary_legislation', 'secondary_legislation')
              AND d.jurisdiction = ?
            ORDER BY rank
            LIMIT ?
        """, [req.query, req.jurisdiction, limit])
    else:
        rows = query("""
            SELECT d.version_id, d.title, d.citation, d.jurisdiction,
                   d.doc_type, d.date, d.url,
                   snippet(documents_fts, 2, '<b>', '</b>', '...', 40) AS snippet
            FROM documents_fts
            JOIN documents d ON d.rowid = documents_fts.rowid
            WHERE documents_fts MATCH ?
              AND d.doc_type IN ('primary_legislation', 'secondary_legislation')
            ORDER BY rank
            LIMIT ?
        """, [req.query, limit])

    return [
        DocumentResult(
            version_id=r[0], title=r[1], citation=r[2], jurisdiction=r[3],
            doc_type=r[4], date=r[5], url=r[6], snippet=r[7],
        )
        for r in rows
    ]


@app.post(
    "/api/search_case_law",
    response_model=list[DocumentResult],
    tags=["Search"],
    summary="판례 키워드 검색",
    description="호주 판례(court decisions)를 키워드로 검색합니다. 날짜·관할 필터 지원.",
)
def search_case_law(req: SearchCaseLawRequest):
    limit = min(req.limit or 10, 50)

    sql = """
        SELECT d.version_id, d.title, d.citation, d.jurisdiction,
               d.doc_type, d.date, d.url,
               snippet(documents_fts, 2, '<b>', '</b>', '...', 40) AS snippet
        FROM documents_fts
        JOIN documents d ON d.rowid = documents_fts.rowid
        WHERE documents_fts MATCH ?
          AND d.doc_type = 'decision'
    """
    params = [req.query]

    if req.jurisdiction:
        sql += " AND d.jurisdiction = ?"
        params.append(req.jurisdiction)
    if req.date_from:
        sql += " AND d.date >= ?"
        params.append(req.date_from)
    if req.date_to:
        sql += " AND d.date <= ?"
        params.append(req.date_to)

    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)

    rows = query(sql, params)

    return [
        DocumentResult(
            version_id=r[0], title=r[1], citation=r[2], jurisdiction=r[3],
            doc_type=r[4], date=r[5], url=r[6], snippet=r[7],
        )
        for r in rows
    ]


@app.post(
    "/api/get_document",
    response_model=DocumentDetail,
    tags=["Document"],
    summary="문서 상세 조회",
    description="version_id로 특정 문서의 메타데이터와 첫 2000자를 조회합니다.",
)
def get_document(req: GetDocumentRequest):
    row = query_one("""
        SELECT version_id, title, jurisdiction, doc_type,
               date, citation, url, source, text_snippet
        FROM documents
        WHERE version_id = ?
    """, [req.version_id])

    if not row:
        raise HTTPException(status_code=404, detail=f"문서를 찾을 수 없습니다: {req.version_id}")

    return DocumentDetail(
        version_id=row[0], title=row[1], jurisdiction=row[2], doc_type=row[3],
        date=row[4], citation=row[5], url=row[6], source=row[7], text_snippet=row[8],
    )


@app.post(
    "/api/filter_by_jurisdiction",
    response_model=list[DocumentResult],
    tags=["Browse"],
    summary="관할별 문서 목록",
    description="특정 관할·문서유형별로 문서 목록을 조회합니다.",
)
def filter_by_jurisdiction(req: FilterRequest):
    limit = min(req.limit or 20, 100)

    if req.doc_type:
        rows = query("""
            SELECT version_id, title, citation, jurisdiction, doc_type, date, url
            FROM documents
            WHERE jurisdiction = ? AND doc_type = ?
            ORDER BY date DESC NULLS LAST
            LIMIT ?
        """, [req.jurisdiction, req.doc_type, limit])
    else:
        rows = query("""
            SELECT version_id, title, citation, jurisdiction, doc_type, date, url
            FROM documents
            WHERE jurisdiction = ?
            ORDER BY date DESC NULLS LAST
            LIMIT ?
        """, [req.jurisdiction, limit])

    return [
        DocumentResult(
            version_id=r[0], title=r[1], citation=r[2], jurisdiction=r[3],
            doc_type=r[4], date=r[5], url=r[6],
        )
        for r in rows
    ]


@app.get(
    "/api/statistics",
    response_model=StatisticsResponse,
    tags=["System"],
    summary="DB 통계",
    description="전체 문서 수, 관할별·유형별 분포를 반환합니다.",
)
def get_statistics():
    total = query_one("SELECT count(*) FROM documents")[0]
    jur = query("SELECT jurisdiction, count(*) FROM documents GROUP BY jurisdiction ORDER BY count(*) DESC")
    types = query("SELECT doc_type, count(*) FROM documents GROUP BY doc_type ORDER BY count(*) DESC")

    return StatisticsResponse(
        total_documents=total,
        jurisdictions={r[0]: r[1] for r in jur},
        document_types={r[0]: r[1] for r in types},
    )


# ── 서버 실행 ──
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
