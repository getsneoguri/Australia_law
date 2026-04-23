"""Australia Law REST API Server - Copilot Studio 호환 버전 (GET 방식)."""

from fastapi.responses import HTMLResponse

from __future__ import annotations
import os
from typing import Optional
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

def _env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f"환경변수 {key} 미설정")
    return val

TURSO_URL = _env("TURSO_DATABASE_URL").replace("libsql://", "https://")
TURSO_TOKEN = _env("TURSO_AUTH_TOKEN")
TURSO_PIPELINE_URL = f"{TURSO_URL}/v2/pipeline"

def _arg(v):
    if v is None:
        return {"type": "null"}
    return {"type": "text", "value": str(v)}

def query(sql: str, args=None):
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [_arg(a) for a in args]
    body = {"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]}
    with httpx.Client(timeout=15) as client:
        r = client.post(
            TURSO_PIPELINE_URL, json=body,
            headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
        )
        r.raise_for_status()
        data = r.json()
    result = data["results"][0]["response"]["result"]
    return [[cell.get("value") for cell in row] for row in result["rows"]]

def query_one(sql: str, args=None):
    rows = query(sql, args)
    return rows[0] if rows else None

app = FastAPI(
    title="Australia Law API",
    description="Australia federal and state legislation and case law search API. Over 232000 legal documents. Jurisdictions: Commonwealth, NSW, QLD, WA, SA, Tasmania, VIC. Not legal advice.",
    version="1.2.0",
    servers=[{"url": "https://au-law-mcp.onrender.com", "description": "Production"}],
    openapi_version="3.0.3",
)
# OpenAPI 3.0.3 강제 변환 (Copilot Studio / Power Automate 호환)
from fastapi.openapi.utils import get_openapi

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        servers=[{"url": "https://au-law-mcp.onrender.com", "description": "Production"}],
    )
    schema["openapi"] = "3.0.3"
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health", summary="서버 상태 확인", tags=["System"])
def health_check():
    return {"status": "ok", "version": "1.2.0"}

@app.get("/api/statistics", summary="DB 통계 조회", tags=["System"])
def get_statistics():
    """전체 문서 수, 관할별·유형별 분포를 반환합니다."""
    total = query_one("SELECT count(*) FROM documents")[0]
    jur = query("SELECT jurisdiction, count(*) FROM documents GROUP BY jurisdiction ORDER BY count(*) DESC")
    types = query("SELECT doc_type, count(*) FROM documents GROUP BY doc_type ORDER BY count(*) DESC")
    return {
        "total_documents": int(total),
        "jurisdictions": {r[0]: int(r[1]) for r in jur},
        "document_types": {r[0]: int(r[1]) for r in types},
    }

@app.get("/api/search_legislation", summary="법령 키워드 검색", tags=["Search"])
def search_legislation(
    q: str = Query(description="검색어 (예: patent, privacy act, negligence)"),
    jurisdiction: str = Query(default="", description="관할 필터: commonwealth, new_south_wales, queensland, victoria, western_australia, south_australia, tasmania. 비워두면 전체."),
    limit: int = Query(default=10, description="최대 결과 수 (기본 10, 최대 50)"),
):
    """호주 법령을 FTS5 키워드로 검색합니다."""
    limit = min(limit, 50)
    if jurisdiction:
        rows = query("""
            SELECT d.version_id, d.title, d.citation, d.jurisdiction, d.doc_type, d.date, d.url,
                   snippet(documents_fts, 2, '', '', '...', 40)
            FROM documents_fts JOIN documents d ON d.rowid = documents_fts.rowid
            WHERE documents_fts MATCH ? AND d.jurisdiction = ?
            ORDER BY rank LIMIT ?
        """, [q, jurisdiction, limit])
    else:
        rows = query("""
            SELECT d.version_id, d.title, d.citation, d.jurisdiction, d.doc_type, d.date, d.url,
                   snippet(documents_fts, 2, '', '', '...', 40)
            FROM documents_fts JOIN documents d ON d.rowid = documents_fts.rowid
            WHERE documents_fts MATCH ? ORDER BY rank LIMIT ?
        """, [q, limit])
    return [{"version_id": r[0], "title": r[1], "citation": r[2], "jurisdiction": r[3],
             "doc_type": r[4], "date": r[5], "url": r[6], "snippet": r[7]} for r in rows]

@app.get("/api/search_case_law", summary="판례 키워드 검색", tags=["Search"])
def search_case_law(
    q: str = Query(description="검색어 (예: negligence, duty of care, patent infringement)"),
    jurisdiction: str = Query(default="", description="관할 필터. 비워두면 전체."),
    date_from: str = Query(default="", description="시작 날짜 (YYYY-MM-DD)"),
    date_to: str = Query(default="", description="종료 날짜 (YYYY-MM-DD)"),
    limit: int = Query(default=10, description="최대 결과 수 (기본 10, 최대 50)"),
):
    """호주 판례를 키워드로 검색합니다."""
    limit = min(limit, 50)
    sql = """
        SELECT d.version_id, d.title, d.citation, d.jurisdiction, d.doc_type, d.date, d.url,
               snippet(documents_fts, 2, '', '', '...', 40)
        FROM documents_fts JOIN documents d ON d.rowid = documents_fts.rowid
        WHERE documents_fts MATCH ? AND d.doc_type = 'decision'
    """
    params = [q]
    if jurisdiction:
        sql += " AND d.jurisdiction = ?"; params.append(jurisdiction)
    if date_from:
        sql += " AND d.date >= ?"; params.append(date_from)
    if date_to:
        sql += " AND d.date <= ?"; params.append(date_to)
    sql += " ORDER BY rank LIMIT ?"; params.append(limit)
    rows = query(sql, params)
    return [{"version_id": r[0], "title": r[1], "citation": r[2], "jurisdiction": r[3],
             "doc_type": r[4], "date": r[5], "url": r[6], "snippet": r[7]} for r in rows]

@app.get("/api/get_document", summary="문서 상세 조회", tags=["Document"])
def get_document(
    version_id: str = Query(description="문서 고유 ID (search 결과의 version_id 값)"),
):
    """version_id로 문서 메타데이터와 본문 첫 2000자를 조회합니다."""
    row = query_one("""
        SELECT version_id, title, jurisdiction, doc_type, date, citation, url, source, text_snippet
        FROM documents WHERE version_id = ?
    """, [version_id])
    if not row:
        raise HTTPException(status_code=404, detail=f"문서 없음: {version_id}")
    return {"version_id": row[0], "title": row[1], "jurisdiction": row[2], "doc_type": row[3],
            "date": row[4], "citation": row[5], "url": row[6], "source": row[7],
            "text_snippet": row[8], "note": "전체 원문은 url에서 확인하세요."}

@app.get("/api/filter_by_jurisdiction", summary="관할별 문서 목록", tags=["Browse"])
def filter_by_jurisdiction(
    jurisdiction: str = Query(description="관할: commonwealth, new_south_wales, queensland, victoria, western_australia, south_australia, tasmania"),
    doc_type: str = Query(default="", description="문서 유형: primary_legislation, secondary_legislation, decision, bill, act, statutory_rule. 비워두면 전체."),
    limit: int = Query(default=20, description="최대 결과 수 (기본 20, 최대 100)"),
):
    """특정 관할·문서유형별 문서 목록을 조회합니다."""
    limit = min(limit, 100)
    if doc_type:
        rows = query("""
            SELECT version_id, title, citation, jurisdiction, doc_type, date, url
            FROM documents WHERE jurisdiction = ? AND doc_type = ?
            ORDER BY date DESC LIMIT ?
        """, [jurisdiction, doc_type, limit])
    else:
        rows = query("""
            SELECT version_id, title, citation, jurisdiction, doc_type, date, url
            FROM documents WHERE jurisdiction = ? ORDER BY date DESC LIMIT ?
        """, [jurisdiction, limit])
    return [{"version_id": r[0], "title": r[1], "citation": r[2], "jurisdiction": r[3],
             "doc_type": r[4], "date": r[5], "url": r[6]} for r in rows]

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
