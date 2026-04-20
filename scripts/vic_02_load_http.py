import json, sys, time, os
from pathlib import Path
from dotenv import load_dotenv
import pyarrow.parquet as pq
import requests

load_dotenv(Path(__file__).parent.parent / ".env")

TURSO_URL = os.getenv("TURSO_DATABASE_URL", "").replace("libsql://", "https://")
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
DATA_DIR = Path(__file__).parent.parent / "data"
in_path = DATA_DIR / "vic_documents.parquet"

INSERT_SQL = """INSERT OR REPLACE INTO documents (
    version_id, title, jurisdiction, doc_type, date, citation,
    url, source, mime, when_scraped, text_snippet
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

HEADERS = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}

def run_sql(session, sql, args=None):
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [{"type": "null"} if v is None else {"type": "text", "value": str(v)} for v in args]
    body = {"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]}
    try:
        r = session.post(f"{TURSO_URL}/v2/pipeline", json=body, headers=HEADERS, timeout=30)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def main():
    if not TURSO_URL or not TURSO_TOKEN:
        print("오류: .env 파일에 TURSO_DATABASE_URL, TURSO_AUTH_TOKEN 필요"); return 1
    if not in_path.exists():
        print(f"오류: {in_path} 없음"); return 1

    print(f"[VIC-02] Turso: {TURSO_URL}")
    table = pq.read_table(str(in_path))
    d = table.to_pydict()
    total = len(d["version_id"])
    print(f"[VIC-02] {total}개 적재 시작...")

    session = requests.Session()

    # 연결 테스트
    test = run_sql(session, "SELECT count(*) FROM documents")
    if "error" in str(test) and "null" not in str(test):
        print(f"연결 실패: {test}"); return 1
    try:
        existing = test["results"][0]["response"]["result"]["rows"][0][0]["value"]
        print(f"[VIC-02] 현재 DB 전체 문서: {existing}개")
    except:
        print(f"[VIC-02] 연결 OK (카운트 파싱 스킵)")

    success = 0; errors = 0
    for i in range(total):
        args = [d[k][i] for k in ["version_id","title","jurisdiction","doc_type","date","citation","url","source","mime","when_scraped","text_snippet"]]
        result = run_sql(session, INSERT_SQL, args)
        resp_str = str(result)
        if '"type":"error"' in resp_str or ('"error"' in resp_str and '"type":"ok"' not in resp_str):
            errors += 1
            if errors <= 3: print(f"  오류 샘플: {resp_str[:200]}")
        else:
            success += 1
        if (i+1) % 10 == 0 or i+1 == total:
            print(f"[VIC-02] {i+1}/{total} | 성공: {success} | 오류: {errors}", flush=True)
        time.sleep(0.1)

    print(f"\n[VIC-02] 완료! 성공: {success}, 오류: {errors}")

    r = run_sql(session, "SELECT count(*) FROM documents WHERE jurisdiction='vic'")
    try:
        vic = r["results"][0]["response"]["result"]["rows"][0][0]["value"]
        print(f"  VIC 문서: {vic}개")
    except:
        print(f"  검증: {r}")
    return 0

if __name__ == "__main__": sys.exit(main())
