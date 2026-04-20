import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
import pyarrow.parquet as pq
from au_law_mcp.config import settings
from au_law_mcp.db.client import get_connection

DATA_DIR = Path(__file__).parent.parent / "data"
in_path = DATA_DIR / "vic_documents.parquet"

INSERT_SQL = """INSERT OR REPLACE INTO documents (
    version_id, title, jurisdiction, doc_type, date, citation,
    url, source, mime, when_scraped, text_snippet
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def main():
    if not in_path.exists():
        print(f"오류: {in_path} 없음"); return 1
    table = pq.read_table(str(in_path))
    d = table.to_pydict()
    total = len(d["version_id"])
    print(f"[VIC-02] {total}개 문서 적재 시작...")
    with get_connection() as conn:
        existing = conn.execute("SELECT count(*) FROM documents WHERE jurisdiction='vic'").fetchone()[0]
        print(f"[VIC-02] 기존 VIC 문서: {existing}개")
        for i in range(total):
            conn.execute(INSERT_SQL, (
                d["version_id"][i], d["title"][i], d["jurisdiction"][i],
                d["doc_type"][i], d["date"][i], d["citation"][i],
                d["url"][i], d["source"][i], d["mime"][i],
                d["when_scraped"][i], d["text_snippet"][i],
            ))
            if (i+1) % 10 == 0:
                conn.commit()
                print(f"[VIC-02] {i+1}/{total} 완료", flush=True)
        conn.commit()
        new_vic = conn.execute("SELECT count(*) FROM documents WHERE jurisdiction='vic'").fetchone()[0]
        total_all = conn.execute("SELECT count(*) FROM documents").fetchone()[0]
        fts = conn.execute("SELECT count(*) FROM documents_fts").fetchone()[0]
    print(f"\n[VIC-02] 완료!")
    print(f"  VIC 문서: {existing} → {new_vic} (+{new_vic-existing})")
    print(f"  전체 DB : {total_all}개")
    print(f"  FTS5   : {fts}개")
    return 0

if __name__ == "__main__": sys.exit(main())
