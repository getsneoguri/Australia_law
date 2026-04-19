"""로컬 SQLite 파일에 232K 문서를 적재한다.

원격 Turso에 행 단위로 insert하면 네트워크 왕복 지연(Seoul→US 150ms)으로
22시간 이상 걸린다. 대신:

  1단계: 이 스크립트로 로컬 SQLite 파일에 적재 (약 5~10분)
  2단계: turso db shell 명령으로 .dump → 원격에 일괄 전송

이 스크립트가 생성하는 파일:
    data/au_law.db          -- SQLite 데이터베이스 (약 500MB)
    data/au_law_dump.sql    -- SQL dump 파일 (약 600MB)
"""

from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

from au_law_mcp.config import settings

_BATCH_SIZE = 5000

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS documents (
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
)
"""

_INSERT_SQL = """
INSERT OR REPLACE INTO documents (
    version_id, title, jurisdiction, doc_type,
    date, citation, url, source, mime, when_scraped,
    text_snippet
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def main() -> int:
    data_dir: Path = settings.build_data_dir
    in_path: Path = data_dir / "documents.parquet"
    db_path: Path = data_dir / "au_law.db"
    dump_path: Path = data_dir / "au_law_insert.sql"

    if not in_path.exists():
        print(f"[04] 오류: {in_path} 없음.", file=sys.stderr)
        return 1

    # ── 1단계: 로컬 SQLite에 적재 ──
    print("[04] documents.parquet 로드 중...")
    table = pq.read_table(str(in_path))
    d = table.to_pydict()
    total = len(d["version_id"])
    print(f"[04] 총 {total:,}개 문서.")

    # 기존 DB 삭제 후 재생성
    if db_path.exists():
        db_path.unlink()
        print(f"[04] 기존 {db_path} 삭제.")

    print(f"[04] 로컬 SQLite에 적재 시작: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(_CREATE_TABLE)

    start_time = time.time()
    rows = []

    for i in range(total):
        rows.append((
            d["version_id"][i],
            d["title"][i],
            d["jurisdiction"][i],
            d["doc_type"][i],
            d["date"][i],
            d["citation"][i],
            d["url"][i],
            d["source"][i],
            d["mime"][i],
            d["when_scraped"][i],
            d["text_snippet"][i],
        ))

        if len(rows) >= _BATCH_SIZE:
            conn.executemany(_INSERT_SQL, rows)
            conn.commit()
            elapsed = time.time() - start_time
            pct = (i + 1) / total * 100
            print(f"[04] {i + 1:>7,}/{total:,} ({pct:5.1f}%) | {elapsed:.0f}초 경과", flush=True)
            rows.clear()

    # 마지막 배치
    if rows:
        conn.executemany(_INSERT_SQL, rows)
        conn.commit()

    count = conn.execute("SELECT count(*) FROM documents").fetchone()[0]
    elapsed = time.time() - start_time
    print(f"[04] 로컬 SQLite 적재 완료: {count:,}행, {elapsed:.0f}초")

    # ── 2단계: INSERT 문만 추출하여 SQL 파일로 덤프 ──
    print(f"[04] SQL dump 생성 중: {dump_path}")

    with open(str(dump_path), "w", encoding="utf-8") as f:
        cursor = conn.execute("SELECT * FROM documents")
        columns = [desc[0] for desc in cursor.description]
        batch_count = 0

        while True:
            batch = cursor.fetchmany(_BATCH_SIZE)
            if not batch:
                break

            for row in batch:
                values = []
                for val in row:
                    if val is None:
                        values.append("NULL")
                    else:
                        # SQL 문자열 이스케이프: 작은따옴표를 두 번으로
                        escaped = str(val).replace("'", "''")
                        values.append(f"'{escaped}'")

                cols = ", ".join(columns)
                vals = ", ".join(values)
                f.write(f"INSERT OR REPLACE INTO documents ({cols}) VALUES ({vals});\n")

            batch_count += 1
            if batch_count % 10 == 0:
                written = batch_count * _BATCH_SIZE
                print(f"[04] dump: {written:,}행 기록...", flush=True)

    conn.close()

    dump_size_mb = dump_path.stat().st_size / 1_048_576
    db_size_mb = db_path.stat().st_size / 1_048_576
    print(f"[04] 완료.")
    print(f"[04] 로컬 DB: {db_path} ({db_size_mb:.0f}MB)")
    print(f"[04] SQL dump: {dump_path} ({dump_size_mb:.0f}MB)")
    print()
    print("[04] ═══════════════════════════════════════════════════")
    print("[04] 다음 단계: Turso에 업로드")
    print("[04] 아래 명령어를 터미널에서 실행하세요:")
    print()
    print(f"    turso db shell au-law-mcp < {dump_path}")
    print()
    print("[04] 완료 후 05_verify.py를 실행하여 검증하세요.")
    print("[04] ═══════════════════════════════════════════════════")
    return 0


if __name__ == "__main__":
    sys.exit(main())
