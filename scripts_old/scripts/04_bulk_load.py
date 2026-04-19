"""documents.parquet를 Turso의 documents 테이블에 일괄 적재한다.

간소화 버전: 벡터 컬럼 없이 메타데이터 + text_snippet만 적재.
체크포인트 지원으로 중간 실패 시 재실행 가능.

Writes 예산:
    약 232K insert × 2 (documents + FTS5 trigger) = 약 465K writes.
    Turso Free 월 10M writes 대비 5% 수준.
"""

from __future__ import annotations

import sys
from pathlib import Path

import libsql_client
import pyarrow.parquet as pq
from tqdm import tqdm

from au_law_mcp.config import settings
from au_law_mcp.db.client import get_client

_CHECKPOINT_NAME = "load_checkpoint.txt"

_INSERT_SQL = """
INSERT OR REPLACE INTO documents (
    version_id, title, jurisdiction, doc_type,
    date, citation, url, source, mime, when_scraped,
    text_snippet
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def load_checkpoint(path: Path) -> int:
    if path.exists():
        try:
            return int(path.read_text().strip())
        except ValueError:
            return 0
    return 0


def save_checkpoint(path: Path, batch_idx: int) -> None:
    path.write_text(str(batch_idx))


def main() -> int:
    data_dir: Path = settings.build_data_dir
    batch_size: int = settings.bulk_insert_batch_size
    in_path: Path = data_dir / "documents.parquet"
    checkpoint_path: Path = data_dir / _CHECKPOINT_NAME

    if not in_path.exists():
        print(f"[04] 오류: {in_path} 없음. 먼저 02_prepare_documents.py 실행.", file=sys.stderr)
        return 1

    pf = pq.ParquetFile(str(in_path))
    total_rows = pf.metadata.num_rows

    start_batch = load_checkpoint(checkpoint_path)
    if start_batch > 0:
        print(f"[04] 체크포인트: 배치 {start_batch}부터 재개.")

    batch_idx = 0
    rows_written = 0

    with get_client() as client:
        with tqdm(total=total_rows, desc="[04] loading", unit="doc") as pbar:
            for batch in pf.iter_batches(batch_size=batch_size):
                batch_idx += 1
                d = batch.to_pydict()
                n = len(d["version_id"])

                if batch_idx <= start_batch:
                    pbar.update(n)
                    continue

                # 배치를 Statement 리스트로 만든다
                statements = []
                for i in range(n):
                    stmt = libsql_client.Statement(
                        _INSERT_SQL.strip(),
                        [
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
                        ]
                    )
                    statements.append(stmt)

                try:
                    client.batch(statements)
                except Exception as e:
                    print(f"\n[04] 배치 {batch_idx} 에러: {e}", file=sys.stderr)
                    print(f"[04] 체크포인트 저장 후 중단. 재실행하면 여기서 재개.", file=sys.stderr)
                    save_checkpoint(checkpoint_path, batch_idx - 1)
                    return 2

                save_checkpoint(checkpoint_path, batch_idx)
                rows_written += n
                pbar.update(n)

    print(f"\n[04] 적재 완료. {rows_written:,}행.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
