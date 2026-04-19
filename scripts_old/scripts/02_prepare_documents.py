"""corpus Parquet에서 Turso 적재용 문서 데이터를 준비한다.

corpus.parquet의 각 행에서:
  - 메타데이터 필드 (version_id, type, jurisdiction 등) 추출
  - text 필드의 앞 2000자를 text_snippet으로 잘라냄
  - title이 없는 경우 citation이나 version_id로 대체

출력:
    data/documents.parquet  -- Turso insert에 필요한 최종 컬럼만 포함
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from au_law_mcp.config import settings

_SNIPPET_CHARS = 2000
_BATCH_SIZE = 5_000


def main() -> int:
    in_path: Path = settings.build_data_dir / "corpus.parquet"
    out_path: Path = settings.build_data_dir / "documents.parquet"

    if not in_path.exists():
        print(f"[02] 오류: {in_path} 없음. 먼저 01_download_corpus.py 실행.", file=sys.stderr)
        return 1

    if out_path.exists():
        size_mb = out_path.stat().st_size / 1_048_576
        print(f"[02] {out_path} 이미 존재 ({size_mb:.0f}MB). 스킵.")
        return 0

    pf = pq.ParquetFile(str(in_path))
    total_rows = pf.metadata.num_rows
    print(f"[02] corpus에서 {total_rows:,}개 문서 처리 중...")

    schema = pa.schema([
        ("version_id", pa.string()),
        ("title", pa.string()),
        ("jurisdiction", pa.string()),
        ("doc_type", pa.string()),
        ("date", pa.string()),
        ("citation", pa.string()),
        ("url", pa.string()),
        ("source", pa.string()),
        ("mime", pa.string()),
        ("when_scraped", pa.string()),
        ("text_snippet", pa.string()),
    ])

    writer = None
    processed = 0

    try:
        for batch in tqdm(pf.iter_batches(batch_size=_BATCH_SIZE),
                          total=total_rows // _BATCH_SIZE + 1,
                          desc="[02] processing"):
            d = batch.to_pydict()
            n = len(d.get("version_id", []))

            rows = {
                "version_id": [],
                "title": [],
                "jurisdiction": [],
                "doc_type": [],
                "date": [],
                "citation": [],
                "url": [],
                "source": [],
                "mime": [],
                "when_scraped": [],
                "text_snippet": [],
            }

            for i in range(n):
                vid = d.get("version_id", [None])[i]
                if not vid:
                    continue

                text = d.get("text", [None])[i] or ""
                title = d.get("title", [None])[i]
                citation = d.get("citation", [None])[i]

                # title 없으면 citation 사용, 그것도 없으면 version_id
                if not title:
                    title = citation or vid

                rows["version_id"].append(vid)
                rows["title"].append(title)
                rows["jurisdiction"].append(d.get("jurisdiction", [None])[i] or "unknown")
                rows["doc_type"].append(d.get("type", [None])[i] or "unknown")
                rows["date"].append(d.get("date", [None])[i])
                rows["citation"].append(citation)
                rows["url"].append(d.get("url", [None])[i] or "")
                rows["source"].append(d.get("source", [None])[i])
                rows["mime"].append(d.get("mime", [None])[i])
                rows["when_scraped"].append(d.get("when_scraped", [None])[i])
                rows["text_snippet"].append(text[:_SNIPPET_CHARS].strip() if text else None)

            table = pa.table(rows, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(str(out_path), schema, compression="zstd")
            writer.write_table(table)
            processed += len(rows["version_id"])

    finally:
        if writer:
            writer.close()

    size_mb = out_path.stat().st_size / 1_048_576
    print(f"[02] 완료. {processed:,}개 문서 → {out_path} ({size_mb:.0f}MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
