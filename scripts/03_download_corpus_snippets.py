"""Open Australian Legal Corpus에서 title과 text_snippet을 추출한다.

embeddings 데이터셋에는 text 필드가 제거되어 있으므로, title과 풀텍스트의 앞부분
2000자를 FTS5 인덱싱용으로 별도 확보해야 한다. 이 스크립트는 corpus 데이터셋을
스트리밍 방식으로 읽어 디스크 사용을 최소화한다.

스트리밍을 쓰는 이유: 전체 corpus는 ~6GB. Parquet로 전부 내려받기보다 필요한
필드만 추출해 압축 저장하면 300~500MB 수준으로 줄어든다.

출력:
    data/doc_snippets.parquet  -- 컬럼: version_id, title, text_snippet
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset
from tqdm import tqdm

from au_law_mcp.config import settings

# text_snippet 길이. FTS5 품질과 DB 용량의 타협점.
# 2000자 ≈ 300~500 단어. 법령의 preamble과 주요 정의부를 충분히 포함.
_SNIPPET_CHARS = 2000

# Turso documents 테이블에 적재할 때 배치 크기와 일치시킬 필요는 없지만,
# Parquet 쓰기 배치 크기로 사용.
_WRITE_BATCH = 5_000


def main() -> int:
    out_path: Path = settings.build_data_dir / "doc_snippets.parquet"

    if out_path.exists():
        size_mb = out_path.stat().st_size / 1_048_576
        print(f"[03] {out_path} 이미 존재 (약 {size_mb:.0f}MB). 스킵.")
        return 0

    print("[03] isaacus/open-australian-legal-corpus를 streaming 모드로 로드...")
    # streaming=True: 로컬에 전체 파일을 받지 않고 iterable처럼 순회.
    # 단, 전체 개수를 모르므로 tqdm total은 공식 데이터셋 카드의 값을 사용.
    ds = load_dataset(
        "isaacus/open-australian-legal-corpus",
        split="corpus",
        streaming=True,
        token=settings.hf_token,
    )

    # v7.1.0 기준 문서 수. 버전 업데이트 시 갱신 필요.
    expected_total = 232_560

    # 메모리에 누적할 버퍼
    buf_version: list[str] = []
    buf_title: list[str | None] = []
    buf_snippet: list[str | None] = []

    # 첫 쓰기 여부 — Parquet에 스키마와 함께 새 파일 생성
    writer: pq.ParquetWriter | None = None
    schema = pa.schema([
        ("version_id", pa.string()),
        ("title", pa.string()),
        ("text_snippet", pa.string()),
    ])

    def flush():
        """버퍼 내용을 Parquet에 추가 쓰기."""
        nonlocal writer
        if not buf_version:
            return
        table = pa.table({
            "version_id": buf_version,
            "title": buf_title,
            "text_snippet": buf_snippet,
        }, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(str(out_path), schema, compression="zstd")
        writer.write_table(table)
        buf_version.clear()
        buf_title.clear()
        buf_snippet.clear()

    try:
        with tqdm(total=expected_total, desc="[03] streaming", unit="doc") as pbar:
            for row in ds:
                version_id = row.get("version_id")
                if not version_id:
                    continue

                # corpus 스키마 상 text와 title 필드. 없으면 None.
                text = row.get("text") or ""
                title = row.get("title")

                # 앞부분만 자르고 양 끝 공백 정리.
                snippet = text[:_SNIPPET_CHARS].strip() if text else None

                buf_version.append(version_id)
                buf_title.append(title)
                buf_snippet.append(snippet)

                if len(buf_version) >= _WRITE_BATCH:
                    flush()

                pbar.update(1)

        flush()

    finally:
        if writer is not None:
            writer.close()

    if out_path.exists():
        size_mb = out_path.stat().st_size / 1_048_576
        print(f"[03] 완료. {out_path} ({size_mb:.0f}MB)")
        return 0
    else:
        print("[03] 오류: 출력 파일이 생성되지 않았습니다.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
