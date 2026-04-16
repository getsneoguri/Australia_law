"""청크 단위 임베딩을 문서 단위(version_id)로 평균 풀링한다.

Phase 1에서는 문서 하나당 하나의 F32 384-dim 벡터만 Turso에 저장한다.
Open Australian Legal Embeddings는 문서를 512-token chunks로 쪼갠 뒤 각 chunk를
bge-small-en-v1.5로 임베딩한 것이므로, 같은 `version_id`를 가진 모든 chunks의
벡터를 산술 평균(mean pooling)하면 문서 수준의 대표 벡터가 된다.

출력:
    data/doc_vectors.parquet  -- 컬럼: version_id (TEXT), embedding (list[float32])
    data/doc_metadata.parquet -- 컬럼: version_id 및 문서 공통 메타 (첫 chunk의 값)

메모리 전략:
    5.2M × 384 F32 = 약 8GB. 전부 in-memory에 올리기는 Render와 무관하게
    로컬 빌드 머신의 RAM 여유가 필요하다. 대안으로 버퍼 방식을 쓴다:
    version_id 기준으로 정렬된 데이터를 순차 스캔하며 같은 id의 벡터만 누적하고,
    id가 바뀌면 flush. 이 방식은 RAM 1~2GB로 처리 가능.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from au_law_mcp.config import settings

# 한 번에 메모리로 읽는 행 수. 너무 크면 OOM, 너무 작으면 느림.
_READ_BATCH = 50_000


def main() -> int:
    in_path: Path = settings.build_data_dir / "embeddings.parquet"
    out_vec_path: Path = settings.build_data_dir / "doc_vectors.parquet"
    out_meta_path: Path = settings.build_data_dir / "doc_metadata.parquet"

    if not in_path.exists():
        print(f"[02] 오류: {in_path} 가 없습니다. 먼저 01_download_embeddings.py 실행.", file=sys.stderr)
        return 1

    if out_vec_path.exists() and out_meta_path.exists():
        print(f"[02] {out_vec_path}, {out_meta_path} 이미 존재. 스킵.")
        print("[02] 재계산하려면 두 파일을 삭제하고 다시 실행하세요.")
        return 0

    # embeddings.parquet은 한 행이 하나의 chunk.
    # version_id로 정렬된 순서가 아니므로, 먼저 정렬된 사본을 메모리에 유지하지 않고
    # dict로 누적 집계한다. 키가 232K개이고 값이 384-dim float32 벡터이므로
    # 누적에 필요한 메모리는 232K × 384 × 4 byte = 약 350MB.
    print(f"[02] {in_path} 를 순차 스캔하며 version_id별로 벡터 평균화...")

    parquet_file = pq.ParquetFile(str(in_path))
    total_rows = parquet_file.metadata.num_rows
    print(f"[02] 총 청크 수: {total_rows:,}")

    # 누적용 자료구조
    # sum_dict[version_id] = numpy.ndarray(384,) float64 (누적 합, 오차 줄이기 위해 f64)
    # count_dict[version_id] = int
    # meta_dict[version_id] = dict (첫 chunk의 메타 저장)
    sum_dict: dict[str, np.ndarray] = {}
    count_dict: dict[str, int] = {}
    meta_dict: dict[str, dict[str, str | None]] = {}

    meta_cols = ["type", "jurisdiction", "source", "mime", "date", "citation", "url", "when_scraped"]

    with tqdm(total=total_rows, desc="[02] aggregating", unit="chunk") as pbar:
        for batch in parquet_file.iter_batches(batch_size=_READ_BATCH):
            df = batch.to_pydict()
            version_ids = df["version_id"]
            embeddings = df["embedding"]  # list of list[float]

            for i, vid in enumerate(version_ids):
                vec = np.asarray(embeddings[i], dtype=np.float64)
                if vid in sum_dict:
                    sum_dict[vid] += vec
                    count_dict[vid] += 1
                else:
                    sum_dict[vid] = vec.copy()
                    count_dict[vid] = 1
                    meta_dict[vid] = {c: df[c][i] for c in meta_cols}

            pbar.update(len(version_ids))

    n_docs = len(sum_dict)
    print(f"[02] 고유 문서 수: {n_docs:,}")

    # 평균 및 F32 캐스팅
    print("[02] 평균 계산 및 F32 캐스팅...")
    version_ids_sorted = sorted(sum_dict.keys())
    mean_vectors = np.zeros((n_docs, 384), dtype=np.float32)
    for i, vid in enumerate(tqdm(version_ids_sorted, desc="[02] mean pool")):
        mean_vectors[i] = (sum_dict[vid] / count_dict[vid]).astype(np.float32)

    # doc_vectors.parquet 저장
    # pyarrow의 list<float32> 타입을 사용. 이후 스크립트에서 row-wise로 읽어
    # Turso에 전송한다.
    print(f"[02] {out_vec_path} 저장 중...")
    vec_table = pa.table({
        "version_id": pa.array(version_ids_sorted, type=pa.string()),
        "embedding": pa.array([v.tolist() for v in mean_vectors], type=pa.list_(pa.float32())),
    })
    pq.write_table(vec_table, str(out_vec_path), compression="zstd")

    # doc_metadata.parquet 저장
    print(f"[02] {out_meta_path} 저장 중...")
    meta_table_dict: dict[str, list] = {"version_id": version_ids_sorted}
    for col in meta_cols:
        meta_table_dict[col] = [meta_dict[vid][col] for vid in version_ids_sorted]
    meta_table = pa.table(meta_table_dict)
    pq.write_table(meta_table, str(out_meta_path), compression="zstd")

    print(f"[02] 완료. 벡터 {out_vec_path.stat().st_size / 1_048_576:.0f}MB, "
          f"메타 {out_meta_path.stat().st_size / 1_048_576:.0f}MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
