"""HuggingFace에서 Open Australian Legal Embeddings 데이터셋을 로컬로 다운로드한다.

이 스크립트는 다음을 수행한다:
1. `isaacus/open-australian-legal-embeddings` 데이터셋을 로컬 캐시에 다운로드
2. `version_id`, `embedding`, 메타데이터 필드만 추출해 Parquet로 직렬화
3. 이후 스크립트에서 Parquet를 빠르게 load하여 처리

Parquet를 쓰는 이유: HuggingFace datasets는 기본 Arrow 포맷이지만, numpy로
벡터를 일괄 처리할 때 Parquet의 컬럼 기반 I/O가 더 효율적. 또한 재실행 시
HF API 재호출 없이 로컬에서만 동작 가능.

소요 시간: 네트워크 대역폭에 따라 30분~2시간. 다운로드 크기 약 8GB.
디스크 공간: 약 10GB 여유 필요 (HF 캐시 + Parquet 중간 파일).
"""

from __future__ import annotations

import sys
from pathlib import Path

from datasets import load_dataset
from tqdm import tqdm

from au_law_mcp.config import settings


# 추출할 컬럼 목록 — 문서 메타 + 청크 식별자 + 벡터.
# 이 스키마가 embeddings 데이터셋의 실제 필드와 일치해야 한다.
# HuggingFace dataset card 기준(v7.x):
#   version_id, type, jurisdiction, source, mime, date, citation, url,
#   when_scraped, chunk_index, is_last_chunk, embedding
_COLUMNS_TO_KEEP = [
    "version_id",
    "type",
    "jurisdiction",
    "source",
    "mime",
    "date",
    "citation",
    "url",
    "when_scraped",
    "chunk_index",
    "is_last_chunk",
    "embedding",
]


def main() -> int:
    """데이터셋 다운로드 및 Parquet 저장."""
    output_path: Path = settings.build_data_dir / "embeddings.parquet"

    if output_path.exists():
        size_mb = output_path.stat().st_size / 1_048_576
        print(f"[01] {output_path} 이미 존재 (약 {size_mb:.0f}MB). 재다운로드 스킵.")
        print("[01] 강제 재다운로드하려면 위 파일을 삭제하고 다시 실행하세요.")
        return 0

    print("[01] HuggingFace에서 isaacus/open-australian-legal-embeddings 로드 중...")
    print("[01] 최초 실행 시 ~8GB를 다운로드하므로 시간이 걸립니다.")

    # keep_in_memory=False: 메모리 부족 회피. Arrow 파일로 스트리밍.
    # token: 레이트 리밋 완화 및 속도 향상에 사용 (선택).
    ds = load_dataset(
        "isaacus/open-australian-legal-embeddings",
        split="corpus",
        keep_in_memory=False,
        token=settings.hf_token,
    )

    # 예상 컬럼이 모두 있는지 검증. 스키마가 바뀌었다면 즉시 실패.
    missing = [c for c in _COLUMNS_TO_KEEP if c not in ds.column_names]
    if missing:
        print(f"[01] 오류: 데이터셋에 기대한 컬럼이 없습니다: {missing}", file=sys.stderr)
        print(f"[01] 실제 컬럼: {ds.column_names}", file=sys.stderr)
        return 1

    # 불필요한 컬럼 제거로 Parquet 크기 최소화.
    to_drop = [c for c in ds.column_names if c not in _COLUMNS_TO_KEEP]
    if to_drop:
        ds = ds.remove_columns(to_drop)

    total = len(ds)
    print(f"[01] 총 {total:,}개 청크. Parquet로 저장 중: {output_path}")

    # datasets의 to_parquet는 내부적으로 청크 단위로 쓴다. 진행률 표시를 위해
    # 배치 사이즈를 명시.
    ds.to_parquet(
        str(output_path),
        batch_size=10_000,
    )

    final_size_mb = output_path.stat().st_size / 1_048_576
    print(f"[01] 완료. {output_path} ({final_size_mb:.0f}MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
