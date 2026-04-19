"""Open Australian Legal Corpus를 다운로드하여 Parquet로 저장한다.

기존 접근법(embeddings 8GB 전체 다운로드 → 평균 풀링)에서 변경:
embeddings 데이터셋의 JSONL 포맷이 datasets 라이브러리 최신 버전과
호환 문제가 있어, corpus 데이터셋만 직접 다운로드한다.

Phase 1에서는 FTS5 전문검색만 제공하고,
벡터 의미 검색은 Phase 2에서 별도 구축한다.

출력:
    data/corpus.parquet  -- 전체 문서 (version_id, title, text 등)

데이터셋 공식 로드 방식: split='corpus' (데이터셋 카드 확인 완료)
"""

from __future__ import annotations

import sys
from pathlib import Path

from datasets import load_dataset

from au_law_mcp.config import settings


def main() -> int:
    output_path: Path = settings.build_data_dir / "corpus.parquet"

    if output_path.exists():
        size_mb = output_path.stat().st_size / 1_048_576
        print(f"[01] {output_path} 이미 존재 ({size_mb:.0f}MB). 스킵.")
        print("[01] 재다운로드하려면 위 파일을 삭제하고 다시 실행하세요.")
        return 0

    print("[01] isaacus/open-australian-legal-corpus 로드 중...")
    print("[01] 최초 실행 시 약 2~3GB를 다운로드합니다.")

    try:
        # 공식 데이터셋 카드 기준: split='corpus'
        ds = load_dataset(
            "isaacus/open-australian-legal-corpus",
            split="corpus",
            keep_in_memory=False,
            token=settings.hf_token,
        )
    except ValueError:
        # 일부 datasets 버전에서 config/split 이름이 다를 수 있음
        print("[01] split='corpus' 실패, split='train'으로 재시도...")
        ds = load_dataset(
            "isaacus/open-australian-legal-corpus",
            name="default",
            split="train",
            keep_in_memory=False,
            token=settings.hf_token,
        )

    total = len(ds)
    print(f"[01] 총 {total:,}개 문서 로드됨.")
    print(f"[01] 컬럼: {ds.column_names}")

    print(f"[01] Parquet로 저장: {output_path}")
    ds.to_parquet(str(output_path), batch_size=10_000)

    final_size_mb = output_path.stat().st_size / 1_048_576
    print(f"[01] 완료. {output_path} ({final_size_mb:.0f}MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
