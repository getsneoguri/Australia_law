"""documents.parquet를 Turso에 적재한다.

행 단위 execute + 100행마다 commit.
executemany() 대량 전송이 Turso 원격 서버에서 타임아웃되는 문제 회피.
체크포인트 지원으로 중간 실패 시 재실행 가능.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

from au_law_mcp.config import settings
from au_law_mcp.db.client import get_connection

_COMMIT_EVERY = 100  # 100행마다 commit
_CHECKPOINT_EVERY = 1000  # 1000행마다 체크포인트 저장
_CHECKPOINT_FILE = "load_progress.txt"

_INSERT_SQL = """
INSERT OR REPLACE INTO documents (
    version_id, title, jurisdiction, doc_type,
    date, citation, url, source, mime, when_scraped,
    text_snippet
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip()


def load_progress(path: Path) -> int:
    """마지막으로 완료한 행 번호 반환. 없으면 0."""
    if path.exists():
        try:
            return int(path.read_text().strip())
        except ValueError:
            return 0
    return 0


def save_progress(path: Path, row_num: int) -> None:
    path.write_text(str(row_num))


def main() -> int:
    data_dir: Path = settings.build_data_dir
    in_path: Path = data_dir / "documents.parquet"
    progress_path: Path = data_dir / _CHECKPOINT_FILE

    if not in_path.exists():
        print(f"[04] 오류: {in_path} 없음.", file=sys.stderr)
        return 1

    # Parquet 전체를 pydict로 읽기 (147MB이므로 메모리 문제 없음)
    print("[04] documents.parquet 로드 중...")
    table = pq.read_table(str(in_path))
    d = table.to_pydict()
    total = len(d["version_id"])
    print(f"[04] 총 {total:,}개 문서.")

    start_row = load_progress(progress_path)
    if start_row > 0:
        print(f"[04] 체크포인트: {start_row:,}행부터 재개.")

    rows_written = 0
    start_time = time.time()

    with get_connection() as conn:
        for i in range(start_row, total):
            conn.execute(_INSERT_SQL, (
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

            rows_written += 1

            # 주기적 commit
            if rows_written % _COMMIT_EVERY == 0:
                conn.commit()

            # 진행률 출력 + 체크포인트
            if rows_written % _CHECKPOINT_EVERY == 0:
                elapsed = time.time() - start_time
                speed = rows_written / elapsed if elapsed > 0 else 0
                remaining = (total - i - 1) / speed if speed > 0 else 0
                pct = (i + 1) / total * 100

                print(
                    f"[04] {i + 1:>7,}/{total:,} ({pct:5.1f}%) "
                    f"| {speed:.1f} doc/s "
                    f"| 남은 시간: {remaining / 60:.0f}분",
                    flush=True,
                )
                save_progress(progress_path, i + 1)

        # 마지막 commit
        conn.commit()
        save_progress(progress_path, total)

    elapsed_total = (time.time() - start_time) / 60
    print(f"\n[04] 적재 완료. {rows_written:,}행, {elapsed_total:.1f}분 소요.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
