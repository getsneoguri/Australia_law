"""세 개의 Parquet 파일(메타, 스니펫, 벡터)을 조인하여 Turso의 documents 테이블에 일괄 적재한다.

입력 파일:
    data/doc_metadata.parquet   (02 산출)
    data/doc_snippets.parquet   (03 산출)
    data/doc_vectors.parquet    (02 산출, embedding 포함)

적재 전략:
    - 세 Parquet를 version_id로 inner join하여 완전한 행만 처리.
    - 배치 단위로 parameterised INSERT. 배치 크기는 .env의 BULK_INSERT_BATCH_SIZE.
    - 중간 실패 시 재실행 안전: `INSERT OR REPLACE` 사용.
    - 진행 상황을 tqdm과 별도로 체크포인트 파일(data/load_checkpoint.txt)에 기록.

Writes 예산:
    약 232K insert × 3 (documents + FTS5 trigger + 벡터 인덱스) = 약 700K writes.
    Turso Free 월 10M writes 대비 7% 수준.
"""

from __future__ import annotations

import sys
from pathlib import Path

import libsql_client
import pyarrow.parquet as pq
from tqdm import tqdm

from au_law_mcp.config import settings
from au_law_mcp.db.client import get_client, vector_to_sqlite_literal


_CHECKPOINT_NAME = "load_checkpoint.txt"

# INSERT 문. vector() 함수 호출은 SQL 리터럴로 생성해야 하므로 동적으로 조립한다.
# 나머지 컬럼은 바인드 파라미터로 전달.
_INSERT_SQL_TEMPLATE = """
INSERT OR REPLACE INTO documents (
    version_id, title, jurisdiction, doc_type,
    date, citation, url, source, mime, when_scraped,
    text_snippet, embedding
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, vector('{vec_literal}'))
"""


def load_checkpoint(path: Path) -> int:
    """마지막으로 완료한 배치 인덱스를 반환. 없으면 0."""
    if path.exists():
        try:
            return int(path.read_text().strip())
        except ValueError:
            return 0
    return 0


def save_checkpoint(path: Path, batch_idx: int) -> None:
    path.write_text(str(batch_idx))


def load_merged_iterator(data_dir: Path):
    """세 Parquet를 version_id 기준 병합하여 행 단위로 yield.

    메모리 상에 한 번에 다 올리지 않기 위해 pandas join 대신 수동 dict 기반 lookup.
    doc_snippets와 doc_vectors는 232K 문서 기준이므로 dict로 유지해도 1~2GB 수준.
    """
    # 벡터 dict: version_id -> list[float]
    print("[05] doc_vectors.parquet 로드 중...")
    vec_tbl = pq.read_table(str(data_dir / "doc_vectors.parquet"))
    vec_dict = {
        vid: emb
        for vid, emb in zip(
            vec_tbl.column("version_id").to_pylist(),
            vec_tbl.column("embedding").to_pylist(),
        )
    }
    print(f"[05] 벡터 {len(vec_dict):,}개 로드 완료")

    # 스니펫 dict: version_id -> (title, text_snippet)
    print("[05] doc_snippets.parquet 로드 중...")
    snip_tbl = pq.read_table(str(data_dir / "doc_snippets.parquet"))
    snip_dict = {
        vid: (title, snippet)
        for vid, title, snippet in zip(
            snip_tbl.column("version_id").to_pylist(),
            snip_tbl.column("title").to_pylist(),
            snip_tbl.column("text_snippet").to_pylist(),
        )
    }
    print(f"[05] 스니펫 {len(snip_dict):,}개 로드 완료")

    # 메타 Parquet은 순회
    print("[05] doc_metadata.parquet 순회하며 병합...")
    meta_pf = pq.ParquetFile(str(data_dir / "doc_metadata.parquet"))

    missing_vec = 0
    missing_snip = 0

    for batch in meta_pf.iter_batches(batch_size=5_000):
        d = batch.to_pydict()
        for i, vid in enumerate(d["version_id"]):
            vec = vec_dict.get(vid)
            snip = snip_dict.get(vid)
            if vec is None:
                missing_vec += 1
                continue
            if snip is None:
                # 스니펫이 없어도 벡터는 있으므로 title/snippet은 빈 값으로 진행.
                missing_snip += 1
                title, snippet = None, None
            else:
                title, snippet = snip

            yield {
                "version_id": vid,
                "title": title or "(no title)",
                "jurisdiction": d["jurisdiction"][i],
                "doc_type": d["type"][i],
                "date": d["date"][i],
                "citation": d["citation"][i],
                "url": d["url"][i],
                "source": d["source"][i],
                "mime": d["mime"][i],
                "when_scraped": d["when_scraped"][i],
                "text_snippet": snippet,
                "embedding": vec,
            }

    if missing_vec:
        print(f"[05] 경고: 벡터 없는 문서 {missing_vec}건 스킵됨", file=sys.stderr)
    if missing_snip:
        print(f"[05] 경고: 스니펫 없는 문서 {missing_snip}건 (빈 값으로 적재)", file=sys.stderr)


def main() -> int:
    data_dir: Path = settings.build_data_dir
    batch_size: int = settings.bulk_insert_batch_size
    checkpoint_path: Path = data_dir / _CHECKPOINT_NAME

    # 필수 입력 파일 존재 확인
    for fname in ("doc_metadata.parquet", "doc_snippets.parquet", "doc_vectors.parquet"):
        if not (data_dir / fname).exists():
            print(f"[05] 오류: {data_dir / fname} 가 없습니다.", file=sys.stderr)
            return 1

    start_batch = load_checkpoint(checkpoint_path)
    if start_batch > 0:
        print(f"[05] 체크포인트 발견: 배치 {start_batch}부터 재개합니다.")

    # 배치 누적 버퍼
    buffer: list[dict] = []
    batch_idx = 0
    rows_written = 0

    with get_client() as client:
        with tqdm(total=232_560, desc="[05] loading", unit="doc") as pbar:
            for row in load_merged_iterator(data_dir):
                buffer.append(row)

                if len(buffer) >= batch_size:
                    batch_idx += 1
                    if batch_idx > start_batch:
                        _flush_batch(client, buffer)
                        save_checkpoint(checkpoint_path, batch_idx)
                        rows_written += len(buffer)
                    pbar.update(len(buffer))
                    buffer.clear()

            # 마지막 남은 배치
            if buffer:
                batch_idx += 1
                if batch_idx > start_batch:
                    _flush_batch(client, buffer)
                    save_checkpoint(checkpoint_path, batch_idx)
                    rows_written += len(buffer)
                pbar.update(len(buffer))
                buffer.clear()

    print(f"[05] 적재 완료. {rows_written:,}행 신규/갱신.")
    print(f"[05] 체크포인트 파일: {checkpoint_path}")
    return 0


def _flush_batch(client, batch: list[dict]) -> None:
    """한 배치를 트랜잭션으로 insert.

    각 행은 vector() 리터럴이 다르므로 execute를 행 단위로 호출하되
    트랜잭션 BEGIN/COMMIT으로 묶어 네트워크 라운드트립을 줄인다.
    libsql-client는 batch() 메서드를 제공하여 동일한 효과를 낸다.
    """
    statements = []
    for row in batch:
        vec_lit = vector_to_sqlite_literal(row["embedding"])
        sql = _INSERT_SQL_TEMPLATE.format(vec_literal=vec_lit).strip()
        statements.append(libsql_stmt(sql, [
            row["version_id"],
            row["title"],
            row["jurisdiction"],
            row["doc_type"],
            row["date"],
            row["citation"],
            row["url"],
            row["source"],
            row["mime"],
            row["when_scraped"],
            row["text_snippet"],
        ]))

    client.batch(statements)


def libsql_stmt(sql: str, args: list) -> libsql_client.Statement:
    """libsql_client.Statement 헬퍼. SQL과 바인드 인자를 묶는다."""
    return libsql_client.Statement(sql, args)


if __name__ == "__main__":
    sys.exit(main())
