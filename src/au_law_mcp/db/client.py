"""Turso libSQL 클라이언트 래퍼.

libsql-client의 동기 API를 프로젝트 공용으로 감싼다. 연결 객체는 context manager로
관리하여 스크립트 종료 시 자동으로 닫히도록 한다.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import libsql_client

from au_law_mcp.config import settings


@contextmanager
def get_client() -> Iterator[libsql_client.Client]:
    """동기 Turso 클라이언트를 with-block으로 제공.

    사용 예:
        with get_client() as client:
            rs = client.execute("SELECT count(*) FROM documents")
            print(rs.rows[0][0])
    """
    client = libsql_client.create_client_sync(
        url=settings.turso_database_url,
        auth_token=settings.turso_auth_token,
    )
    try:
        yield client
    finally:
        client.close()


def vector_to_sqlite_literal(vec: list[float] | tuple[float, ...]) -> str:
    """Python 리스트/튜플을 libSQL `vector('[...]')` 리터럴 문자열로 변환.

    libSQL은 vector() 함수에 JSON 배열 리터럴을 받는다. 반올림 없이
    6자리 소수로 직렬화하여 BGE 임베딩의 정밀도를 보존한다.
    """
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
