"""Turso libSQL 클라이언트 래퍼 (libsql-experimental 기반).

libsql_experimental은 sqlite3 호환 API를 제공한다.
connect() → conn.execute() → conn.commit() → conn.close() 패턴.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import libsql_experimental as libsql

from au_law_mcp.config import settings


@contextmanager
def get_connection() -> Iterator[libsql.Connection]:
    """Turso 연결을 with-block으로 제공.

    사용 예:
        with get_connection() as conn:
            row = conn.execute("SELECT count(*) FROM documents").fetchone()
            print(row[0])
    """
    conn = libsql.connect(
        settings.turso_database_url,
        auth_token=settings.turso_auth_token,
    )
    try:
        yield conn
    finally:
        conn.close()
