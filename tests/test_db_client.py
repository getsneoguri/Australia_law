"""db.client의 순수 함수 단위 테스트.

실제 Turso 접속이 필요한 get_client 테스트는 여기서 제외하고
(통합 테스트 별도 필요), SQL 리터럴 변환 함수만 검증한다.
"""

from __future__ import annotations


def test_vector_literal_basic():
    from au_law_mcp.db.client import vector_to_sqlite_literal

    result = vector_to_sqlite_literal([1.0, 2.5, -0.333333])
    # 소수 6자리 직렬화, 대괄호 포함
    assert result == "[1.000000,2.500000,-0.333333]"


def test_vector_literal_empty():
    from au_law_mcp.db.client import vector_to_sqlite_literal

    assert vector_to_sqlite_literal([]) == "[]"


def test_vector_literal_tuple():
    """튜플 입력도 허용되어야 함."""
    from au_law_mcp.db.client import vector_to_sqlite_literal

    assert vector_to_sqlite_literal((0.1, 0.2)) == "[0.100000,0.200000]"
