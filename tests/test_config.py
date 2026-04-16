"""config 모듈의 환경변수 로딩 동작을 검증한다.

이 테스트는 실제 Turso에 접속하지 않고, 환경변수 검증 로직만 테스트한다.
CI에서 TURSO_* 환경변수 없이도 통과해야 하므로 _required, _optional의
단위 동작을 모킹 기반으로 확인한다.
"""

from __future__ import annotations

import os

import pytest


def test_required_raises_on_missing(monkeypatch):
    """필수 환경변수 누락 시 RuntimeError 발생 확인."""
    # 이미 import된 모듈에서 settings는 이미 로드된 상태이므로,
    # _required 함수를 직접 import하여 테스트한다.
    from au_law_mcp.config import _required

    monkeypatch.delenv("TEST_KEY_DOES_NOT_EXIST", raising=False)
    with pytest.raises(RuntimeError, match="환경변수"):
        _required("TEST_KEY_DOES_NOT_EXIST")


def test_optional_returns_default(monkeypatch):
    """선택 환경변수 누락 시 기본값 반환 확인."""
    from au_law_mcp.config import _optional

    monkeypatch.delenv("TEST_OPTIONAL_KEY", raising=False)
    assert _optional("TEST_OPTIONAL_KEY", "fallback") == "fallback"


def test_optional_returns_env_value(monkeypatch):
    """환경변수가 설정되어 있으면 그 값을 반환."""
    from au_law_mcp.config import _optional

    monkeypatch.setenv("TEST_OPTIONAL_KEY", "actual_value")
    assert _optional("TEST_OPTIONAL_KEY", "fallback") == "actual_value"
