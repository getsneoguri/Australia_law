"""환경변수 및 전역 설정 로딩.

.env 파일에서 값을 읽어오며, 필수 값이 빠지면 즉시 오류를 발생시킨다.
모든 스크립트와 MCP 서버 코드가 이 모듈의 `settings`를 공유한다.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# 프로젝트 루트 디렉터리의 .env를 읽는다.
# 스크립트를 어디서 실행하든 동일한 설정을 로드하기 위함.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / ".env", override=False)


def _required(key: str) -> str:
    """필수 환경변수 조회. 없으면 명시적으로 실패시킨다."""
    value = os.getenv(key)
    if not value:
        raise RuntimeError(
            f"환경변수 {key}가 설정되지 않았습니다. "
            f"프로젝트 루트의 .env 파일 또는 Render 대시보드에서 지정하세요."
        )
    return value


def _optional(key: str, default: str) -> str:
    """선택 환경변수 조회. 비어 있으면 기본값 반환."""
    return os.getenv(key) or default


@dataclass(frozen=True)
class Settings:
    """런타임 설정 스냅샷 (불변)."""

    # Turso
    turso_database_url: str
    turso_auth_token: str

    # HuggingFace (선택)
    hf_token: str | None

    # 빌드 파이프라인
    build_data_dir: Path
    bulk_insert_batch_size: int

    # MCP 서버
    mcp_host: str
    mcp_port: int
    log_level: str


def load_settings() -> Settings:
    """환경변수로부터 Settings 인스턴스를 생성.

    스크립트 시작 시 한 번만 호출. 필수 값 누락 시 즉시 RuntimeError.
    """
    data_dir = Path(_optional("BUILD_DATA_DIR", "./data")).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        turso_database_url=_required("TURSO_DATABASE_URL"),
        turso_auth_token=_required("TURSO_AUTH_TOKEN"),
        hf_token=os.getenv("HF_TOKEN") or None,
        build_data_dir=data_dir,
        bulk_insert_batch_size=int(_optional("BULK_INSERT_BATCH_SIZE", "500")),
        mcp_host=_optional("MCP_HOST", "0.0.0.0"),
        mcp_port=int(_optional("MCP_PORT", "8000")),
        log_level=_optional("LOG_LEVEL", "INFO"),
    )


# 모듈 최초 import 시 즉시 로드. 누락 시 여기서 실패하므로 문제를 조기 발견.
settings = load_settings()
