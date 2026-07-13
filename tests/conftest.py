"""Shared fixtures for storage tests."""

import shutil
import sys
import tempfile
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from app.storage.abstract import AbstractStorage


@pytest.fixture(autouse=True, scope="session")
def configure_logging() -> None:
    """Configure logging for tests."""
    from app.log import log_format, log_level_filter, logger, remove_loguru_sinks

    remove_loguru_sinks()
    logger.add(
        sys.stdout,
        level="DEBUG",
        diagnose=False,
        enqueue=False,
        format=log_format,
        filter=log_level_filter(),
    )


def uid() -> str:
    """Return a short unique identifier for test isolation."""
    return uuid.uuid4().hex[:12]


def _param_of(impl: str):  # noqa: ANN202
    """Return a pytest.param for the given storage implementation."""
    return pytest.param(impl, marks=getattr(pytest.mark, impl), id=impl)


@pytest.fixture(
    params=[
        _param_of("memory"),
        _param_of("local"),
        _param_of("s3"),
        _param_of("cached"),
        _param_of("index"),
    ]
)
async def storage(request: pytest.FixtureRequest) -> AsyncIterator[AbstractStorage]:
    """Parametrized fixture: yields each storage backend for every test."""
    match request.param:
        case "memory":
            from app.storage.memory import MemoryStorage

            async with MemoryStorage("/") as s:
                yield s

        case "local":
            from app.storage.local import LocalStorage

            root = Path(tempfile.mkdtemp(prefix="storegate_test_"))
            try:
                async with LocalStorage(root) as s:
                    yield s
            finally:
                shutil.rmtree(root, ignore_errors=True)

        case "s3":
            from app.storage.s3 import S3Storage

            config_path = Path("data/s3/mock.json")
            if not config_path.exists():
                pytest.skip("S3 config file not found")
            async with S3Storage(config_path) as s:
                yield s

        case "cached":
            from app.storage.cached import CachedStorage
            from app.storage.memory import MemoryStorage

            async with MemoryStorage("/") as inner, CachedStorage(inner) as s:
                yield s

        case "index":
            from app.storage.index import IndexStorage
            from app.storage.memory import MemoryStorage

            async with IndexStorage(
                index=MemoryStorage("/"),
                chunks=MemoryStorage("/"),
                block_size=16 * 1024,
            ) as s:
                yield s
