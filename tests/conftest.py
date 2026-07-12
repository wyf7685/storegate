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


@pytest.fixture(
    params=[
        pytest.param("memory", id="memory"),
        pytest.param("local", id="local"),
        pytest.param("cos", id="cos"),
        pytest.param("cached", id="cached"),
        pytest.param("index", id="index"),
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

        case "cos":
            from app.storage.cos import CosStorage

            config_path = Path("data/cos/mock.json")
            if not config_path.exists():
                pytest.skip("COS config file not found")
            async with CosStorage(config_path) as s:
                yield s

        case "cached":
            from app.storage.cached import CachedStorage
            from app.storage.memory import MemoryStorage

            async with MemoryStorage("/") as inner, CachedStorage(inner) as s:
                yield s

        case "index":
            from app.storage.index import IndexStorage
            from app.storage.memory import MemoryStorage

            async with (
                MemoryStorage("/") as index_backend,
                MemoryStorage("/") as chunks_backend,
                IndexStorage(index_backend, chunks_backend, block_size=16 * 1024) as s,
            ):
                yield s
