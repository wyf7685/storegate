from collections.abc import AsyncIterator

import pytest

from app.storage.cached import CachedStorage
from app.storage.memory import MemoryStorage


@pytest.fixture
async def cached() -> AsyncIterator[CachedStorage]:
    """A CachedStorage wrapping MemoryStorage for fast, deterministic tests."""
    async with MemoryStorage("/") as inner, CachedStorage(inner) as s:
        yield s
