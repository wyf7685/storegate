from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from storegate.storage.cached import CachedStorage
from storegate.storage.memory import MemoryStorage


@pytest.fixture
async def cached() -> AsyncIterator[CachedStorage]:
    """A CachedStorage wrapping MemoryStorage for fast, deterministic tests."""
    async with CachedStorage(MemoryStorage("/")) as s:
        yield s
