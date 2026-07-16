from collections.abc import AsyncIterator

import pytest

from app.storage.index import IndexStorage
from app.storage.memory import MemoryStorage
from tests.storage.index.helpers import BLOCK_SIZE


@pytest.fixture
async def index_storage() -> AsyncIterator[IndexStorage]:
    """An IndexStorage backed by two MemoryStorage instances."""
    async with (
        MemoryStorage("/") as idx,
        MemoryStorage("/") as chunks,
        IndexStorage(idx, chunks, block_size=BLOCK_SIZE) as s,
    ):
        yield s
