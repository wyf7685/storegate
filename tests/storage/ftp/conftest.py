from collections.abc import AsyncIterator

import pytest

from app.storage.ftp import FTPConfig, FTPStorage


@pytest.fixture
async def ftp_storage(ftp_endpoint: tuple[str, int]) -> AsyncIterator[FTPStorage]:
    host, port = ftp_endpoint
    async with FTPStorage(FTPConfig(host=host, port=port, chunk_size=4)) as storage:
        yield storage
