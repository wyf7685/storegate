from collections.abc import AsyncIterator

import pytest

from app.storage.dav import DavConfig, DavStorage


@pytest.fixture
async def dav_storage(_dav_server: str) -> AsyncIterator[DavStorage]:
    config = DavConfig(base_url=_dav_server, auth_mode="anonymous")
    async with DavStorage(config) as s:
        yield s
