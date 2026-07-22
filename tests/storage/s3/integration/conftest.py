from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from storegate.storage.s3 import S3Storage


@pytest.fixture
async def real_s3_storage() -> AsyncIterator[S3Storage]:
    config_path = Path("data/s3/mock.json")
    if not config_path.exists():
        pytest.skip("S3 config file not found")
    async with S3Storage(config_path) as s:
        yield s
