import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from app.storage.abstract import AbstractStorage


@pytest.fixture(
    params=[
        pytest.param("memory", id="memory"),
        pytest.param("local", id="local"),
        pytest.param("s3", marks=pytest.mark.s3, id="s3"),
        pytest.param("cached", id="cached"),
        pytest.param("index", id="index"),
        pytest.param("ftp", marks=pytest.mark.integration, id="ftp"),
        pytest.param("dav", marks=pytest.mark.integration, id="dav"),
    ]
)
async def storage(request: pytest.FixtureRequest) -> AsyncIterator[AbstractStorage]:
    """Yield every storage backend for the shared storage contract tests."""
    match request.param:
        case "memory":
            from app.storage.memory import MemoryStorage

            async with MemoryStorage("/") as instance:
                yield instance

        case "local":
            from app.storage.local import LocalStorage

            root = Path(tempfile.mkdtemp(prefix="storegate_test_"))
            try:
                async with LocalStorage(root) as instance:
                    yield instance
            finally:
                shutil.rmtree(root, ignore_errors=True)

        case "s3":
            from app.storage.s3 import S3Storage

            config_path = Path("data/s3/mock.json")
            if not config_path.exists():
                pytest.skip("S3 config file not found")
            async with S3Storage(config_path) as instance:
                yield instance

        case "cached":
            from app.storage.cached import CachedStorage
            from app.storage.memory import MemoryStorage

            async with MemoryStorage("/") as inner, CachedStorage(inner) as instance:
                yield instance

        case "index":
            from app.storage.index import IndexStorage
            from app.storage.memory import MemoryStorage

            async with IndexStorage(
                index=MemoryStorage("/"),
                chunks=MemoryStorage("/"),
                block_size=16 * 1024,
            ) as instance:
                yield instance

        case "dav":
            from app.storage.dav import DavConfig, DavStorage

            base_url = request.getfixturevalue("_dav_server")
            config = DavConfig(base_url=base_url, auth_mode="anonymous")
            async with DavStorage(config) as instance:
                yield instance

        case "ftp":
            from app.storage.ftp import FTPConfig, FTPStorage

            host, port = request.getfixturevalue("_ftp_server")
            config = FTPConfig(host=host, port=port, root_prefix="/")
            async with FTPStorage(config) as instance:
                yield instance
