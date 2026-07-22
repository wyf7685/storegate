import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from pydantic import SecretStr

from storegate.storage.abstract import AbstractStorage


@pytest.fixture(scope="session")
def host_symlink_create() -> bool:
    """Whether this process can create OS symbolic links on the host filesystem."""
    from tests.support.host_symlinks import probe_host_symlink_create

    return probe_host_symlink_create()


@pytest.fixture(
    params=[
        pytest.param("memory", id="memory"),
        pytest.param("local", id="local"),
        pytest.param("s3", marks=[pytest.mark.integration, pytest.mark.httpx], id="s3"),
        pytest.param("cached", id="cached"),
        pytest.param("index", id="index"),
        pytest.param("ftp", marks=pytest.mark.integration, id="ftp"),
        pytest.param("dav", marks=[pytest.mark.integration, pytest.mark.httpx], id="dav"),
        pytest.param("sftp", marks=pytest.mark.integration, id="sftp"),
    ]
)
async def storage(request: pytest.FixtureRequest) -> AsyncIterator[AbstractStorage]:
    """Yield every storage backend for the shared storage contract tests."""
    match request.param:
        case "memory":
            from storegate.storage.memory import MemoryStorage

            async with MemoryStorage("/shared-contract-root") as instance:
                yield instance

        case "local":
            from storegate.storage.local import LocalStorage

            root = Path(tempfile.mkdtemp(prefix="storegate_test_"))
            try:
                async with LocalStorage(root) as instance:
                    yield instance
            finally:
                shutil.rmtree(root, ignore_errors=True)

        case "s3":
            from storegate.storage.s3 import S3Config, S3Storage

            endpoint, bucket = request.getfixturevalue("_s3_server")
            config = S3Config(
                access_key_id=SecretStr("test"),
                secret_access_key=SecretStr("test"),
                region="us-east-1",
                bucket=bucket,
                endpoint_url=endpoint.removeprefix("http://"),
                path_style=True,
                scheme="http",
            )
            async with S3Storage(config) as instance:
                yield instance

        case "cached":
            from storegate.storage.cached import CachedStorage
            from storegate.storage.memory import MemoryStorage

            async with CachedStorage(MemoryStorage("/shared-contract-root")) as instance:
                yield instance

        case "index":
            from storegate.storage.index import IndexStorage
            from storegate.storage.memory import MemoryStorage

            async with IndexStorage(
                index=MemoryStorage("/"),
                chunks=MemoryStorage("/"),
                block_size=16 * 1024,
            ) as instance:
                yield instance

        case "dav":
            from storegate.storage.dav import DavConfig, DavStorage

            base_url = request.getfixturevalue("_dav_server")
            config = DavConfig(base_url=base_url, auth_mode="anonymous")
            async with DavStorage(config) as instance:
                yield instance

        case "ftp":
            from storegate.storage.ftp import FTPConfig, FTPStorage

            host, port = request.getfixturevalue("_ftp_server")
            config = FTPConfig(host=host, port=port, root_prefix="/")
            async with FTPStorage(config) as instance:
                yield instance

        case "sftp":
            from storegate.storage.sftp import SFTPConfig, SFTPStorage

            server = request.getfixturevalue("_sftp_server")
            config = SFTPConfig(
                host=server.host,
                port=server.port,
                username=server.username,
                password=SecretStr(server.password),
                known_hosts=server.known_hosts,
                root_prefix=server.root_prefix,
            )
            async with SFTPStorage(config) as instance:
                yield instance
