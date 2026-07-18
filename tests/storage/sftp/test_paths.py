import pytest

from app.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config
from tests.support.ids import uid


@pytest.mark.integration
async def test_paths_metadata_listing_and_walk(sftp_server: SFTPServerInfo) -> None:
    root = f"/paths-{uid()}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.mkdir(f"{root}/b", parents=True)
        await storage.mkdir(f"{root}/a", parents=True)
        await storage.upload_bytes(b"payload", f"{root}/a/file.txt")
        assert await storage.exists(root)
        assert await storage.is_dir(root)
        assert not await storage.is_file(root)
        assert await storage.is_file(f"{root}/a/file.txt")
        assert [item.name async for item in storage.iterdir(root)] == ["a", "b"]
        walked = [item async for item in storage.walk(root)]
        assert [current for current, _, _ in walked] == [root, f"{root}/a", f"{root}/b"]
        file_info = await storage.stat(f"{root}/a/file.txt")
        assert file_info.size == 7
        assert file_info.modified is not None
        await storage.rmtree(root)


@pytest.mark.integration
@pytest.mark.parametrize("path", ["/a/../b", "bad\x00path"])
async def test_rejects_unsafe_paths(sftp_server: SFTPServerInfo, path: str) -> None:
    async with SFTPStorage(make_config(sftp_server)) as storage:
        with pytest.raises(ValueError, match="SFTP path must not contain"):
            await storage.exists(path)


def test_identity_excludes_secrets(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    assert storage.id == f"sftp:{sftp_server.username}@{sftp_server.host}:{sftp_server.port}/"
    assert sftp_server.password not in storage.id
    assert sftp_server.password not in storage.cache_identity
    assert str(sftp_server.known_hosts) not in storage.cache_identity
