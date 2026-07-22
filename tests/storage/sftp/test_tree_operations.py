import pytest

from storegate.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config
from tests.support.ids import uid


@pytest.mark.integration
async def test_copytree_overwrite_merge(sftp_server: SFTPServerInfo) -> None:
    token = uid()
    source = f"/source-{token}"
    destination = f"/destination-{token}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.upload_bytes(b"new", f"{source}/nested/file.bin")
        await storage.upload_bytes(b"old", f"{destination}/nested/file.bin")
        await storage.upload_bytes(b"keep", f"{destination}/unrelated.bin")
        await storage.copytree(source, destination)
        assert await storage.download_bytes(f"{destination}/nested/file.bin") == b"new"
        assert await storage.download_bytes(f"{destination}/unrelated.bin") == b"keep"
        assert await storage.exists(source)
        names = [item.name async for item in storage.iterdir(f"{destination}/nested")]
        assert not any(".storegate-" in name for name in names)
        await storage.rmtree(source)
        await storage.rmtree(destination)


@pytest.mark.integration
async def test_movetree_fast_path_and_merge(sftp_server: SFTPServerInfo) -> None:
    token = uid()
    source = f"/move-source-{token}"
    fast_destination = f"/move-fast-{token}"
    merge_source = f"/merge-source-{token}"
    merge_destination = f"/merge-destination-{token}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.upload_bytes(b"fast", f"{source}/file.bin")
        await storage.movetree(source, fast_destination)
        assert not await storage.exists(source)
        assert await storage.download_bytes(f"{fast_destination}/file.bin") == b"fast"

        await storage.upload_bytes(b"new", f"{merge_source}/nested/file.bin")
        await storage.upload_bytes(b"old", f"{merge_destination}/nested/file.bin")
        await storage.upload_bytes(b"keep", f"{merge_destination}/keep.bin")
        await storage.movetree(merge_source, merge_destination)
        assert not await storage.exists(merge_source)
        assert await storage.download_bytes(f"{merge_destination}/nested/file.bin") == b"new"
        assert await storage.download_bytes(f"{merge_destination}/keep.bin") == b"keep"
        await storage.rmtree(fast_destination)
        await storage.rmtree(merge_destination)


@pytest.mark.integration
async def test_copytree_conflict_and_descendant_rejection(sftp_server: SFTPServerInfo) -> None:
    source = f"/tree-{uid()}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.upload_bytes(b"data", f"{source}/file.bin")
        with pytest.raises(FileExistsError):
            await storage.copytree(source, source)
        with pytest.raises(ValueError, match="Destination must not be inside"):
            await storage.copytree(source, f"{source}/child")
        await storage.rmtree(source)
