import pytest

from app.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config
from tests.support.ids import uid


@pytest.mark.integration
async def test_mkdir_delete_and_rmtree_semantics(sftp_server: SFTPServerInfo) -> None:
    root = f"/mutations-{uid()}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.mkdir(f"{root}/nested", parents=True)
        with pytest.raises(FileExistsError):
            await storage.mkdir(f"{root}/nested")
        await storage.upload_bytes(b"data", f"{root}/nested/file.bin")
        with pytest.raises(IsADirectoryError):
            await storage.unlink(f"{root}/nested")
        with pytest.raises(OSError, match="Directory not empty"):
            await storage.rmdir(f"{root}/nested")
        await storage.unlink(f"{root}/nested/file.bin")
        await storage.unlink(f"{root}/nested/file.bin", missing_ok=True)
        await storage.rmdir(f"{root}/nested")
        await storage.rmtree(root)
        assert not await storage.exists(root)


@pytest.mark.integration
async def test_copy_and_move_overwrite(sftp_server: SFTPServerInfo) -> None:
    root = f"/files-{uid()}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.mkdir(root)
        await storage.upload_bytes(b"source", f"{root}/source.bin")
        await storage.upload_bytes(b"old", f"{root}/target.bin")
        await storage.copy(f"{root}/source.bin", f"{root}/target.bin")
        assert await storage.download_bytes(f"{root}/target.bin") == b"source"
        with pytest.raises(FileExistsError):
            await storage.copy(f"{root}/source.bin", f"{root}/target.bin", overwrite=False)
        await storage.move(f"{root}/source.bin", f"{root}/moved.bin")
        assert not await storage.exists(f"{root}/source.bin")
        assert await storage.download_bytes(f"{root}/moved.bin") == b"source"
        await storage.upload_bytes(b"replacement", f"{root}/replacement.bin")
        await storage.move(f"{root}/replacement.bin", f"{root}/moved.bin")
        assert await storage.download_bytes(f"{root}/moved.bin") == b"replacement"
        await storage.rmtree(root)


@pytest.mark.integration
async def test_same_path_file_semantics(sftp_server: SFTPServerInfo) -> None:
    path = f"/same-{uid()}.bin"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.upload_bytes(b"data", path)
        await storage.copy(path, path)
        await storage.move(path, path)
        with pytest.raises(FileExistsError):
            await storage.copy(path, path, overwrite=False)
        with pytest.raises(FileExistsError):
            await storage.move(path, path, overwrite=False)
        await storage.unlink(path)
