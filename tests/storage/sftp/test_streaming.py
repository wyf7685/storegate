from __future__ import annotations

import contextlib

import pytest

from storegate.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config


@pytest.mark.integration
async def test_streaming_metadata_and_offset_download(sftp_server: SFTPServerInfo) -> None:
    async with SFTPStorage(make_config(sftp_server, chunk_size=3)) as storage:
        await storage.mkdir("/folder", parents=True)
        await storage.upload_bytes(b"abcdefgh", "/folder/file.bin")
        info = await storage.stat("/folder/file.bin")
        assert info.path == "/folder/file.bin"
        assert info.size == 8
        assert await storage.download_bytes("/folder/file.bin") == b"abcdefgh"
        chunks = [chunk async for chunk in storage.download_stream("/folder/file.bin", offset=3)]
        assert b"".join(chunks) == b"defgh"
        for offset in (8, 9):
            assert [chunk async for chunk in storage.download_stream("/folder/file.bin", offset=offset)] == []
        with pytest.raises(ValueError, match="non-negative"):
            await anext(storage.download_stream("/folder/file.bin", offset=-1))
        assert [entry.name async for entry in storage.iterdir("/folder")] == ["file.bin"]


@pytest.mark.integration
async def test_download_early_close_releases_channel(sftp_server: SFTPServerInfo) -> None:
    async with SFTPStorage(make_config(sftp_server, max_channels=1, chunk_size=2)) as storage:
        await storage.upload_bytes(b"abcdef", "/file.bin")
        stream = storage.download_stream("/file.bin")
        assert await anext(stream) == b"ab"
        await stream.aclose()
        assert await storage.exists("/file.bin")


@pytest.mark.integration
async def test_staged_overwrite_and_cleanup(sftp_server: SFTPServerInfo) -> None:
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.upload_bytes(b"old", "/target.bin")
        await storage.upload_bytes(b"new", "/target.bin")
        assert await storage.download_bytes("/target.bin") == b"new"
        with pytest.raises(FileExistsError):
            await storage.upload_bytes(b"blocked", "/target.bin", overwrite=False)
        names = [entry.name async for entry in storage.iterdir("/")]
        assert not any(".storegate-" in name for name in names)
        with contextlib.suppress(FileNotFoundError):
            await storage.unlink("/target.bin")
