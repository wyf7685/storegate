import pytest

from app.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config
from tests.support.ids import uid


@pytest.mark.integration
async def test_sftp_end_to_end_smoke(sftp_server: SFTPServerInfo) -> None:
    root = f"/smoke-{uid()}"
    async with SFTPStorage(make_config(sftp_server, chunk_size=3)) as storage:
        await storage.mkdir(f"{root}/files", parents=True)
        await storage.upload_bytes(b"abcdefgh", f"{root}/files/source.bin")

        info = await storage.stat(f"{root}/files/source.bin")
        assert info.size == 8
        assert [item.name async for item in storage.iterdir(f"{root}/files")] == ["source.bin"]
        assert [walk_entry.path async for walk_entry in storage.walk(root)] == [root, f"{root}/files"]

        offset_data = b"".join([chunk async for chunk in storage.download_stream(f"{root}/files/source.bin", offset=3)])
        assert offset_data == b"defgh"

        await storage.copy(f"{root}/files/source.bin", f"{root}/files/copied.bin")
        await storage.move(f"{root}/files/copied.bin", f"{root}/files/moved.bin")
        assert await storage.download_bytes(f"{root}/files/moved.bin") == b"abcdefgh"

        await storage.upload_bytes(b"new", f"{root}/tree-source/nested/file.bin")
        await storage.upload_bytes(b"old", f"{root}/tree-destination/nested/file.bin")
        await storage.upload_bytes(b"keep", f"{root}/tree-destination/keep.bin")
        await storage.copytree(f"{root}/tree-source", f"{root}/tree-destination")
        assert await storage.download_bytes(f"{root}/tree-destination/nested/file.bin") == b"new"
        assert await storage.download_bytes(f"{root}/tree-destination/keep.bin") == b"keep"

        stream = storage.download_stream(f"{root}/files/source.bin")
        assert await anext(stream) == b"abc"
        await stream.aclose()
        assert await storage.exists(f"{root}/files/source.bin")

        temporary_names = [
            entry.name
            async for walk_entry in storage.walk(root)
            for entry in walk_entry.entries
            if ".storegate-" in entry.name
        ]
        assert temporary_names == []
        await storage.rmtree(root)
        assert not await storage.exists(root)
