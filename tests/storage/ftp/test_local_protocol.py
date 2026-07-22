"""FTP client integration against aioftp's local filesystem server."""

from collections.abc import AsyncIterator
from pathlib import Path, PurePosixPath

import aioftp
import pytest

from storegate.storage import EntryKind, WalkEntry
from storegate.storage.ftp import FTPConfig, FTPStorage

pytestmark = pytest.mark.integration


@pytest.fixture
async def local_protocol_storage(tmp_path: Path) -> AsyncIterator[FTPStorage]:
    server = aioftp.Server([aioftp.User("storegate", "secret", base_path=tmp_path)])
    await server.start("127.0.0.1", 0)
    storage = FTPStorage(
        FTPConfig(
            host="127.0.0.1",
            port=server.server_port,
            username="storegate",
            password="secret",
            chunk_size=4,
            max_connections=2,
        )
    )
    try:
        async with storage:
            yield storage
    finally:
        await server.close()


async def test_local_protocol_metadata_streaming_and_trees(local_protocol_storage: FTPStorage) -> None:
    storage = local_protocol_storage
    await storage.upload_bytes(b"root", "/source/root.txt")
    await storage.upload_bytes(b"nested", "/source/sub/nested.txt")

    source_info = await storage.stat("/source")
    file_info = await storage.stat("/source/root.txt")
    assert source_info.kind is EntryKind.DIRECTORY
    assert file_info.kind is EntryKind.FILE
    assert await storage.download_bytes("/source/root.txt") == b"root"

    entries = await storage.list_("/source")
    assert [(entry.name, entry.kind) for entry in entries] == [
        ("root.txt", EntryKind.FILE),
        ("sub", EntryKind.DIRECTORY),
    ]

    walked = [entry async for entry in storage.walk("/source")]
    assert all(isinstance(entry, WalkEntry) for entry in walked)
    assert [entry.path for entry in walked] == ["/source", "/source/sub"]

    await storage.copy("/source/root.txt", "/copy.txt")
    await storage.move("/copy.txt", "/moved.txt")
    await storage.copytree("/source", "/destination")
    await storage.upload_bytes(b"keep", "/merge/keep.txt")
    await storage.movetree("/destination", "/merge")
    assert await storage.download_bytes("/moved.txt") == b"root"
    assert await storage.download_bytes("/merge/keep.txt") == b"keep"
    assert await storage.download_bytes("/merge/sub/nested.txt") == b"nested"

    await storage.rmtree("/source")
    await storage.rmtree("/merge")
    await storage.unlink("/moved.txt")
    assert not await storage.exists("/source")
    assert not await storage.exists("/merge")
    assert not await storage.exists("/moved.txt")


async def test_local_protocol_copytree_rollback_preserves_destination(
    local_protocol_storage: FTPStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = local_protocol_storage
    await storage.upload_bytes(b"new-a", "/source/a.txt")
    await storage.upload_bytes(b"new-b", "/source/b.txt")
    await storage.upload_bytes(b"old-a", "/destination/a.txt")
    await storage.upload_bytes(b"keep", "/destination/keep.txt")
    original_copy_stream = FTPStorage._copy_stream

    async def fail_second_copy(
        self: FTPStorage,
        source_client: aioftp.Client,
        destination_client: aioftp.Client,
        source: PurePosixPath,
        destination: PurePosixPath,
    ) -> None:
        if source.name == "b.txt":
            raise OSError("injected copy failure")
        await original_copy_stream(self, source_client, destination_client, source, destination)

    monkeypatch.setattr(FTPStorage, "_copy_stream", fail_second_copy)
    try:
        with pytest.raises(OSError, match="injected copy failure"):
            await storage.copytree("/source", "/destination", overwrite=True)
        assert await storage.download_bytes("/destination/a.txt") == b"old-a"
        assert await storage.download_bytes("/destination/keep.txt") == b"keep"
        assert not await storage.exists("/destination/b.txt")
        assert await storage.download_bytes("/source/a.txt") == b"new-a"
        assert await storage.download_bytes("/source/b.txt") == b"new-b"
        names = {entry.name for entry in await storage.list_("/destination")}
        assert not any(name.startswith(".storegate-copytree-") for name in names)
    finally:
        monkeypatch.setattr(FTPStorage, "_copy_stream", original_copy_stream)
        await storage.rmtree("/source")
        await storage.rmtree("/destination")
