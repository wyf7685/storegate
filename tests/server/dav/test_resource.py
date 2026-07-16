"""State and thread-bridge tests for DAV resources."""

from collections.abc import AsyncIterable, AsyncIterator
from unittest.mock import MagicMock

import anyio
import anyio.lowlevel
import anyio.to_thread
import pytest
from pytest_mock import MockerFixture

from app.server.dav.resource import ResourceReader, ResourceWriter, StorageResource
from app.server.dav.utils import current_event_loop_token
from app.storage.memory import MemoryStorage


@pytest.fixture
async def dav_thread_bridge() -> AsyncIterator[None]:
    reset_token = current_event_loop_token.set(anyio.lowlevel.current_token())
    try:
        yield
    finally:
        current_event_loop_token.reset(reset_token)


pytestmark = pytest.mark.usefixtures("dav_thread_bridge")


async def test_reader_seek_buffer_close_and_closed_state() -> None:
    storage = MemoryStorage("/")
    await storage.upload_bytes(b"abcdef", "/file.bin")
    reader = ResourceReader(storage, "/file.bin")
    reader.seek(1)

    assert await anyio.to_thread.run_sync(reader.read, 2) == b"bc"
    with pytest.raises(ValueError, match="Cannot seek"):
        reader.seek(0)
    assert await anyio.to_thread.run_sync(reader.read, 10) == b"def"

    await anyio.to_thread.run_sync(reader.close)
    await anyio.to_thread.run_sync(reader.close)
    with pytest.raises(ValueError, match="closed file"):
        await anyio.to_thread.run_sync(reader.read, 1)


async def test_writer_streams_chunks_and_rejects_writes_after_close() -> None:
    received = bytearray()

    async def upload(stream: AsyncIterable[bytes]) -> None:
        async for chunk in stream:
            received.extend(chunk)

    writer = ResourceWriter(upload)
    await anyio.to_thread.run_sync(writer.start)
    await anyio.to_thread.run_sync(writer.write, b"first")
    await anyio.to_thread.run_sync(writer.write, b"second")
    await anyio.to_thread.run_sync(writer.close)

    assert received == b"firstsecond"
    await anyio.to_thread.run_sync(writer.close)
    with pytest.raises(ValueError, match="closed file"):
        await anyio.to_thread.run_sync(writer.write, b"late")


async def test_writer_abort_cancels_active_upload() -> None:
    upload_started = anyio.Event()
    upload_finished = anyio.Event()

    async def upload(stream: AsyncIterable[bytes]) -> None:
        upload_started.set()
        try:
            async for _chunk in stream:
                pass
        finally:
            upload_finished.set()

    writer = ResourceWriter(upload)
    await anyio.to_thread.run_sync(writer.abort)
    await anyio.to_thread.run_sync(writer.start)
    await upload_started.wait()
    await anyio.to_thread.run_sync(writer.abort)
    with anyio.fail_after(1):
        await upload_finished.wait()
    await anyio.to_thread.run_sync(writer.close)


def test_storage_resource_write_state_transitions(mocker: MockerFixture) -> None:
    storage = MemoryStorage("/")
    resource = StorageResource("/file.bin", {"wsgidav.provider": MagicMock()}, storage)
    writer = MagicMock(spec=ResourceWriter)
    writer_cls = mocker.patch("app.server.dav.resource.ResourceWriter", return_value=writer)

    with pytest.raises(RuntimeError, match="No write operation"):
        resource.end_write(with_errors=False)

    assert resource.begin_write() is writer
    writer.start.assert_called_once_with()
    writer_cls.assert_called_once()
    with pytest.raises(RuntimeError, match="already in progress"):
        resource.begin_write()

    resource.end_write(with_errors=True)
    writer.abort.assert_called_once_with()
    writer.close.assert_called_once_with()

    writer.reset_mock()
    resource.begin_write()
    resource.end_write(with_errors=False)
    writer.abort.assert_not_called()
    writer.close.assert_called_once_with()
