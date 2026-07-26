"""State and thread-bridge tests for DAV resources."""

from collections.abc import AsyncIterable, AsyncIterator
from unittest.mock import MagicMock

import anyio
import anyio.lowlevel
import anyio.to_thread
import pytest
from pytest_mock import MockerFixture

from storegate.server.dav.resource import ResourceReader, ResourceWriter, StorageResource
from storegate.server.dav.utils import current_event_loop_token

from ._storage import SymlinkTrapStorage


@pytest.fixture
async def dav_thread_bridge() -> AsyncIterator[None]:
    reset_token = current_event_loop_token.set(anyio.lowlevel.current_token())
    try:
        yield
    finally:
        current_event_loop_token.reset(reset_token)


pytestmark = pytest.mark.usefixtures("dav_thread_bridge")


async def test_reader_seek_buffer_close_and_closed_state() -> None:
    storage = SymlinkTrapStorage()
    storage.files["/file.bin"] = b"abcdef"
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


async def test_reader_negative_size_reads_to_eof_and_zero_is_non_consuming() -> None:
    storage = SymlinkTrapStorage()
    storage.files["/file.bin"] = b"abcdef"

    reader = ResourceReader(storage, "/file.bin")
    assert await anyio.to_thread.run_sync(reader.read, 0) == b""
    with pytest.raises(ValueError, match="Cannot seek"):
        reader.seek(1)
    assert await anyio.to_thread.run_sync(reader.read, 2) == b"ab"
    assert await anyio.to_thread.run_sync(reader.read, -1) == b"cdef"
    assert await anyio.to_thread.run_sync(reader.read, -1) == b""
    await anyio.to_thread.run_sync(reader.close)


async def test_reader_seek_then_negative_size_reads_remaining_file() -> None:
    storage = SymlinkTrapStorage()
    storage.files["/file.bin"] = b"abcdef"
    reader = ResourceReader(storage, "/file.bin")
    reader.seek(3)

    assert await anyio.to_thread.run_sync(reader.read, -1) == b"def"
    await anyio.to_thread.run_sync(reader.close)


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


async def test_writer_close_raises_commit_failure_after_stream_drained() -> None:
    """A commit-time failure must reach close(), not die with the worker thread.

    Regression: upload_fn runs on the writer's own thread, so an error raised
    after the stream was consumed (S3 CompleteMultipartUpload, an index commit)
    was swallowed and close() returned None -- wsgidav then answered the PUT
    with 201/204 for an upload that never landed.
    """
    received = bytearray()

    async def upload(stream: AsyncIterable[bytes]) -> None:
        async for chunk in stream:
            received.extend(chunk)
        raise OSError("simulated commit failure")

    writer = ResourceWriter(upload)
    await anyio.to_thread.run_sync(writer.start)
    await anyio.to_thread.run_sync(writer.write, b"payload")
    with pytest.raises(OSError, match="simulated commit failure"):
        await anyio.to_thread.run_sync(writer.close)

    # The whole body still reached upload_fn; only the commit failed.
    assert received == b"payload"


async def test_writer_close_raises_failure_from_mid_stream() -> None:
    """A failure while the stream is still being consumed also surfaces."""

    async def upload(stream: AsyncIterable[bytes]) -> None:
        async for _chunk in stream:
            raise RuntimeError("simulated mid-stream failure")

    writer = ResourceWriter(upload)
    await anyio.to_thread.run_sync(writer.start)
    await anyio.to_thread.run_sync(writer.write, b"payload")
    with pytest.raises(RuntimeError, match="simulated mid-stream failure"):
        await anyio.to_thread.run_sync(writer.close)


async def test_writer_abort_suppresses_upload_error_on_close() -> None:
    """end_write(with_errors=True) aborts then closes; the teardown is not an error."""
    upload_started = anyio.Event()

    async def upload(stream: AsyncIterable[bytes]) -> None:
        upload_started.set()
        async for _chunk in stream:
            pass
        raise OSError("commit failure the caller already gave up on")

    writer = ResourceWriter(upload)
    await anyio.to_thread.run_sync(writer.start)
    await upload_started.wait()
    await anyio.to_thread.run_sync(writer.abort)
    # Must not raise: the caller requested the abort.
    await anyio.to_thread.run_sync(writer.close)


async def test_storage_resource_write_state_transitions(mocker: MockerFixture) -> None:
    storage = SymlinkTrapStorage()
    storage.files["/file.bin"] = b"original"
    resource = StorageResource("/file.bin", {"wsgidav.provider": MagicMock()}, storage)
    writer = MagicMock(spec=ResourceWriter)
    writer_cls = mocker.patch("storegate.server.dav.resource.ResourceWriter", return_value=writer)
    # end_write now tolerates a missing writer (returns silently).
    await anyio.to_thread.run_sync(lambda: resource.end_write(with_errors=False))

    assert await anyio.to_thread.run_sync(resource.begin_write) is writer
    writer.start.assert_called_once_with()
    writer_cls.assert_called_once()
    with pytest.raises(RuntimeError, match="already in progress"):
        await anyio.to_thread.run_sync(resource.begin_write)

    await anyio.to_thread.run_sync(lambda: resource.end_write(with_errors=True))
    writer.abort.assert_called_once_with()
    writer.close.assert_called_once_with()

    writer.reset_mock()
    await anyio.to_thread.run_sync(resource.begin_write)
    await anyio.to_thread.run_sync(lambda: resource.end_write(with_errors=False))
    writer.abort.assert_not_called()
    writer.close.assert_called_once_with()
