"""FTPStorage behavior tests."""

from collections.abc import AsyncIterator
from datetime import UTC
from pathlib import PurePosixPath

import anyio
import pytest

from storegate.storage.ftp import FTPConfig, FTPStorage
from tests.support.ids import uid

pytestmark = pytest.mark.integration


class TestMetadataAndStreaming:
    def test_fact_conversion_uses_utc_and_rejects_unknown_type(self) -> None:
        storage = FTPStorage(FTPConfig(host="ftp.example.test"))
        info = storage._file_info_from_facts(
            PurePosixPath("/file.txt"),
            {
                "type": "file",
                "size": "12",
                "modify": "20260102030405.123",
                "create": "20250102030405",
            },
        )
        assert info.size == 12
        assert info.modified is not None
        assert info.modified.tzinfo is UTC
        assert info.created is not None
        assert info.created.tzinfo is UTC

        with pytest.raises(OSError, match="Unsupported FTP entry type"):
            storage._file_info_from_facts(PurePosixPath("/link"), {"type": "slink", "size": "0"})

    async def test_empty_multichunk_and_offsets(self, ftp_storage: FTPStorage) -> None:
        base = f"/ftp-stream-{uid()}"
        await ftp_storage.upload_bytes(b"", f"{base}/empty.bin")
        await ftp_storage.upload_bytes(b"0123456789", f"{base}/data.bin")
        try:
            assert await ftp_storage.download_bytes(f"{base}/empty.bin") == b""
            assert await ftp_storage.download_bytes(f"{base}/data.bin") == b"0123456789"
            chunks = [chunk async for chunk in ftp_storage.download_stream(f"{base}/data.bin", offset=4)]
            assert b"".join(chunks) == b"456789"
            beyond = [chunk async for chunk in ftp_storage.download_stream(f"{base}/data.bin", offset=99)]
            assert beyond == []
            with pytest.raises(ValueError, match="non-negative"):
                await anext(ftp_storage.download_stream(f"{base}/data.bin", offset=-1))
        finally:
            await ftp_storage.rmtree(base)

    async def test_early_download_close_releases_client(self, ftp_storage: FTPStorage) -> None:
        path = f"/ftp-close-{uid()}.bin"
        await ftp_storage.upload_bytes(b"abcdefgh", path)
        try:
            stream = ftp_storage.download_stream(path)
            assert await anext(stream) == b"abcd"
            await stream.aclose()
            with anyio.fail_after(1):
                assert (await ftp_storage.stat(path)).size == 8
        finally:
            await ftp_storage.unlink(path, missing_ok=True)

    async def test_listing_snapshot_does_not_hold_lock(self, ftp_storage: FTPStorage) -> None:
        base = f"/ftp-list-{uid()}"
        await ftp_storage.upload_bytes(b"a", f"{base}/a.txt")
        await ftp_storage.upload_bytes(b"b", f"{base}/b.txt")
        try:
            async for entry in ftp_storage.iterdir(base):
                with anyio.fail_after(1):
                    assert await ftp_storage.stat(entry.path) == entry
        finally:
            await ftp_storage.rmtree(base)

    async def test_pool_size_one_serializes_upload_and_stat(self, ftp_storage: FTPStorage) -> None:
        path = f"/ftp-lock-{uid()}.bin"
        producer_started = anyio.Event()
        release_producer = anyio.Event()

        async def producer() -> AsyncIterator[bytes]:
            yield b"first"
            producer_started.set()
            await release_producer.wait()
            yield b"second"

        async def upload() -> None:
            await ftp_storage.upload_stream(producer(), path)

        try:
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(upload)
                await producer_started.wait()
                with anyio.move_on_after(0.05) as scope:
                    await ftp_storage.stat("/")
                assert scope.cancel_called
                release_producer.set()
            assert await ftp_storage.download_bytes(path) == b"firstsecond"
        finally:
            release_producer.set()
            await ftp_storage.unlink(path, missing_ok=True)

    async def test_pool_size_two_allows_concurrent_uploads(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port, chunk_size=4, max_connections=2))
        release_producers = anyio.Event()
        first_started = anyio.Event()
        second_started = anyio.Event()
        first_path = f"/ftp-pool-first-{uid()}.bin"
        second_path = f"/ftp-pool-second-{uid()}.bin"

        async def producer(started: anyio.Event) -> AsyncIterator[bytes]:
            yield b"first"
            started.set()
            await release_producers.wait()
            yield b"second"

        async def upload(path: str, started: anyio.Event) -> None:
            await storage.upload_stream(producer(started), path)

        async with storage:
            try:
                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(upload, first_path, first_started)
                    task_group.start_soon(upload, second_path, second_started)
                    with anyio.fail_after(1):
                        await first_started.wait()
                        await second_started.wait()
                    assert len(storage._pool._borrowed) == 2
                    release_producers.set()
                assert await storage.download_bytes(first_path) == b"firstsecond"
                assert await storage.download_bytes(second_path) == b"firstsecond"
            finally:
                release_producers.set()
                await storage.unlink(first_path, missing_ok=True)
                await storage.unlink(second_path, missing_ok=True)

    async def test_pool_size_two_allows_stat_during_upload(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port, chunk_size=4, max_connections=2))
        producer_started = anyio.Event()
        release_producer = anyio.Event()
        path = f"/ftp-pool-stat-{uid()}.bin"

        async def producer() -> AsyncIterator[bytes]:
            yield b"first"
            producer_started.set()
            await release_producer.wait()
            yield b"second"

        async with storage:
            try:
                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(storage.upload_stream, producer(), path)
                    await producer_started.wait()
                    with anyio.fail_after(1):
                        assert (await storage.stat("/")).is_dir
                    release_producer.set()
            finally:
                release_producer.set()
                await storage.unlink(path, missing_ok=True)

    async def test_failed_transfer_invalidates_pooled_client(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port, chunk_size=4))
        path = f"/ftp-pool-failure-{uid()}.bin"

        async def failing_producer() -> AsyncIterator[bytes]:
            yield b"first"
            raise RuntimeError("injected producer failure")

        async with storage:
            with pytest.raises(RuntimeError, match="injected producer failure"):
                await storage.upload_stream(failing_producer(), path)
            assert storage._pool._total == 0
            assert (await storage.stat("/")).is_dir
            assert storage._pool._total == 1
            await storage.unlink(path, missing_ok=True)

    async def test_failed_overwrite_keeps_destination_entry(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port, chunk_size=4))
        path = f"/ftp-overwrite-failure-{uid()}.bin"

        async def failing_producer() -> AsyncIterator[bytes]:
            yield b"partial"
            raise RuntimeError("injected overwrite failure")

        async with storage:
            await storage.upload_bytes(b"original", path)
            try:
                with pytest.raises(RuntimeError, match="injected overwrite failure"):
                    await storage.upload_stream(failing_producer(), path, overwrite=True)
                assert await storage.exists(path)
            finally:
                await storage.unlink(path, missing_ok=True)

    async def test_business_error_reuses_pooled_client(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port))
        async with storage:
            client = storage._pool._idle[-1]
            with pytest.raises(FileNotFoundError):
                await storage.stat(f"/missing-{uid()}")
            assert storage._pool._idle[-1] is client
            assert storage._pool._total == 1
