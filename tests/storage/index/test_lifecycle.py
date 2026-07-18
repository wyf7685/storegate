from unittest.mock import AsyncMock

import pytest

from app.storage.abstract import BytesLike, EntryKind, FileInfo, PathLike
from app.storage.ftp import FTPConfig, FTPStorage
from app.storage.index import IndexStorage
from app.storage.index.storage import CHUNKS_INDEX_FILE
from app.storage.memory import MemoryStorage


class FailingDownloadStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(self) -> None:
        super().__init__("/")
        self.fail_download = True
        self.connect_calls = 0
        self.close_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def lstat(self, path: PathLike) -> FileInfo:
        if self.fail_download and self.normalize_path(path).as_posix() == CHUNKS_INDEX_FILE:
            return FileInfo(
                path=CHUNKS_INDEX_FILE,
                name=CHUNKS_INDEX_FILE.removeprefix("/"),
                kind=EntryKind.FILE,
            )
        return await super().lstat(path)

    async def download_bytes(self, remote_path: PathLike) -> bytes:
        if self.fail_download:
            self.fail_download = False
            raise RuntimeError("download failed")
        return await super().download_bytes(remote_path)


class FailingBindingStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(self) -> None:
        super().__init__("/")
        self.fail_upload = True
        self.connect_calls = 0
        self.close_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def upload_bytes(self, data: BytesLike, remote_path: PathLike, *, overwrite: bool = True) -> None:
        if self.fail_upload:
            self.fail_upload = False
            raise RuntimeError("binding failed")
        await super().upload_bytes(data, remote_path, overwrite=overwrite)


class RollbackStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(
        self,
        label: str = "storage",
        *,
        close_failures: int = 0,
        connect_error: BaseException | None = None,
        read_error: BaseException | None = None,
    ) -> None:
        super().__init__("/")
        self.label = label
        self.close_failures = close_failures
        self.connect_error = connect_error
        self.read_error = read_error
        self.connect_calls = 0
        self.close_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            error = self.connect_error
            self.connect_error = None
            raise error

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_failures:
            self.close_failures -= 1
            raise OSError(f"{self.label} rollback close failed")

    async def lstat(self, path: PathLike) -> FileInfo:
        if self.read_error is not None and self.normalize_path(path).as_posix() == CHUNKS_INDEX_FILE:
            return FileInfo(
                path=CHUNKS_INDEX_FILE,
                name=CHUNKS_INDEX_FILE.removeprefix("/"),
                kind=EntryKind.FILE,
            )
        return await super().lstat(path)

    async def download_bytes(self, remote_path: PathLike) -> bytes:
        if self.read_error is not None:
            error = self.read_error
            self.read_error = None
            raise error
        return await super().download_bytes(remote_path)


async def test_partial_connect_rollback_is_retryable() -> None:
    index = MemoryStorage("/")
    chunks = FailingDownloadStorage()
    storage = IndexStorage(index, chunks)

    with pytest.raises(RuntimeError, match="download failed"):
        await storage.connect()
    assert chunks.close_calls == 1
    assert storage._lifecycle_state == "NEW"

    await storage.connect()
    assert chunks.connect_calls == 2
    await storage.close()


async def test_binding_rollback_is_retryable() -> None:
    index = MemoryStorage("/")
    chunks = FailingBindingStorage()
    storage = IndexStorage(index, chunks)

    with pytest.raises(RuntimeError, match="binding failed"):
        await storage.connect()
    assert chunks.close_calls == 1
    await storage.connect()
    assert chunks.connect_calls == 2
    assert await chunks.download_bytes(CHUNKS_INDEX_FILE) == index.id.encode()
    await storage.close()


async def test_ftp_chunks_pool_is_recreated_after_index_binding_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakePool:
        def __init__(self) -> None:
            self.is_closed = False
            self.is_open = False
            self.start_calls = 0
            self.close_calls = 0

        async def start(self) -> None:
            self.start_calls += 1
            self.is_closed = False
            self.is_open = True

        async def close(self) -> None:
            self.close_calls += 1
            self.is_open = False
            self.is_closed = True

    ftp = FTPStorage(FTPConfig(host="127.0.0.1", username="u", password="p"))
    first_pool = FakePool()
    replacement_pool = FakePool()
    ftp._pool = first_pool  # ty: ignore[invalid-assignment]
    monkeypatch.setattr(ftp, "_new_pool", lambda: replacement_pool)
    monkeypatch.setattr(
        ftp,
        "lstat",
        AsyncMock(
            side_effect=[
                FileInfo(
                    path=CHUNKS_INDEX_FILE,
                    name=CHUNKS_INDEX_FILE.removeprefix("/"),
                    kind=EntryKind.FILE,
                ),
                FileNotFoundError(),
            ]
        ),
    )
    monkeypatch.setattr(ftp, "download_bytes", AsyncMock(side_effect=RuntimeError("binding read failed")))
    monkeypatch.setattr(ftp, "upload_bytes", AsyncMock())
    storage = IndexStorage(MemoryStorage("/"), ftp)

    with pytest.raises(RuntimeError, match="binding read failed"):
        await storage.connect()
    assert first_pool.close_calls == 1

    await storage.connect()
    assert replacement_pool.start_calls == 1
    await storage.close()


async def test_binding_conflict_preserves_reverse_close_errors_and_retries_cleanup() -> None:
    index = RollbackStorage("index", close_failures=1)
    chunks = RollbackStorage("chunks", close_failures=1)
    storage = IndexStorage(index, chunks)
    await chunks.upload_bytes(b"other-index", CHUNKS_INDEX_FILE)

    with pytest.raises(BaseExceptionGroup) as caught:
        await storage.connect()

    assert isinstance(caught.value.exceptions[0], RuntimeError)
    assert [str(error) for error in caught.value.exceptions[1:]] == [
        "chunks rollback close failed",
        "index rollback close failed",
    ]
    assert chunks._lifecycle_state == "CONNECTED"
    assert index._lifecycle_state == "CONNECTED"

    await chunks.upload_bytes(index.id.encode(), CHUNKS_INDEX_FILE, overwrite=True)
    await storage.connect()
    assert (chunks.connect_calls, index.connect_calls) == (2, 2)
    assert (chunks.close_calls, index.close_calls) == (2, 2)
    assert storage._pending_rollback == []
    await storage.close()


async def test_read_failure_retries_failed_chunks_cleanup_before_connecting() -> None:
    index = RollbackStorage("index")
    chunks = RollbackStorage("chunks", close_failures=1, read_error=RuntimeError("binding read failed"))
    storage = IndexStorage(index, chunks)

    with pytest.raises(BaseExceptionGroup) as caught:
        await storage.connect()

    assert [str(error) for error in caught.value.exceptions] == ["binding read failed", "chunks rollback close failed"]
    assert chunks._lifecycle_state == "CONNECTED"
    assert index._lifecycle_state == "CLOSED"

    await storage.connect()
    assert (chunks.connect_calls, index.connect_calls) == (2, 2)
    assert chunks.close_calls == 2
    assert storage._pending_rollback == []
    await storage.close()


async def test_chunks_connect_failure_retries_failed_index_cleanup_before_connecting() -> None:
    index = RollbackStorage("index", close_failures=1)
    chunks = RollbackStorage("chunks", connect_error=RuntimeError("chunks connect failed"))
    storage = IndexStorage(index, chunks)

    with pytest.raises(BaseExceptionGroup) as caught:
        await storage.connect()

    assert [str(error) for error in caught.value.exceptions] == [
        "chunks connect failed",
        "index rollback close failed",
    ]
    assert chunks.close_calls == 1
    assert chunks._lifecycle_state == "CLOSED"
    assert index._lifecycle_state == "CONNECTED"

    await storage.connect()
    assert (chunks.connect_calls, index.connect_calls) == (2, 2)
    assert (chunks.close_calls, index.close_calls) == (1, 2)
    assert storage._pending_rollback == []
    await storage.close()
