import functools
import threading
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from typing import Any, Protocol, final, override

import anyio
from wsgidav import dav_error
from wsgidav.dav_provider import DAVNonCollection

from storegate.storage import AbstractStorage, FileInfo

from .utils import (
    NativeHandlerResult,
    call_with_catch,
    current_event_loop_token,
    raise_with_catch,
    reject_hidden_destination,
    require_visible_file,
    run_async,
)


class DAVReader(Protocol):  # pragma: no cover
    def read(self, size: int) -> bytes: ...
    def seek(self, offset: int) -> None: ...
    def close(self) -> None: ...


class DAVWriter(Protocol):  # pragma: no cover
    def write(self, data: bytes) -> None: ...
    def close(self) -> None: ...


class ResourceReader:
    def __init__(self, storage: AbstractStorage, path: str) -> None:
        self._storage = storage
        self._path = path
        self._offset = 0
        self._buffer = bytearray()
        self._closed = False
        self._read_started = False
        self._agen: AsyncGenerator[bytes] | None = None

    def seek(self, offset: int) -> None:
        if self._read_started:
            raise ValueError("Cannot seek after reading has started")
        self._offset = offset

    async def _read_impl(self, size: int) -> bytes:
        self._read_started = True
        if size == 0:
            return b""
        if self._agen is None:
            await require_visible_file(self._storage, self._path)
        if self._agen is None:
            self._agen = self._storage.download_stream(self._path, offset=self._offset)

        if size < 0:
            while True:
                try:
                    chunk = await anext(self._agen)
                except StopAsyncIteration:
                    break
                self._buffer.extend(chunk)
            result = bytes(self._buffer)
            self._buffer.clear()
            return result

        while len(self._buffer) < size:
            try:
                chunk = await anext(self._agen)
            except StopAsyncIteration:
                break
            self._buffer.extend(chunk)
        result = self._buffer[:size]
        del self._buffer[:size]
        return bytes(result)

    def read(self, size: int) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return run_async(self._read_impl, size)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._agen is not None:
            run_async(self._agen.aclose)


class ResourceWriter:
    def __init__(
        self,
        upload_fn: Callable[[AsyncIterable[bytes]], Awaitable[object]],
    ) -> None:
        self._send, self._recv = anyio.create_memory_object_stream[bytes](2)
        self._closed = False
        self._upload_fn = upload_fn
        self._token = current_event_loop_token.get()
        self._scope = None
        self._scope_lock = threading.Lock()
        self._worker_thread = threading.Thread(
            target=self._run,
            name="ResourceWriterWorker",
            daemon=True,
        )
        self._worker_ready = threading.Event()

    def write(self, data: bytes) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        self._worker_ready.wait()
        run_async(self._send.send, data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        run_async(self._send.aclose)
        self._worker_thread.join()

    def _run(self) -> None:
        with current_event_loop_token.set(self._token):
            run_async(self._arun)

    async def _arun(self) -> None:
        try:
            with anyio.CancelScope() as scope, self._recv:
                with self._scope_lock:
                    self._scope = scope
                self._worker_ready.set()
                await self._upload_fn(self._recv)
        finally:
            with self._scope_lock:
                self._scope = None

    def start(self) -> None:
        self._worker_thread.start()
        self._worker_ready.wait()

    async def _abort(self) -> None:
        with self._scope_lock:
            scope = self._scope
        if scope is not None:
            scope.cancel()

    def abort(self) -> None:
        run_async(self._abort)


@final
class StorageResource(DAVNonCollection):
    @override
    def __init__(
        self,
        path: str,
        environ: dict[str, Any],
        storage: AbstractStorage,
        *,
        read_only: bool = False,
    ) -> None:
        super().__init__(path, environ)
        self._storage = storage
        self._info: FileInfo | None = None
        self._writer: ResourceWriter | None = None
        self._read_only = read_only

    def _get_file_info(self) -> FileInfo:
        if self._info is None:
            self._info = run_async(require_visible_file, self._storage, self.path)
        return self._info

    @override
    def get_content_length(self) -> int:
        return self._get_file_info().size

    @override
    def get_creation_date(self) -> float | None:
        info = self._get_file_info()
        return info.created.timestamp() if info.created else None

    @override
    def get_etag(self) -> str | None:
        info = self._get_file_info()
        if info.modified is None:
            return None
        return f"{info.size}-{int(info.modified.timestamp())}"

    @override
    def get_last_modified(self) -> float | None:
        info = self._get_file_info()
        return info.modified.timestamp() if info.modified else None

    @override
    def support_ranges(self) -> bool:
        return True

    @override
    def support_content_length(self) -> bool:
        return True

    @override
    def support_etag(self) -> bool:
        return True

    @override
    def support_modified(self) -> bool:
        return True

    @override
    def get_content(self) -> DAVReader:
        run_async(require_visible_file, self._storage, self.path)
        return ResourceReader(self._storage, self.path)

    async def _upload_visible(self, stream: AsyncIterable[bytes]) -> None:
        await require_visible_file(self._storage, self.path)
        await raise_with_catch(
            self.path,
            functools.partial(self._storage.upload_stream, stream, remote_path=self.path, overwrite=True),
        )

    @override
    def begin_write(self, *, content_type: object = None) -> DAVWriter:
        if self._read_only:
            raise dav_error.DAVError(dav_error.HTTP_FORBIDDEN, "Server is read-only")
        if self._writer is not None:
            raise RuntimeError("Write operation already in progress.")

        run_async(require_visible_file, self._storage, self.path)
        self._writer = ResourceWriter(self._upload_visible)
        self._writer.start()
        return self._writer

    @override
    def end_write(self, *, with_errors: bool) -> None:
        if self._writer is None:
            return
        if with_errors:
            self._writer.abort()
        self._writer.close()
        self._writer = None

    async def _delete_visible(self) -> None:
        if self._read_only:
            raise dav_error.DAVError(dav_error.HTTP_FORBIDDEN, "Server is read-only")
        await require_visible_file(self._storage, self.path)
        await self._storage.unlink(self.path, missing_ok=False)

    async def _copy_visible(self, dest_path: str) -> None:
        if self._read_only:
            raise dav_error.DAVError(dav_error.HTTP_FORBIDDEN, "Server is read-only")
        await require_visible_file(self._storage, self.path)
        await reject_hidden_destination(self._storage, dest_path)
        await self._storage.copy(self.path, dest_path)

    async def _move_visible(self, dest_path: str) -> None:
        if self._read_only:
            raise dav_error.DAVError(dav_error.HTTP_FORBIDDEN, "Server is read-only")
        await require_visible_file(self._storage, self.path)
        await reject_hidden_destination(self._storage, dest_path)
        await self._storage.move(self.path, dest_path)

    @override
    def handle_delete(self) -> NativeHandlerResult:
        return run_async(call_with_catch, self, self._delete_visible)

    @override
    def handle_copy(self, dest_path: str, *, depth_infinity: bool) -> NativeHandlerResult:
        return run_async(call_with_catch, self, functools.partial(self._copy_visible, dest_path))

    @override
    def handle_move(self, dest_path: str) -> NativeHandlerResult:
        return run_async(call_with_catch, self, functools.partial(self._move_visible, dest_path))

    @override
    def copy_move_single(self, dest_path: str, *, is_move: bool) -> None:
        if self._read_only:
            raise dav_error.DAVError(dav_error.HTTP_FORBIDDEN, "Server is read-only")
        run_async(require_visible_file, self._storage, self.path)
        run_async(reject_hidden_destination, self._storage, dest_path)
        operation = self._storage.move if is_move else self._storage.copy
        run_async(raise_with_catch, self.path, functools.partial(operation, self.path, dest_path))

    @override
    def support_recursive_move(self, dest_path: str) -> bool:
        return False
