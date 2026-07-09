import functools
import threading
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from typing import Any, Protocol, final, override

import anyio
from wsgidav import dav_error
from wsgidav.dav_provider import DAVNonCollection

from app.log import escape_tag, logger
from app.storage import AbstractStorage, FileInfo

from .utils import NativeHandlerResult, run_async


class DAVReader(Protocol):
    def read(self, size: int) -> bytes: ...
    def close(self) -> None: ...


class DAVWriter(Protocol):
    def write(self, data: bytes) -> None: ...
    def close(self) -> None: ...


class ResourceReader:
    def __init__(self, agen: AsyncGenerator[bytes]) -> None:
        self._agen = agen
        self._buffer = bytearray()
        self._closed = False

    async def _read_impl(self, size: int) -> bytes:
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
        self._closed = True
        run_async(self._agen.aclose)


class ResourceWriter:
    def __init__(
        self,
        upload_fn: Callable[[AsyncIterable[bytes]], Awaitable[object]],
    ) -> None:
        self._send, self._recv = anyio.create_memory_object_stream[bytes](max_buffer_size=2)
        self._closed = False
        self._upload_fn = upload_fn
        self._scope = None
        self._scope_lock = threading.Lock()
        self._worker_thread: threading.Thread = threading.Thread(
            target=run_async,
            args=(self._arun,),
            name="ResourceWriterWorker",
            daemon=True,
        )

    def write(self, data: bytes) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        run_async(self._send.send, data)

    def close(self) -> None:
        if self._closed:
            return
        self._send.close()
        self._closed = True
        with self._scope_lock:
            self._scope = None
        self._worker_thread.join()

    async def _arun(self) -> None:
        try:
            with anyio.CancelScope() as scope, self._recv:
                with self._scope_lock:
                    self._scope = scope
                await self._upload_fn(self._recv)
        finally:
            with self._scope_lock:
                self._scope = None

    def start(self) -> None:
        self._worker_thread.start()

    def abort(self) -> None:
        with self._scope_lock:
            if self._scope is not None:
                self._scope.cancel()
                self._scope = None


@final
class StorageResource(DAVNonCollection):
    @override
    def __init__(self, path: str, environ: dict[str, Any], storage: AbstractStorage) -> None:
        super().__init__(path, environ)
        self._storage = storage
        self._info: FileInfo | None = None
        self._writer: ResourceWriter | None = None

    def _get_file_info(self) -> FileInfo:
        if self._info is None:
            self._info = run_async(self._storage.stat, self.path)
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
        return False

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
        async def download() -> AsyncGenerator[bytes]:
            async for chunk in self._storage.download_stream(self.path):
                yield chunk

        return ResourceReader(download())

    @override
    def begin_write(self, *, content_type: object = None) -> DAVWriter:
        if self._writer is not None:
            raise RuntimeError("Write operation already in progress.")

        self._writer = ResourceWriter(
            functools.partial(
                self._storage.upload_stream,
                remote_path=self.path,
                overwrite=True,
            )
        )
        self._writer.start()
        return self._writer

    @override
    def end_write(self, *, with_errors: bool) -> None:
        if self._writer is None:
            raise RuntimeError("No write operation in progress.")
        if with_errors:
            self._writer.abort()
        self._writer.close()
        self._writer = None

    async def _call_with_catch(self, func: Callable[[], Awaitable[object]]) -> NativeHandlerResult:
        error = None
        try:
            await func()
        except IsADirectoryError:
            error = dav_error.HTTP_FORBIDDEN
        except FileNotFoundError:
            error = dav_error.HTTP_NOT_FOUND
        except Exception:
            error = dav_error.HTTP_INTERNAL_ERROR
            logger.opt(colors=True).exception(f"Error in DAV operation for path <y>{escape_tag(self.path)}</>")

        return [(self.get_href(), dav_error.DAVError(error))] if error is not None else True

    @override
    def handle_delete(self) -> NativeHandlerResult:
        return run_async(self._call_with_catch, functools.partial(self._storage.unlink, self.path, missing_ok=False))

    @override
    def handle_copy(self, dest_path: str, *, depth_infinity: bool) -> NativeHandlerResult:
        return run_async(self._call_with_catch, functools.partial(self._storage.copy, self.path, dest_path))

    @override
    def handle_move(self, dest_path: str) -> NativeHandlerResult:
        return run_async(self._call_with_catch, functools.partial(self._storage.move, self.path, dest_path))

    @override
    def support_recursive_move(self, dest_path: str) -> bool:
        return False
