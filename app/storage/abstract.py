import functools
import json
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePath, PurePosixPath
from typing import Self, cast

import anyio
import anyio.lowlevel
import ayafileio

from app.log import escape_tag
from app.utils import LoggerWrapper, logger_wrapper


@dataclass(slots=True, frozen=True)
class FileInfo:
    path: str
    name: str
    is_dir: bool
    size: int = 0
    modified: datetime | None = None
    created: datetime | None = None


type BytesLike = bytes | bytearray | memoryview
type PathLike = str | PurePath

type LifecycleImplementation = Callable[["AbstractStorage"], Awaitable[None]]


def make_cache_identity(kind: str, **fields: object) -> str:
    """Build a canonical, non-secret identity for a persistent cache scope.

    Callers must whitelist only fields that identify the underlying data
    location.  Credentials and runtime-only settings must not be included.
    """
    return json.dumps({"kind": kind, **fields}, separators=(",", ":"), sort_keys=True)


_LIFECYCLE_WRAPPED_ATTRIBUTE = "__storegate_lifecycle_wrapped__"


class AbstractStorage(ABC):
    """Abstract storage interface with a concurrency-safe lifecycle."""

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        connect_impl = cls.__dict__.get("connect")
        close_impl = cls.__dict__.get("close")
        if connect_impl is not None and not getattr(connect_impl, _LIFECYCLE_WRAPPED_ATTRIBUTE, False):
            connect_callable = cast("LifecycleImplementation", connect_impl)

            @functools.wraps(connect_callable)
            async def connect(self: AbstractStorage) -> None:
                if self._lifecycle_owner == anyio.get_current_task().id:
                    await connect_callable(self)
                    return
                await self._connect_lifecycle(connect_callable)

            setattr(connect, _LIFECYCLE_WRAPPED_ATTRIBUTE, True)
            type.__setattr__(cls, "connect", connect)
        if close_impl is not None and not getattr(close_impl, _LIFECYCLE_WRAPPED_ATTRIBUTE, False):
            close_callable = cast("LifecycleImplementation", close_impl)

            @functools.wraps(close_callable)
            async def close(self: AbstractStorage) -> None:
                if self._lifecycle_owner == anyio.get_current_task().id:
                    await close_callable(self)
                    return
                await self._close_lifecycle(close_callable)

            setattr(close, _LIFECYCLE_WRAPPED_ATTRIBUTE, True)
            type.__setattr__(cls, "close", close)

    def __init__(self) -> None:
        self.__ctx = 0
        self._lifecycle_lock = anyio.Lock()
        self._lifecycle_state = "NEW"
        self._lifecycle_event: anyio.Event | None = None
        self._lifecycle_owner: int | None = None
        self._lifecycle_closed = False

    @functools.cached_property
    def log(self) -> LoggerWrapper:
        return logger_wrapper(f"{self.__class__.__name__} <c><i>{escape_tag(self.id)}</></>")

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    def cache_identity(self) -> str | None:
        return None

    @staticmethod
    def normalize_path(path: PathLike) -> PurePosixPath:
        return "/" / PurePosixPath(path)

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def ping(self) -> bool:
        raise NotImplementedError

    def _lifecycle_impl(self, name: str) -> LifecycleImplementation | None:
        for base in type(self).__mro__:
            implementation = base.__dict__.get(name)
            if implementation is not None:
                unwrapped = getattr(implementation, "__wrapped__", implementation)
                if callable(unwrapped):
                    return cast("LifecycleImplementation", unwrapped)
        return None

    async def _connect_lifecycle(self, implementation: LifecycleImplementation) -> None:
        while True:
            async with self._lifecycle_lock:
                if self._lifecycle_state == "CONNECTED":
                    return
                if self._lifecycle_state in {"CONNECTING", "CLOSING"}:
                    event = self._lifecycle_event
                else:
                    event = anyio.Event()
                    self._lifecycle_event = event
                    self._lifecycle_state = "CONNECTING"
                    self._lifecycle_owner = anyio.get_current_task().id
                    break
            assert event is not None
            await event.wait()
        try:
            await implementation(self)
        except BaseException:
            with anyio.CancelScope(shield=True):
                async with self._lifecycle_lock:
                    self._lifecycle_state = "NEW"
                    self._lifecycle_owner = None
                    event.set()
                    self._lifecycle_event = None
            raise
        with anyio.CancelScope(shield=True):
            async with self._lifecycle_lock:
                self._lifecycle_state = "CONNECTED"
                self._lifecycle_owner = None
                event.set()
                self._lifecycle_event = None

    async def _close_lifecycle(self, implementation: LifecycleImplementation) -> None:
        while True:
            async with self._lifecycle_lock:
                if self._lifecycle_state == "CLOSED":
                    self.__ctx = 0
                    return
                if self._lifecycle_state in {"CONNECTING", "CLOSING"}:
                    event = self._lifecycle_event
                else:
                    event = anyio.Event()
                    self._lifecycle_event = event
                    self._lifecycle_state = "CLOSING"
                    self._lifecycle_owner = anyio.get_current_task().id
                    break
            assert event is not None
            await event.wait()
        try:
            await implementation(self)
        except BaseException:
            with anyio.CancelScope(shield=True):
                async with self._lifecycle_lock:
                    self._lifecycle_state = "CONNECTED"
                    self._lifecycle_owner = None
                    event.set()
                    self._lifecycle_event = None
            raise
        with anyio.CancelScope(shield=True):
            async with self._lifecycle_lock:
                self._lifecycle_state = "CLOSED"
                self.__ctx = 0
                self._lifecycle_owner = None
                event.set()
                self._lifecycle_event = None

    async def __aenter__(self) -> Self:
        impl = self._lifecycle_impl("connect")
        if impl is None:
            raise RuntimeError("Storage has no connect implementation")
        connected = False
        try:
            while True:
                await self._connect_lifecycle(impl)
                connected = True
                with anyio.CancelScope(shield=True):
                    async with self._lifecycle_lock:
                        if self._lifecycle_state == "CONNECTED":
                            self.__ctx += 1
                            return self
                        event = self._lifecycle_event
                if event is not None:
                    await event.wait()
        except BaseException as primary:
            if connected:
                close_impl = self._lifecycle_impl("close")
                if close_impl is not None:
                    cleanup_error: BaseException | None = None
                    close_event: anyio.Event | None = None
                    with anyio.CancelScope(shield=True):
                        async with self._lifecycle_lock:
                            if self._lifecycle_state == "CONNECTED" and self.__ctx == 0:
                                close_event = anyio.Event()
                                self._lifecycle_event = close_event
                                self._lifecycle_state = "CLOSING"
                                self._lifecycle_owner = anyio.get_current_task().id
                        if close_event is not None:
                            try:
                                await close_impl(self)
                            except BaseException as secondary:
                                cleanup_error = secondary
                            with anyio.CancelScope(shield=True):
                                async with self._lifecycle_lock:
                                    self._lifecycle_state = "CONNECTED" if cleanup_error is not None else "CLOSED"
                                    self.__ctx = 0
                                    self._lifecycle_owner = None
                                    close_event.set()
                                    self._lifecycle_event = None
                    if cleanup_error is not None:
                        raise BaseExceptionGroup(
                            "Context registration rollback failed", [primary, cleanup_error]
                        ) from None
            raise

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        with anyio.CancelScope(shield=True):
            async with self._lifecycle_lock:
                if self.__ctx == 0:
                    return
                self.__ctx -= 1
                if self.__ctx:
                    return
                event = anyio.Event()
                self._lifecycle_event = event
                self._lifecycle_state = "CLOSING"
                self._lifecycle_owner = anyio.get_current_task().id
            impl = self._lifecycle_impl("close")
            try:
                if impl is not None:
                    await impl(self)
            except BaseException:
                async with self._lifecycle_lock:
                    self._lifecycle_state = "CONNECTED"
                    self._lifecycle_owner = None
                    event.set()
                    self._lifecycle_event = None
                raise
            async with self._lifecycle_lock:
                self._lifecycle_state = "CLOSED"
                self._lifecycle_owner = None
                event.set()
                self._lifecycle_event = None

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    @abstractmethod
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload from a binary stream."""
        raise NotImplementedError

    async def upload_bytes(
        self,
        data: BytesLike,
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload bytes."""
        buf = memoryview(data).toreadonly()

        async def aiterable() -> AsyncIterable[memoryview[int]]:
            ptr = 0
            while ptr < len(buf):
                yield buf[ptr : ptr + 8192]
                ptr += 8192
                await anyio.lowlevel.checkpoint()

        await self.upload_stream(aiterable(), remote_path, overwrite=overwrite)

    async def upload_file(
        self,
        local_path: PathLike,
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload a local file."""

        async with ayafileio.open(Path(local_path), "rb") as file:
            await self.upload_stream(file.chunk(1024 * 1024), remote_path, overwrite=overwrite)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    @abstractmethod
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        """Download as an async byte stream.

        Args:
            remote_path: Path to the file.
            offset: Byte offset to start reading from.  Default 0 (start of file).
        """
        raise NotImplementedError
        yield

    async def download_bytes(
        self,
        remote_path: PathLike,
    ) -> bytes:
        """Download as bytes."""
        buffer = bytearray()
        async for chunk in self.download_stream(remote_path):
            buffer.extend(chunk)
        return bytes(buffer)

    async def download_file(
        self,
        remote_path: PathLike,
        local_path: PathLike,
    ) -> None:
        """Download to a local file."""
        async with ayafileio.open(Path(local_path), "wb") as file:
            async for chunk in self.download_stream(remote_path):
                await file.write(chunk)

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        """Delete a file.

        Args:
            path: Path to the file.
            missing_ok: If ``True``, silently succeed when the file does not exist.

        Raises:
            IsADirectoryError: If *path* is a directory.
            FileNotFoundError: If *path* does not exist and *missing_ok* is ``False``.
        """
        raise NotImplementedError

    @abstractmethod
    async def rmdir(self, path: PathLike) -> None:
        """Delete an empty directory.

        Missing paths raise ``FileNotFoundError``. ``NotADirectoryError`` is
        raised for files and ``OSError`` for non-empty directories.
        """
        raise NotImplementedError

    async def delete(self, path: PathLike) -> None:
        """Delete a file or an empty directory.

        Missing paths raise :class:`FileNotFoundError`. Non-empty directories
        raise ``OSError`` without removing the directory. Use :meth:`unlink`
        with ``missing_ok=True`` for idempotent file cleanup.
        """
        if await self.is_dir(path):
            await self.rmdir(path)
        else:
            await self.unlink(path)

    async def delete_many(self, *paths: PathLike) -> None:
        """Delete paths in order, skipping only paths that do not exist.

        The operation is fail-fast for every error other than
        :class:`FileNotFoundError`; paths before the failing path stay deleted.
        """
        for path in paths:
            try:
                await self.delete(path)
            except FileNotFoundError:
                continue

    @abstractmethod
    async def move(
        self,
        src: PathLike,
        dst: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        """Move a file using the explicit destination conflict policy.

        A missing source raises ``FileNotFoundError`` and a source or
        destination directory raises ``IsADirectoryError``. An existing
        destination file is replaced when ``overwrite=True`` and raises
        ``FileExistsError`` otherwise. Moving a file onto itself is a no-op
        only when ``overwrite=True``.
        """
        raise NotImplementedError

    @abstractmethod
    async def copy(
        self,
        src: PathLike,
        dst: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        """Copy a file using the explicit destination conflict policy.

        A missing source raises ``FileNotFoundError`` and a source or
        destination directory raises ``IsADirectoryError``. An existing
        destination file is replaced when ``overwrite=True`` and raises
        ``FileExistsError`` otherwise. Copying a file onto itself is a no-op
        only when ``overwrite=True``.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Directory
    # ------------------------------------------------------------------

    @abstractmethod
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create directory."""
        raise NotImplementedError

    @abstractmethod
    async def rmtree(self, path: PathLike) -> None:
        """Recursively remove a directory."""
        raise NotImplementedError

    @abstractmethod
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        """Recursively copy a directory tree.

        Args:
            src: Source directory path.
            dst: Destination directory path.
            overwrite: If ``True``, silently overwrite existing files at
                destination. If ``False``, raise FileExistsError when
                destination already exists.

        Raises:
            NotADirectoryError: If *src* is not a directory.
            FileExistsError: If *dst* exists and *overwrite* is ``False``.
        """
        raise NotImplementedError

    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        """Recursively move a directory tree.

        The default implementation copies the tree then removes the source.
        Backends may override with a more efficient implementation when
        available (e.g. native rename on the same filesystem).

        Args:
            src: Source directory path.
            dst: Destination directory path.
            overwrite: If ``True``, silently overwrite existing files at
                destination. If ``False``, raise FileExistsError.

        Raises:
            NotADirectoryError: If *src* is not a directory.
            FileExistsError: If *dst* exists and *overwrite* is ``False``.
        """
        await self.copytree(src, dst, overwrite=overwrite)
        await self.rmtree(src)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @abstractmethod
    async def exists(self, path: PathLike) -> bool:
        """Return whether a path exists."""
        raise NotImplementedError

    @abstractmethod
    async def is_file(self, path: PathLike) -> bool:
        """Return whether the path is a file."""
        raise NotImplementedError

    @abstractmethod
    async def is_dir(self, path: PathLike) -> bool:
        """Return whether the path is a directory."""
        raise NotImplementedError

    @abstractmethod
    async def stat(self, path: PathLike) -> FileInfo:
        """Return metadata."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    async def list_(self, path: PathLike) -> list[FileInfo]:
        """List directory."""
        return [item async for item in self.iterdir(path)]

    @abstractmethod
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        """Iterate directory entries."""
        raise NotImplementedError
        yield

    @abstractmethod
    async def walk(self, path: PathLike) -> AsyncGenerator[tuple[str, list[FileInfo], list[FileInfo]]]:
        """Recursively walk a directory tree."""
        raise NotImplementedError
        yield

    async def _is_dir_empty(self, path: PathLike) -> bool:
        """Check if a directory is empty."""
        async for _ in self.iterdir(path):
            return False
        return True
