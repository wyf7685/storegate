import contextlib
import errno
import functools
import hashlib
import json
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path, PurePath, PurePosixPath
from typing import Self, cast

import anyio
import anyio.lowlevel

from storegate.log import escape_tag
from storegate.utils import LoggerWrapper, logger_wrapper, open_file_rb, open_file_wb


class EntryKind(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


@dataclass(slots=True, frozen=True)
class FileInfo:
    path: str
    name: str
    kind: EntryKind
    size: int = 0
    modified: datetime | None = None
    created: datetime | None = None

    @property
    def is_file(self) -> bool:
        return self.kind is EntryKind.FILE

    @property
    def is_dir(self) -> bool:
        return self.kind is EntryKind.DIRECTORY

    @property
    def is_symlink(self) -> bool:
        return self.kind is EntryKind.SYMLINK


@dataclass(slots=True, frozen=True)
class WalkEntry:
    path: str
    entries: tuple[FileInfo, ...]


@dataclass(slots=True, frozen=True)
class StorageCapabilities:
    symlink_metadata: bool = False
    readlink: bool = False
    symlink_create: bool = False
    compare_exchange: bool = False


@dataclass(slots=True, frozen=True)
class VersionedBytes:
    data: bytes
    token: str


_UNSUPPORTED_ERRNO = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)


class UnsupportedOperationError(OSError):
    """An operation or storage entry is unsupported by the backend."""


_NO_CAPABILITIES = StorageCapabilities()


type BytesLike = bytes | bytearray | memoryview
type PathLike = str | PurePath

type LifecycleImplementation = Callable[["AbstractStorage"], Awaitable[None]]


def make_namespace_identity(kind: str, **fields: object) -> str:
    """Build a secret-free, stable namespace identity.

    Callers must whitelist only fields that identify the underlying data
    location. Credentials and runtime-only settings must not be included.
    """
    payload = json.dumps({"kind": kind, **fields}, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(payload.encode()).hexdigest()
    return f"{kind}:sha256:{digest}"


def validate_download_offset(offset: int) -> int:
    """Reject negative download offsets while accepting zero and positive values."""
    if offset < 0:
        raise ValueError("offset must be non-negative")
    return offset


_LIFECYCLE_WRAPPED_ATTRIBUTE = "__storegate_lifecycle_wrapped__"


class AbstractStorage(ABC):
    """Abstract storage interface with a concurrency-safe lifecycle.

    Public paths are logical storage paths. Symlink-aware backends reject a
    symlink in a caller-supplied intermediate path component. Follow operations
    may resolve a final symlink chain only while every resolved target remains
    inside the logical storage root. Metadata returned after following a link
    keeps the queried logical ``path`` and ``name``; only kind, size, and
    timestamps come from the resolved target.
    """

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
        return logger_wrapper(f"{self.__class__.__name__} <c><i>{escape_tag(self.display_id)}</></>")

    @property
    @abstractmethod
    def display_id(self) -> str:
        """Human-readable, secret-free identity for logs and error messages."""
        raise NotImplementedError

    @property
    @abstractmethod
    def namespace_identity(self) -> str:
        """Stable secret-free identity for persistent namespaces and locks."""
        raise NotImplementedError

    @property
    def capabilities(self) -> StorageCapabilities:
        """Return the immutable primitive capabilities implemented by this backend.

        Capabilities describe backend support, not whether current credentials,
        operating-system policy, or a remote ACL permits a particular request.
        """
        return _NO_CAPABILITIES

    @staticmethod
    def normalize_path(path: PathLike) -> PurePosixPath:
        """Normalize a caller-supplied logical path to an absolute POSIX path.

        Rejects NUL bytes and independent ``..`` segments before constructing
        the absolute path. ``.`` and repeated ``/`` are normalized; ordinary
        dots in filenames such as ``a..b`` or ``.hidden`` are preserved.
        """
        raw = PurePosixPath(path)
        raw_text = raw.as_posix()
        if "\x00" in raw_text:
            raise ValueError("path must not contain NUL")
        if ".." in raw.parts:
            raise ValueError("path must not contain '..' segments")
        return "/" / raw

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
        """Upload from a binary stream.

        A final symlink is rejected regardless of *overwrite*: upload never
        writes through to its target and never implicitly replaces the link.
        Replace a link only by explicitly unlinking it or by using
        :meth:`symlink` with ``overwrite=True``. Caller-supplied intermediate
        symlinks are rejected.
        """
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

        async with contextlib.aclosing(open_file_rb(Path(local_path))) as stream:
            await self.upload_stream(stream, remote_path, overwrite=overwrite)

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

        A final symlink is followed through its complete target chain only when
        the resolved target remains inside the storage root and is a regular
        file. A dangling target raises :class:`FileNotFoundError`, a cycle raises
        ``OSError(errno.ELOOP)``, and root escape raises
        ``PermissionError(errno.EACCES)``. Caller-supplied intermediate symlinks
        are rejected. Offset, stream lifetime, and cleanup follow the regular
        file contract.

        Args:
            remote_path: Path to the file.
            offset: Byte offset to start reading from. Default 0 (start of file).
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
        async with open_file_wb(Path(local_path)) as write:
            async for chunk in self.download_stream(remote_path):
                await write(chunk)

    # ------------------------------------------------------------------
    # Versioned compare-exchange
    # ------------------------------------------------------------------

    async def read_versioned(self, path: PathLike) -> VersionedBytes | None:
        """Read file bytes with an opaque version token.

        Backends without compare-exchange support raise
        :class:`UnsupportedOperationError`.
        """
        del path
        raise UnsupportedOperationError(
            _UNSUPPORTED_ERRNO,
            f"compare-exchange is not supported by {type(self).__name__}",
        )

    async def compare_exchange(
        self,
        path: PathLike,
        *,
        expected_token: str | None,
        data: BytesLike,
    ) -> VersionedBytes | None:
        """Atomically create or replace *path* when *expected_token* matches.

        ``expected_token=None`` means create-if-absent. A non-``None`` token
        replaces only when the current version matches exactly. Success returns
        the new value and token; conflict or absence returns ``None``.

        Backends without compare-exchange support raise
        :class:`UnsupportedOperationError`.
        """
        del path, expected_token, data
        raise UnsupportedOperationError(
            _UNSUPPORTED_ERRNO,
            f"compare-exchange is not supported by {type(self).__name__}",
        )

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        """Delete a regular file or symlink without following a symlink target.

        Args:
            path: Path to the lexical entry.
            missing_ok: If ``True``, silently succeed when the entry does not exist.

        Raises:
            IsADirectoryError: If *path* is a lexical directory.
            FileNotFoundError: If *path* does not exist and *missing_ok* is ``False``.
        """
        raise NotImplementedError

    @abstractmethod
    async def rmdir(self, path: PathLike) -> None:
        """Delete an empty lexical directory.

        Missing paths raise :class:`FileNotFoundError`. Files and symlinks,
        including symlinks to directories, raise :class:`NotADirectoryError`.
        Every raw child kind makes a directory non-empty, including symlinks and
        unsupported special entries.
        """
        raise NotImplementedError

    async def delete(self, path: PathLike) -> None:
        """Delete a file, symlink, or empty directory without following links.

        Dispatch is based on :meth:`lstat`, so a symlink to a directory is
        unlinked rather than passed to :meth:`rmdir`. Missing paths raise
        :class:`FileNotFoundError`; non-empty directories raise :class:`OSError`
        without removing the directory.
        """
        info = await self.lstat(path)
        if info.is_dir:
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
        """Move a regular file or symlink entry without following it.

        Native rename is preferred. Any fallback must preserve a symlink's raw
        target string. A missing source raises :class:`FileNotFoundError`, and a
        source or destination directory raises :class:`IsADirectoryError`.
        Existing file or symlink destinations are replaced when
        ``overwrite=True`` and raise :class:`FileExistsError` otherwise. Moving
        an entry onto itself is a no-op only when ``overwrite=True``.
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
        """Copy a regular file or preserve a symlink entry without following it.

        Symlink copy uses its raw target string and is independent of whether
        that target exists. A missing source raises :class:`FileNotFoundError`,
        and a source or destination directory raises :class:`IsADirectoryError`.
        Existing file or symlink destinations are replaced when
        ``overwrite=True`` and raise :class:`FileExistsError` otherwise. Copying
        an entry onto itself is a no-op only when ``overwrite=True``.
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
        """Recursively remove a lexical directory without following symlinks.

        A symlink root raises :class:`NotADirectoryError`. Symlinks inside the
        tree are unlinked as leaves and are never traversed. Implementations
        preflight a complete strict snapshot and raise
        :class:`UnsupportedOperationError` for unsupported special entries
        before making any change.
        """
        raise NotImplementedError

    @abstractmethod
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        """Recursively copy a lexical directory while preserving symlinks.

        A symlink root raises :class:`NotADirectoryError`. Directories recurse,
        regular files copy content, and symlinks copy their raw target as leaf
        entries, including dangling links. Unsupported special entries raise
        :class:`UnsupportedOperationError` during strict preflight before any
        change. Destination file and symlink conflicts follow *overwrite*;
        rollback must include created or replaced symlinks.

        Args:
            src: Source lexical directory path.
            dst: Destination directory path.
            overwrite: Replace destination file or symlink entries when true.
        """
        raise NotImplementedError

    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        """Recursively move a lexical directory while preserving symlinks.

        Native rename is preferred. The default merge path uses
        :meth:`copytree` preserve semantics and then removes the source tree;
        source symlinks are deleted as leaves and are never followed. A symlink
        root raises :class:`NotADirectoryError`, and destination conflicts follow
        *overwrite*.

        Args:
            src: Source lexical directory path.
            dst: Destination directory path.
            overwrite: Replace destination file or symlink entries when true.
        """
        await self.copytree(src, dst, overwrite=overwrite)
        await self.rmtree(src)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @abstractmethod
    async def exists(self, path: PathLike) -> bool:
        """Return whether the followed path exists.

        A final symlink is followed: a valid target returns true and a dangling
        target returns false. Root escape and other safety errors are propagated;
        lexical link existence is queried with :meth:`lstat` or
        :meth:`is_symlink`.
        """
        raise NotImplementedError

    @abstractmethod
    async def is_file(self, path: PathLike) -> bool:
        """Return whether the followed path resolves to a regular file.

        Symlinks to files return true, dangling links return false, and root
        escape or other safety errors are propagated.
        """
        raise NotImplementedError

    @abstractmethod
    async def is_dir(self, path: PathLike) -> bool:
        """Return whether the followed path resolves to a directory.

        Symlinks to directories return true, dangling links return false, and
        root escape or other safety errors are propagated.
        """
        raise NotImplementedError

    @abstractmethod
    async def stat(self, path: PathLike) -> FileInfo:
        """Return metadata, following a final symlink chain.

        Regular files and directories describe themselves. A final symlink is
        resolved completely while caller-supplied intermediate symlinks are
        rejected. Dangling targets raise :class:`FileNotFoundError`, cycles raise
        ``OSError(errno.ELOOP)``, and targets outside the logical root raise
        ``PermissionError(errno.EACCES)``. After a successful follow, ``path``
        and ``name`` retain the queried logical identity while kind, size, and
        timestamps describe the resolved target.
        """
        raise NotImplementedError

    async def lstat(self, path: PathLike) -> FileInfo:
        """Return lexical metadata without following the final symlink.

        Symlink-aware backends reject caller-supplied intermediate symlinks. A
        final symlink, including a dangling link, returns
        :attr:`EntryKind.SYMLINK`; its ``path`` and ``name`` describe the queried
        logical path and its remaining metadata describes the link itself.
        Unsupported special entries raise :class:`UnsupportedOperationError`.
        Backends without symlink metadata support reuse :meth:`stat`.
        """
        return await self.stat(path)

    async def is_symlink(self, path: PathLike) -> bool:
        """Return whether the lexical path is a symlink without following it."""
        try:
            return (await self.lstat(path)).is_symlink
        except FileNotFoundError:
            return False

    async def readlink(self, path: PathLike) -> str:
        """Return a symlink's raw target string without canonicalizing it.

        Implementations raise ``OSError(errno.EINVAL)`` for a non-link and
        :class:`UnsupportedOperationError` with the platform unsupported errno
        when the backend lacks this primitive.
        """
        del path
        raise UnsupportedOperationError(_UNSUPPORTED_ERRNO, "readlink is not supported")

    async def symlink(
        self,
        target: PathLike,
        link_path: PathLike,
        *,
        target_is_directory: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Create a symlink with the raw relative POSIX *target*.

        Absolute targets raise :class:`ValueError`. Relative targets are
        interpreted from the link parent and may contain ``..``; later follow
        operations still enforce root containment. Dangling targets are allowed.
        The link parent chain may not contain a symlink. With
        ``overwrite=False``, :meth:`lstat` detects any lexical entry at
        *link_path* and raises :class:`FileExistsError`; with ``overwrite=True``,
        only regular files and symlinks may be replaced, never directories.
        *target_is_directory* is a Windows hint for dangling directory links.
        Unsupported backends raise :class:`UnsupportedOperationError` with the
        platform unsupported errno. A failed non-idempotent create request must
        not be replayed after a connection failure.
        """
        del target, link_path, target_is_directory, overwrite
        raise UnsupportedOperationError(_UNSUPPORTED_ERRNO, "symlink is not supported")

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    async def list_(self, path: PathLike) -> list[FileInfo]:
        """List directory."""
        return [item async for item in self.iterdir(path)]

    @abstractmethod
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        """Iterate all supported direct lexical entries of a directory.

        Entries explicitly identify regular files, directories, and symlinks;
        listing a symlink does not read or follow its target. A symlink root
        raises :class:`NotADirectoryError`.
        """
        raise NotImplementedError
        yield

    @abstractmethod
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        """Recursively walk a lexical directory as structured snapshots.

        Each result contains the current absolute logical directory path and a
        tuple of all supported direct entries sorted by absolute logical path.
        Only :attr:`EntryKind.DIRECTORY` entries recurse; symlinks, including
        symlinks to directories, remain leaf entries. A symlink root raises
        :class:`NotADirectoryError`. Network backends materialize each required
        snapshot inside their lease and release it before yielding.
        """
        raise NotImplementedError
        yield

    async def _is_dir_empty(self, path: PathLike) -> bool:
        """Check if a directory is empty."""
        async for _ in self.iterdir(path):
            return False
        return True
