import asyncio
import contextlib
import errno
import stat as stat_module
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
import asyncssh

from storegate.log import escape_tag
from storegate.storage.abstract import (
    AbstractStorage,
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    StorageCapabilities,
    UnsupportedOperationError,
    WalkEntry,
    make_cache_identity,
)
from storegate.utils import coalesce_chunks

from .config import SFTPConfig
from .pool import SFTPChannelLease, SFTPChannelPool

_ROOT = PurePosixPath("/")
_MAX_SYMLINK_HOPS = 40
_UNSUPPORTED_ERRNO = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)
_CAPABILITIES = StorageCapabilities(symlink_metadata=True, readlink=True, symlink_create=True)
_CONNECTION_ERRORS = (
    asyncssh.ConnectionLost,
    asyncssh.DisconnectError,
    asyncssh.SFTPConnectionLost,
    asyncssh.SFTPNoConnection,
)


class _RawKind(Enum):
    FILE = auto()
    DIRECTORY = auto()
    SYMLINK = auto()
    SPECIAL = auto()
    UNKNOWN = auto()


@dataclass(slots=True, frozen=True)
class _RawEntry:
    logical: PurePosixPath
    attrs: asyncssh.SFTPAttrs
    kind: _RawKind


@dataclass(slots=True)
class _TreeJournal:
    created_dirs: set[PurePosixPath] = field(default_factory=set)
    created_entries: set[PurePosixPath] = field(default_factory=set)
    replaced_entries: dict[PurePosixPath, PurePosixPath] = field(default_factory=dict)
    temporary_entries: set[PurePosixPath] = field(default_factory=set)


def translate_sftp_error(exc: BaseException, message: str) -> BaseException:
    """Translate AsyncSSH errors without guessing generic SFTP v3 failures."""
    if isinstance(exc, asyncio.CancelledError):
        return exc
    if isinstance(exc, OSError):
        return exc
    if isinstance(exc, (asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath)):
        return FileNotFoundError(f"{message}: {exc}")
    if isinstance(exc, (asyncssh.PermissionDenied, asyncssh.SFTPPermissionDenied)):
        return PermissionError(f"{message}: {exc}")
    if isinstance(exc, asyncssh.SFTPFileAlreadyExists):
        return FileExistsError(f"{message}: {exc}")
    if isinstance(exc, asyncssh.SFTPFileIsADirectory):
        return IsADirectoryError(f"{message}: {exc}")
    if isinstance(exc, asyncssh.SFTPNotADirectory):
        return NotADirectoryError(f"{message}: {exc}")
    if isinstance(exc, asyncssh.SFTPDirNotEmpty):
        return OSError(f"{message}: directory not empty")
    if isinstance(exc, asyncssh.SFTPLinkLoop):
        return OSError(errno.ELOOP, f"{message}: {exc}")
    if isinstance(exc, asyncssh.SFTPOpUnsupported):
        return UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"{message}: {exc}")
    if isinstance(exc, asyncssh.HostKeyNotVerifiable):
        return OSError(f"{message}: SSH host key verification failed")
    if isinstance(exc, (asyncssh.Error, asyncssh.SFTPError)):
        return OSError(f"{message}: {exc}")
    return exc


@final
class SFTPStorage(AbstractStorage):
    """SFTP client backend with bounded channel reuse and symlink-safe paths."""

    def __init__(self, config: str | Path | SFTPConfig) -> None:
        super().__init__()
        self._config = config if isinstance(config, SFTPConfig) else SFTPConfig.from_file(config)
        self._configured_root = PurePosixPath(self._config.root_prefix)
        self._canonical_root = self._configured_root
        self._validated_generation = 0
        self._pool = self._new_pool()

    def _new_pool(self) -> SFTPChannelPool:
        return SFTPChannelPool(
            max_channels=self._config.max_channels,
            close_timeout=self._config.close_timeout,
            transport_factory=self._open_transport,
            channel_factory=self._open_channel,
            transport_closer=self._close_transport,
            channel_closer=self._close_channel,
            logger=self.log,
        )

    @property
    @override
    def id(self) -> str:
        config = self._config
        return f"sftp:{config.username}@{config.host}:{config.port}{config.root_prefix}"

    @property
    @override
    def cache_identity(self) -> str:
        config = self._config
        return make_cache_identity(
            "sftp",
            host=config.host,
            port=config.port,
            root_prefix=self._configured_root.as_posix(),
            username=config.username,
        )

    @property
    @override
    def capabilities(self) -> StorageCapabilities:
        return _CAPABILITIES

    def _logical_path(self, path: PathLike) -> PurePosixPath:
        raw = PurePosixPath(path)
        if "\x00" in raw.as_posix():
            raise ValueError("SFTP path must not contain NUL")
        if ".." in raw.parts:
            raise ValueError("SFTP path must not contain '..' segments")
        return self.normalize_path(raw)

    def _remote_path(self, path: PathLike) -> str:
        logical = self._logical_path(path)
        relative = logical.relative_to(_ROOT)
        if relative == PurePosixPath("."):
            return self._canonical_root.as_posix()
        return (self._canonical_root / relative).as_posix()

    async def _io[T](self, awaitable: Awaitable[T]) -> T:
        with anyio.fail_after(self._config.io_timeout):
            return await awaitable

    async def _open_transport(self) -> asyncssh.SSHClientConnection:
        config = self._config
        kwargs: dict[str, object] = {
            "agent_path": None,
            "client_keys": list(config.client_keys),
            "config": None,
            "connect_timeout": config.connect_timeout,
            "keepalive_count_max": config.keepalive_count_max,
            "keepalive_interval": config.keepalive_interval,
            "login_timeout": config.login_timeout,
            "username": config.username,
        }
        if config.password is not None:
            kwargs["password"] = config.password.get_secret_value()
        if config.passphrase is not None:
            kwargs["passphrase"] = config.passphrase.get_secret_value()
        if config.disable_host_key_check:
            kwargs["known_hosts"] = None
        elif config.known_hosts is not None:
            kwargs["known_hosts"] = str(config.known_hosts)
        return await asyncssh.connect(config.host, config.port, **kwargs)

    async def _open_channel(
        self,
        connection: asyncssh.SSHClientConnection,
        generation: int,
    ) -> asyncssh.SFTPClient:
        client = await self._io(
            connection.start_sftp_client(path_encoding="utf-8", path_errors="strict", sftp_version=3)
        )
        try:
            if generation != self._validated_generation:
                configured_attrs = await self._io(client.lstat(self._configured_root.as_posix()))
                configured_kind = self._attrs_kind(configured_attrs)
                if configured_kind is _RawKind.SYMLINK:
                    raise OSError(f"SFTP root_prefix must not be a symlink: {self._configured_root.as_posix()}")
                if configured_kind is not _RawKind.DIRECTORY:
                    raise NotADirectoryError(f"SFTP root_prefix is not a directory: {self._configured_root.as_posix()}")
                resolved = await self._io(client.realpath(self._configured_root.as_posix()))
                if not isinstance(resolved, str):
                    raise OSError("SFTP server returned an invalid canonical root")
                canonical = PurePosixPath(resolved)
                if not canonical.is_absolute():
                    raise OSError("SFTP server returned a relative canonical root")
                canonical_attrs = await self._io(client.stat(canonical.as_posix()))
                if self._attrs_kind(canonical_attrs) is not _RawKind.DIRECTORY:
                    raise NotADirectoryError(f"SFTP canonical root is not a directory: {canonical.as_posix()}")
                self._canonical_root = canonical
                self._validated_generation = generation
        except BaseException:
            await self._close_channel_safely(client)
            raise
        return client

    @staticmethod
    async def _close_transport(connection: asyncssh.SSHClientConnection) -> None:
        connection.close()
        await connection.wait_closed()

    @staticmethod
    async def _close_channel(client: asyncssh.SFTPClient) -> None:
        client.exit()
        await client.wait_closed()

    async def _close_channel_safely(self, client: asyncssh.SFTPClient) -> None:
        with anyio.CancelScope(shield=True):
            with contextlib.suppress(BaseException):
                await self._close_channel(client)

    @staticmethod
    def _connection_error(exc: BaseException) -> bool:
        return isinstance(exc, _CONNECTION_ERRORS)

    @asynccontextmanager
    async def _client_lease(self, message: str) -> AsyncIterator[SFTPChannelLease]:
        try:
            async with self._pool.acquire() as lease:
                try:
                    yield lease
                except BaseException as exc:
                    if self._connection_error(exc):
                        lease.invalidate_transport()
                    elif isinstance(
                        exc,
                        (asyncssh.SFTPBadMessage, TimeoutError, anyio.get_cancelled_exc_class()),
                    ):
                        lease.invalidate()
                    translated = translate_sftp_error(exc, message)
                    if translated is exc:
                        raise
                    raise translated from exc
        except BaseException as exc:
            translated = translate_sftp_error(exc, message)
            if translated is exc:
                raise
            raise translated from exc

    @override
    async def connect(self) -> None:
        if self._pool.is_closed:
            self._validated_generation = 0
            self._canonical_root = self._configured_root
            self._pool = self._new_pool()
        try:
            await self._pool.start()
        except BaseException as exc:
            translated = translate_sftp_error(exc, "Failed to connect to SFTP server")
            if translated is exc:
                raise
            raise translated from exc
        self.log.info(f"Connected to <c>{escape_tag(self._config.host)}</c>:<c>{self._config.port}</c>")

    @override
    async def close(self) -> None:
        await self._pool.close()
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        if not self._pool.is_open:
            return False
        try:
            async with self._client_lease("Failed to ping SFTP server") as lease:
                attrs = await self._io(lease.client.stat(self._canonical_root.as_posix()))
                return self._attrs_kind(attrs) is _RawKind.DIRECTORY
        except BaseException:
            return False

    @staticmethod
    def _attrs_kind(attrs: asyncssh.SFTPAttrs) -> _RawKind:
        if attrs.type == asyncssh.FILEXFER_TYPE_REGULAR:
            return _RawKind.FILE
        if attrs.type == asyncssh.FILEXFER_TYPE_DIRECTORY:
            return _RawKind.DIRECTORY
        if attrs.type == asyncssh.FILEXFER_TYPE_SYMLINK:
            return _RawKind.SYMLINK
        if attrs.type == asyncssh.FILEXFER_TYPE_SPECIAL:
            return _RawKind.SPECIAL
        permissions = attrs.permissions
        if permissions is not None:
            if stat_module.S_ISREG(permissions):
                return _RawKind.FILE
            if stat_module.S_ISDIR(permissions):
                return _RawKind.DIRECTORY
            if stat_module.S_ISLNK(permissions):
                return _RawKind.SYMLINK
            if any(
                predicate(permissions)
                for predicate in (
                    stat_module.S_ISFIFO,
                    stat_module.S_ISSOCK,
                    stat_module.S_ISCHR,
                    stat_module.S_ISBLK,
                )
            ):
                return _RawKind.SPECIAL
        return _RawKind.UNKNOWN

    @staticmethod
    def _entry_kind(kind: _RawKind, path: PurePosixPath) -> EntryKind:
        if kind is _RawKind.FILE:
            return EntryKind.FILE
        if kind is _RawKind.DIRECTORY:
            return EntryKind.DIRECTORY
        if kind is _RawKind.SYMLINK:
            return EntryKind.SYMLINK
        raise UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"Unsupported SFTP entry type: {path.as_posix()}")

    @staticmethod
    def _timestamp(seconds: int | None, nanoseconds: int | None) -> datetime | None:
        if seconds is None:
            return None
        result = datetime.fromtimestamp(seconds, UTC)
        if nanoseconds:
            result += timedelta(microseconds=nanoseconds // 1000)
        return result

    def _file_info_from_attrs(
        self,
        logical: PurePosixPath,
        attrs: asyncssh.SFTPAttrs,
        kind: _RawKind | None = None,
    ) -> FileInfo:
        entry_kind = self._entry_kind(self._attrs_kind(attrs) if kind is None else kind, logical)
        return FileInfo(
            path=logical.as_posix(),
            name="" if logical == _ROOT else logical.name,
            kind=entry_kind,
            size=0 if entry_kind is EntryKind.DIRECTORY else max(attrs.size or 0, 0),
            modified=self._timestamp(attrs.mtime, attrs.mtime_ns),
            created=self._timestamp(attrs.crtime, attrs.crtime_ns),
        )

    async def _lstat_components(
        self,
        client: asyncssh.SFTPClient,
        logical: PurePosixPath,
        *,
        allow_missing_final: bool = False,
    ) -> asyncssh.SFTPAttrs | None:
        if logical == _ROOT:
            return await self._io(client.lstat(self._canonical_root.as_posix()))
        current = _ROOT
        parts = logical.parts[1:]
        for index, part in enumerate(parts):
            current /= part
            final = index == len(parts) - 1
            try:
                attrs = await self._io(client.lstat(self._remote_path(current)))
            except asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath:
                if allow_missing_final:
                    return None
                raise
            kind = self._attrs_kind(attrs)
            if kind in {_RawKind.SPECIAL, _RawKind.UNKNOWN}:
                self._entry_kind(kind, current)
            if not final:
                if kind is _RawKind.SYMLINK:
                    raise PermissionError(errno.EACCES, f"Intermediate symlink is not allowed: {current.as_posix()}")
                if kind is not _RawKind.DIRECTORY:
                    raise NotADirectoryError(f"Not a directory: {current.as_posix()}")
        return attrs

    async def _lstat_info(self, client: asyncssh.SFTPClient, path: PathLike) -> FileInfo:
        logical = self._logical_path(path)
        attrs = await self._lstat_components(client, logical)
        assert attrs is not None
        return self._file_info_from_attrs(logical, attrs)

    def _require_directory_entry(self, info: FileInfo, path: PurePosixPath) -> None:
        if info.is_symlink:
            raise PermissionError(errno.EACCES, f"Intermediate symlink is not allowed: {path.as_posix()}")
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {path.as_posix()}")

    async def _lstat_or_none(self, client: asyncssh.SFTPClient, logical: PurePosixPath) -> FileInfo | None:
        try:
            return await self._lstat_info(client, logical)
        except asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath, FileNotFoundError:
            return None

    @staticmethod
    def _normalize_remote(path: PurePosixPath) -> PurePosixPath:
        if not path.is_absolute():
            raise OSError(f"SFTP target path is not absolute: {path.as_posix()}")
        parts: list[str] = []
        for part in path.parts[1:]:
            if part in {"", "."}:
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        return PurePosixPath("/", *parts)

    def _require_remote_containment(self, path: PurePosixPath) -> PurePosixPath:
        normalized = self._normalize_remote(path)
        try:
            normalized.relative_to(self._canonical_root)
        except ValueError:
            message = f"Resolved symlink target is outside root: {normalized.as_posix()}"
            raise PermissionError(errno.EACCES, message) from None
        return normalized

    async def _readlink_remote(self, client: asyncssh.SFTPClient, remote: PurePosixPath) -> str:
        target = await self._io(client.readlink(remote.as_posix()))
        if not isinstance(target, str) or "\x00" in target:
            raise OSError(errno.EINVAL, f"SFTP server returned an invalid symlink target for {remote.as_posix()}")
        return target

    def _link_target_path(self, link_remote: PurePosixPath, raw_target: str) -> PurePosixPath:
        target = PurePosixPath(raw_target)
        candidate = target if target.is_absolute() else link_remote.parent / target
        return self._require_remote_containment(candidate)

    async def _resolve_remote_target(
        self,
        client: asyncssh.SFTPClient,
        initial: PurePosixPath,
    ) -> tuple[PurePosixPath, asyncssh.SFTPAttrs]:
        candidate = self._require_remote_containment(initial)
        hops = 0
        while True:
            relative = candidate.relative_to(self._canonical_root)
            current = self._canonical_root
            pending = [] if relative == PurePosixPath(".") else list(relative.parts)
            attrs = await self._io(client.lstat(current.as_posix()))
            if not pending:
                kind = self._attrs_kind(attrs)
                self._entry_kind(kind, _ROOT)
                return current, attrs
            index = 0
            while index < len(pending):
                current /= pending[index]
                attrs = await self._io(client.lstat(current.as_posix()))
                kind = self._attrs_kind(attrs)
                if kind in {_RawKind.SPECIAL, _RawKind.UNKNOWN}:
                    self._entry_kind(kind, current)
                if kind is _RawKind.SYMLINK:
                    hops += 1
                    if hops > _MAX_SYMLINK_HOPS:
                        raise OSError(errno.ELOOP, f"Too many symbolic link levels: {current.as_posix()}")
                    raw_target = await self._readlink_remote(client, current)
                    target = PurePosixPath(raw_target)
                    base = target if target.is_absolute() else current.parent / target
                    candidate = self._require_remote_containment(PurePosixPath(base, *pending[index + 1 :]))
                    break
                if index < len(pending) - 1 and kind is not _RawKind.DIRECTORY:
                    raise NotADirectoryError(f"Not a directory: {current.as_posix()}")
                index += 1
            else:
                return current, attrs

    async def _followed_attrs(
        self,
        client: asyncssh.SFTPClient,
        logical: PurePosixPath,
    ) -> tuple[PurePosixPath, asyncssh.SFTPAttrs]:
        attrs = await self._lstat_components(client, logical)
        assert attrs is not None
        kind = self._attrs_kind(attrs)
        if kind is not _RawKind.SYMLINK:
            self._entry_kind(kind, logical)
            return PurePosixPath(self._remote_path(logical)), attrs
        remote = PurePosixPath(self._remote_path(logical))
        raw_target = await self._readlink_remote(client, remote)
        return await self._resolve_remote_target(client, self._link_target_path(remote, raw_target))

    async def _stat_info(self, client: asyncssh.SFTPClient, path: PathLike) -> FileInfo:
        logical = self._logical_path(path)
        _, attrs = await self._followed_attrs(client, logical)
        return self._file_info_from_attrs(logical, attrs)

    async def _stat_or_none(self, client: asyncssh.SFTPClient, logical: PurePosixPath) -> FileInfo | None:
        try:
            return await self._stat_info(client, logical)
        except asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath, FileNotFoundError:
            return None

    @override
    async def lstat(self, path: PathLike) -> FileInfo:
        async with self._client_lease(f"Failed to lstat {path}") as lease:
            return await self._lstat_info(lease.client, path)

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        async with self._client_lease(f"Failed to stat {path}") as lease:
            return await self._stat_info(lease.client, path)

    @override
    async def exists(self, path: PathLike) -> bool:
        async with self._client_lease(f"Failed to check existence of {path}") as lease:
            return await self._stat_or_none(lease.client, self._logical_path(path)) is not None

    @override
    async def is_file(self, path: PathLike) -> bool:
        async with self._client_lease(f"Failed to check whether {path} is a file") as lease:
            info = await self._stat_or_none(lease.client, self._logical_path(path))
            return info is not None and info.is_file

    @override
    async def is_dir(self, path: PathLike) -> bool:
        async with self._client_lease(f"Failed to check whether {path} is a directory") as lease:
            info = await self._stat_or_none(lease.client, self._logical_path(path))
            return info is not None and info.is_dir

    @override
    async def readlink(self, path: PathLike) -> str:
        logical = self._logical_path(path)
        async with self._client_lease(f"Failed to read symbolic link {path}") as lease:
            attrs = await self._lstat_components(lease.client, logical)
            assert attrs is not None
            if self._attrs_kind(attrs) is not _RawKind.SYMLINK:
                raise OSError(errno.EINVAL, f"Not a symbolic link: {logical.as_posix()}")
            return await self._readlink_remote(lease.client, PurePosixPath(self._remote_path(logical)))

    @override
    async def symlink(
        self,
        target: PathLike,
        link_path: PathLike,
        *,
        target_is_directory: bool = False,
        overwrite: bool = False,
    ) -> None:
        del target_is_directory
        raw_target = PurePosixPath(target).as_posix()
        if "\x00" in raw_target:
            raise ValueError("SFTP symlink target must not contain NUL")
        if PurePosixPath(raw_target).is_absolute():
            raise ValueError("SFTP symlink target must be relative")
        link = self._logical_path(link_path)
        if link == _ROOT:
            raise FileExistsError("Cannot replace the storage root with a symlink")
        self.log.info(f"SymLink: <y>{escape_tag(link.as_posix())}</y> → <y>{escape_tag(raw_target)}</y>")
        async with self._client_lease(f"Failed to create symbolic link {link_path}") as lease:
            client = lease.client
            parent = await self._lstat_info(client, link.parent)
            self._require_directory_entry(parent, link.parent)
            existing = await self._lstat_or_none(client, link)
            if existing is None:
                await self._io(client.symlink(raw_target, self._remote_path(link)))
                return
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {link.as_posix()}")
            if existing.is_dir:
                raise IsADirectoryError(f"Destination is a directory: {link.as_posix()}")
            backup = self._temporary_path(link, "symlink-backup")
            await self._io(client.rename(self._remote_path(link), self._remote_path(backup)))
            try:
                await self._io(client.symlink(raw_target, self._remote_path(link)))
            except BaseException as primary:
                rollback_errors: list[BaseException] = []
                with anyio.CancelScope(shield=True):
                    try:
                        await self._remove_if_exists(client, link)
                    except BaseException as exc:
                        rollback_errors.append(exc)
                    try:
                        await self._io(client.rename(self._remote_path(backup), self._remote_path(link)))
                    except BaseException as exc:
                        rollback_errors.append(exc)
                if rollback_errors:
                    raise BaseExceptionGroup(
                        "SFTP symlink replacement and rollback failed",
                        [primary, *rollback_errors],
                    ) from None
                raise
            await self._remove_if_exists(client, backup)

    @staticmethod
    def _validate_filename(filename: object) -> str:
        if not isinstance(filename, str):
            raise OSError("SFTP server returned a non-text filename")
        if filename in {"", ".", ".."} or "\x00" in filename or "/" in filename or "\\" in filename:
            raise OSError(f"SFTP server returned an invalid filename: {filename!r}")
        return filename

    async def _scan_raw(
        self,
        client: asyncssh.SFTPClient,
        logical: PurePosixPath,
        *,
        strict: bool,
    ) -> list[_RawEntry]:
        info = await self._lstat_info(client, logical)
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
        entries: list[_RawEntry] = []
        iterator = client.scandir(self._remote_path(logical)).__aiter__()
        while True:
            try:
                entry = await self._io(anext(iterator))
            except StopAsyncIteration:
                break
            if entry.filename in {".", ".."}:
                continue
            filename = self._validate_filename(entry.filename)
            child = logical / filename
            attrs = entry.attrs
            kind = self._attrs_kind(attrs)
            if kind is _RawKind.UNKNOWN:
                attrs = await self._io(client.lstat(self._remote_path(child)))
                kind = self._attrs_kind(attrs)
            if kind in {_RawKind.SPECIAL, _RawKind.UNKNOWN}:
                if strict:
                    self._entry_kind(kind, child)
                self.log.debug(f"Skipping unsupported SFTP entry <y>{escape_tag(child.as_posix())}</y>")
                continue
            entries.append(_RawEntry(child, attrs, kind))
        entries.sort(key=lambda item: item.logical.as_posix())
        return entries

    async def _directory_has_raw_child(self, client: asyncssh.SFTPClient, logical: PurePosixPath) -> bool:
        info = await self._lstat_info(client, logical)
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
        iterator = client.scandir(self._remote_path(logical)).__aiter__()
        while True:
            try:
                entry = await self._io(anext(iterator))
            except StopAsyncIteration:
                return False
            if entry.filename in {".", ".."}:
                continue
            self._validate_filename(entry.filename)
            return True

    async def _discovery_scan(self, client: asyncssh.SFTPClient, logical: PurePosixPath) -> list[FileInfo]:
        return [
            self._file_info_from_attrs(entry.logical, entry.attrs, entry.kind)
            for entry in await self._scan_raw(client, logical, strict=False)
        ]

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        logical = self._logical_path(path)
        async with self._client_lease(f"Failed to iterate directory {path}") as lease:
            entries = await self._discovery_scan(lease.client, logical)
        for entry in entries:
            yield entry

    async def _walk_snapshot(self, client: asyncssh.SFTPClient, path: PathLike) -> list[WalkEntry]:
        root = self._logical_path(path)
        root_info = await self._lstat_info(client, root)
        if not root_info.is_dir:
            raise NotADirectoryError(f"Not a directory: {root.as_posix()}")
        pending = [root]
        snapshot: list[WalkEntry] = []
        while pending:
            current = pending.pop()
            entries = await self._discovery_scan(client, current)
            snapshot.append(WalkEntry(current.as_posix(), tuple(entries)))
            directories = [entry for entry in entries if entry.kind is EntryKind.DIRECTORY]
            pending.extend(PurePosixPath(entry.path) for entry in reversed(directories))
        return snapshot

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        async with self._client_lease(f"Failed to walk directory {path}") as lease:
            snapshot = await self._walk_snapshot(lease.client, path)
        for entry in snapshot:
            yield entry

    async def _mkdir(
        self,
        client: asyncssh.SFTPClient,
        path: PathLike,
        *,
        parents: bool,
        exist_ok: bool,
    ) -> set[PurePosixPath]:
        logical = self._logical_path(path)
        if logical == _ROOT:
            if exist_ok:
                return set()
            raise FileExistsError("Root directory already exists")

        existing = await self._lstat_or_none(client, logical)
        if existing is not None:
            if not existing.is_dir or not exist_ok:
                raise FileExistsError(f"Path already exists: {logical.as_posix()}")
            return set()

        created: set[PurePosixPath] = set()
        if not parents:
            parent = await self._lstat_or_none(client, logical.parent)
            if parent is None:
                raise FileNotFoundError(f"Parent directory does not exist: {logical.parent.as_posix()}")
            self._require_directory_entry(parent, logical.parent)
            await self._io(client.mkdir(self._remote_path(logical)))
            created.add(logical)
            return created

        current = _ROOT
        for part in logical.parts[1:]:
            current /= part
            info = await self._lstat_or_none(client, current)
            if info is None:
                await self._io(client.mkdir(self._remote_path(current)))
                created.add(current)
            elif info.is_symlink:
                raise PermissionError(errno.EACCES, f"Intermediate symlink is not allowed: {current.as_posix()}")
            elif not info.is_dir:
                raise FileExistsError(f"Path component is not a directory: {current.as_posix()}")
        return created

    @override
    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        logical = self._logical_path(path)
        self.log.info(f"MkDir: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease(f"Failed to create directory {path}") as lease:
            await self._mkdir(lease.client, path, parents=parents, exist_ok=exist_ok)

    @staticmethod
    def _temporary_path(target: PurePosixPath, kind: str) -> PurePosixPath:
        return target.with_name(f".{target.name}.storegate-{kind}-{uuid.uuid4().hex}")

    async def _remove_if_exists(self, client: asyncssh.SFTPClient, path: PurePosixPath) -> None:
        info = await self._lstat_or_none(client, path)
        if info is None:
            return
        if info.is_dir:
            await self._io(client.rmdir(self._remote_path(path)))
        else:
            await self._io(client.remove(self._remote_path(path)))

    async def _commit_staged_entry(
        self,
        client: asyncssh.SFTPClient,
        temporary: PurePosixPath,
        target: PurePosixPath,
        *,
        target_exists: bool,
    ) -> None:
        if not target_exists:
            await self._io(client.rename(self._remote_path(temporary), self._remote_path(target)))
            return
        try:
            await self._io(client.posix_rename(self._remote_path(temporary), self._remote_path(target)))
        except asyncssh.SFTPOpUnsupported:
            pass
        else:
            return

        backup = self._temporary_path(target, "backup")
        await self._io(client.rename(self._remote_path(target), self._remote_path(backup)))
        try:
            await self._io(client.rename(self._remote_path(temporary), self._remote_path(target)))
        except BaseException as primary:
            rollback_errors: list[BaseException] = []
            with anyio.CancelScope(shield=True):
                try:
                    await self._remove_if_exists(client, target)
                except BaseException as exc:
                    rollback_errors.append(exc)
                try:
                    await self._io(client.rename(self._remote_path(backup), self._remote_path(target)))
                except BaseException as exc:
                    rollback_errors.append(exc)
            if rollback_errors:
                raise BaseExceptionGroup(
                    "SFTP staged commit and rollback failed",
                    [primary, *rollback_errors],
                ) from None
            raise
        await self._remove_if_exists(client, backup)

    async def _cleanup_temporary(self, client: asyncssh.SFTPClient, temporary: PurePosixPath) -> None:
        with anyio.move_on_after(self._config.close_timeout, shield=True):
            with contextlib.suppress(BaseException):
                await self._remove_if_exists(client, temporary)

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        target = self._logical_path(remote_path)
        if target == _ROOT:
            raise IsADirectoryError("Cannot upload to root directory")
        async with self._client_lease(f"Failed to upload stream to {remote_path}") as lease:
            client = lease.client
            attrs = await self._lstat_components(client, target, allow_missing_final=True)
            target_exists = attrs is not None
            if attrs is not None:
                kind = self._attrs_kind(attrs)
                if kind is _RawKind.DIRECTORY:
                    raise IsADirectoryError(f"Destination is a directory: {target.as_posix()}")
                if kind is _RawKind.SYMLINK:
                    raise OSError(errno.EACCES, f"Upload destination is a symbolic link: {target.as_posix()}")
                if kind is not _RawKind.FILE:
                    self._entry_kind(kind, target)
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {target.as_posix()}")
            self.log.info(f"Upload: <y>{escape_tag(target.as_posix())}</y>")
            await self._mkdir(client, target.parent, parents=True, exist_ok=True)
            temporary = self._temporary_path(target, "upload")
            handle: asyncssh.SFTPClientFile[bytes] | None = None
            try:
                handle = await self._io(client.open(self._remote_path(temporary), "wb", encoding=None))
                async for chunk in coalesce_chunks(stream, self._config.chunk_size):
                    await self._io(handle.write(chunk))
                await self._io(handle.close())
                handle = None
                await self._commit_staged_entry(client, temporary, target, target_exists=target_exists)
            except BaseException:
                if handle is not None:
                    with anyio.CancelScope(shield=True):
                        with contextlib.suppress(BaseException):
                            await self._io(handle.close())
                await self._cleanup_temporary(client, temporary)
                raise

    @override
    async def download_stream(self, remote_path: PathLike, *, offset: int = 0) -> AsyncGenerator[bytes]:
        if offset < 0:
            raise ValueError("offset must not be negative")
        logical = self._logical_path(remote_path)
        async with self._client_lease(f"Failed to download {remote_path}") as lease:
            client = lease.client
            resolved, attrs = await self._followed_attrs(client, logical)
            info = self._file_info_from_attrs(logical, attrs)
            if not info.is_file:
                raise IsADirectoryError(f"Path is not a regular file: {logical.as_posix()}")
            if offset >= info.size:
                return
            self.log.debug(
                f"Download: <y>{escape_tag(logical.as_posix())}</y> (<g>{info.size}</g> bytes, offset=<g>{offset}</g>)"
            )
            handle: asyncssh.SFTPClientFile[bytes] | None = None
            try:
                handle = await self._io(client.open(resolved.as_posix(), "rb", encoding=None))
                position = offset
                while True:
                    data = await self._io(handle.read(self._config.chunk_size, offset=position))
                    if not data:
                        break
                    position += len(data)
                    yield data
            finally:
                if handle is not None:
                    with anyio.CancelScope(shield=True):
                        try:
                            await self._io(handle.close())
                        except BaseException:
                            lease.invalidate()

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        logical = self._logical_path(path)
        if logical == _ROOT:
            raise IsADirectoryError("Cannot unlink root directory")
        async with self._client_lease(f"Failed to unlink {path}") as lease:
            info = await self._lstat_or_none(lease.client, logical)
            if info is None:
                if missing_ok:
                    return
                raise FileNotFoundError(f"Path does not exist: {logical.as_posix()}")
            if info.is_dir:
                raise IsADirectoryError(f"Path is a directory: {logical.as_posix()}")
            self.log.info(f"Delete: <y>{escape_tag(logical.as_posix())}</y>")
            await self._io(lease.client.remove(self._remote_path(logical)))

    @override
    async def rmdir(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == _ROOT:
            raise OSError("Cannot remove root directory")
        self.log.info(f"Delete dir: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease(f"Failed to remove directory {path}") as lease:
            info = await self._lstat_or_none(lease.client, logical)
            if info is None:
                raise FileNotFoundError(f"Path does not exist: {logical.as_posix()}")
            if not info.is_dir:
                raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
            if await self._directory_has_raw_child(lease.client, logical):
                raise OSError(f"Directory not empty: {logical.as_posix()}")
            await self._io(lease.client.rmdir(self._remote_path(logical)))

    async def _strict_snapshot(
        self,
        client: asyncssh.SFTPClient,
        path: PathLike,
    ) -> list[tuple[PurePosixPath, list[_RawEntry]]]:
        root = self._logical_path(path)
        root_info = await self._lstat_info(client, root)
        if not root_info.is_dir:
            raise NotADirectoryError(f"Not a directory: {root.as_posix()}")
        pending = [root]
        snapshot: list[tuple[PurePosixPath, list[_RawEntry]]] = []
        while pending:
            current = pending.pop()
            entries = await self._scan_raw(client, current, strict=True)
            snapshot.append((current, entries))
            directories = [entry for entry in entries if entry.kind is _RawKind.DIRECTORY]
            pending.extend(entry.logical for entry in reversed(directories))
        return snapshot

    async def _rmtree(self, client: asyncssh.SFTPClient, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == _ROOT:
            raise OSError("Cannot remove root directory")
        snapshot = await self._strict_snapshot(client, logical)
        for _, entries in snapshot:
            for entry in entries:
                if entry.kind in {_RawKind.FILE, _RawKind.SYMLINK}:
                    await self._io(client.remove(self._remote_path(entry.logical)))
        for current, _ in reversed(snapshot):
            await self._io(client.rmdir(self._remote_path(current)))

    @override
    async def rmtree(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        self.log.info(f"RmTree: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease(f"Failed to remove directory tree {path}") as lease:
            await self._rmtree(lease.client, path)

    async def _copy_contents(
        self,
        client: asyncssh.SFTPClient,
        source: PurePosixPath,
        destination: PurePosixPath,
    ) -> None:
        source_handle: asyncssh.SFTPClientFile[bytes] | None = None
        destination_handle: asyncssh.SFTPClientFile[bytes] | None = None
        try:
            source_handle = await self._io(client.open(self._remote_path(source), "rb", encoding=None))
            destination_handle = await self._io(client.open(self._remote_path(destination), "wb", encoding=None))
            position = 0
            while True:
                data = await self._io(source_handle.read(self._config.chunk_size, offset=position))
                if not data:
                    break
                position += len(data)
                await self._io(destination_handle.write(data))
            await self._io(destination_handle.close())
            destination_handle = None
        finally:
            with anyio.CancelScope(shield=True):
                if destination_handle is not None:
                    with contextlib.suppress(BaseException):
                        await self._io(destination_handle.close())
                if source_handle is not None:
                    with contextlib.suppress(BaseException):
                        await self._io(source_handle.close())

    async def _copy_to_temporary(
        self,
        client: asyncssh.SFTPClient,
        source: PurePosixPath,
        source_kind: EntryKind,
        temporary: PurePosixPath,
    ) -> None:
        if source_kind is EntryKind.FILE:
            await self._copy_contents(client, source, temporary)
            return
        if source_kind is EntryKind.SYMLINK:
            raw_target = await self._readlink_remote(client, PurePosixPath(self._remote_path(source)))
            await self._io(client.symlink(raw_target, self._remote_path(temporary)))
            return
        raise IsADirectoryError(f"Source is a directory: {source.as_posix()}")

    @override
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"Copy: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        if source == _ROOT:
            raise IsADirectoryError("Cannot copy root directory as a file")
        async with self._client_lease(f"Failed to copy {src} to {dst}") as lease:
            client = lease.client
            source_info = await self._lstat_or_none(client, source)
            if source_info is None:
                raise FileNotFoundError(f"Source does not exist: {source.as_posix()}")
            if source_info.is_dir:
                raise IsADirectoryError(f"Source is a directory: {source.as_posix()}")
            if source == destination:
                if overwrite:
                    return
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            destination_info = await self._lstat_or_none(client, destination)
            if destination_info is not None:
                if destination_info.is_dir:
                    raise IsADirectoryError(f"Destination is a directory: {destination.as_posix()}")
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            await self._mkdir(client, destination.parent, parents=True, exist_ok=True)
            temporary = self._temporary_path(destination, "copy")
            try:
                await self._copy_to_temporary(client, source, source_info.kind, temporary)
                await self._commit_staged_entry(
                    client,
                    temporary,
                    destination,
                    target_exists=destination_info is not None,
                )
            except BaseException:
                await self._cleanup_temporary(client, temporary)
                raise

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"Move: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        if source == _ROOT:
            raise IsADirectoryError("Cannot move root directory as a file")
        async with self._client_lease(f"Failed to move {src} to {dst}") as lease:
            client = lease.client
            source_info = await self._lstat_or_none(client, source)
            if source_info is None:
                raise FileNotFoundError(f"Source does not exist: {source.as_posix()}")
            if source_info.is_dir:
                raise IsADirectoryError(f"Source is a directory: {source.as_posix()}")
            if source == destination:
                if overwrite:
                    return
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            destination_info = await self._lstat_or_none(client, destination)
            if destination_info is not None:
                if destination_info.is_dir:
                    raise IsADirectoryError(f"Destination is a directory: {destination.as_posix()}")
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            await self._mkdir(client, destination.parent, parents=True, exist_ok=True)
            if destination_info is None:
                await self._io(client.rename(self._remote_path(source), self._remote_path(destination)))
                return
            try:
                await self._io(client.posix_rename(self._remote_path(source), self._remote_path(destination)))
            except asyncssh.SFTPOpUnsupported:
                pass
            else:
                return
            backup = self._temporary_path(destination, "move-backup")
            await self._io(client.rename(self._remote_path(destination), self._remote_path(backup)))
            try:
                await self._io(client.rename(self._remote_path(source), self._remote_path(destination)))
            except BaseException as primary:
                rollback_errors: list[BaseException] = []
                with anyio.CancelScope(shield=True):
                    try:
                        await self._io(client.rename(self._remote_path(backup), self._remote_path(destination)))
                    except BaseException as exc:
                        rollback_errors.append(exc)
                if rollback_errors:
                    raise BaseExceptionGroup("SFTP move and rollback failed", [primary, *rollback_errors]) from None
                raise
            await self._remove_if_exists(client, backup)

    @staticmethod
    def _is_descendant(path: PurePosixPath, parent: PurePosixPath) -> bool:
        if path == parent:
            return False
        try:
            path.relative_to(parent)
        except ValueError:
            return False
        return True

    async def _rollback_tree(self, client: asyncssh.SFTPClient, journal: _TreeJournal) -> None:
        errors: list[BaseException] = []
        for temporary in list(journal.temporary_entries):
            try:
                await self._remove_if_exists(client, temporary)
            except BaseException as exc:
                errors.append(exc)
        for created in sorted(journal.created_entries, key=lambda item: len(item.parts), reverse=True):
            try:
                await self._remove_if_exists(client, created)
            except BaseException as exc:
                errors.append(exc)
        for directory in sorted(journal.created_dirs, key=lambda item: len(item.parts), reverse=True):
            try:
                await self._remove_if_exists(client, directory)
            except BaseException as exc:
                errors.append(exc)
        for target, backup in reversed(journal.replaced_entries.items()):
            try:
                await self._remove_if_exists(client, target)
                await self._io(client.rename(self._remote_path(backup), self._remote_path(target)))
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise BaseExceptionGroup("Failed to rollback SFTP tree operation", errors)

    async def _cleanup_tree_backups(self, client: asyncssh.SFTPClient, journal: _TreeJournal) -> None:
        errors: list[BaseException] = []
        for backup in journal.replaced_entries.values():
            try:
                await self._remove_if_exists(client, backup)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise BaseExceptionGroup("Failed to clean SFTP tree backups", errors)
        journal.replaced_entries.clear()

    async def _preflight_tree_destination(
        self,
        client: asyncssh.SFTPClient,
        snapshot: list[tuple[PurePosixPath, list[_RawEntry]]],
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> FileInfo | None:
        destination_info = await self._lstat_or_none(client, destination)
        if destination_info is not None:
            if not destination_info.is_dir:
                raise FileExistsError(f"Destination is not a directory: {destination.as_posix()}")
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            await self._strict_snapshot(client, destination)
        for current, entries in snapshot:
            relative = current.relative_to(source)
            target_dir = destination if relative == PurePosixPath(".") else destination / relative
            for entry in entries:
                target = target_dir / entry.logical.name
                target_info = await self._lstat_or_none(client, target)
                if target_info is None:
                    continue
                if entry.kind is _RawKind.DIRECTORY:
                    if target_info.is_dir:
                        continue
                    if not overwrite:
                        raise FileExistsError(f"Destination already exists: {target.as_posix()}")
                    continue
                if target_info.is_dir:
                    raise IsADirectoryError(f"Destination is a directory: {target.as_posix()}")
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {target.as_posix()}")
        return destination_info

    async def _copytree_transaction(
        self,
        client: asyncssh.SFTPClient,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
        defer_cleanup: bool = False,
    ) -> _TreeJournal:
        source_info = await self._lstat_or_none(client, source)
        if source_info is None or not source_info.is_dir:
            raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
        if source == destination:
            raise FileExistsError(f"Source and destination are the same: {source.as_posix()}")
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")
        snapshot = await self._strict_snapshot(client, source)
        destination_info = await self._preflight_tree_destination(
            client,
            snapshot,
            source,
            destination,
            overwrite=overwrite,
        )

        journal = _TreeJournal()
        try:
            journal.created_dirs.update(
                await self._mkdir(client, destination, parents=True, exist_ok=destination_info is not None)
            )
            for current, entries in snapshot:
                relative = current.relative_to(source)
                target_dir = destination if relative == PurePosixPath(".") else destination / relative
                for entry in entries:
                    target = target_dir / entry.logical.name
                    target_info = await self._lstat_or_none(client, target)
                    if entry.kind is _RawKind.DIRECTORY:
                        if target_info is None:
                            created = await self._mkdir(client, target, parents=False, exist_ok=False)
                            journal.created_dirs.update(created)
                        elif not target_info.is_dir:
                            backup = self._temporary_path(target, "copytree-backup")
                            await self._io(client.rename(self._remote_path(target), self._remote_path(backup)))
                            journal.replaced_entries[target] = backup
                            await self._io(client.mkdir(self._remote_path(target)))
                            journal.created_dirs.add(target)
                        continue

                    temporary = self._temporary_path(target, "copytree")
                    journal.temporary_entries.add(temporary)
                    public_kind = self._entry_kind(entry.kind, entry.logical)
                    await self._copy_to_temporary(client, entry.logical, public_kind, temporary)
                    if target_info is None:
                        await self._io(client.rename(self._remote_path(temporary), self._remote_path(target)))
                        journal.created_entries.add(target)
                    else:
                        backup = self._temporary_path(target, "copytree-backup")
                        await self._io(client.rename(self._remote_path(target), self._remote_path(backup)))
                        journal.replaced_entries[target] = backup
                        await self._io(client.rename(self._remote_path(temporary), self._remote_path(target)))
                    journal.temporary_entries.discard(temporary)
        except BaseException as primary:
            rollback_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await self._rollback_tree(client, journal)
                except BaseException as exc:
                    rollback_error = exc
            if rollback_error is not None:
                raise BaseExceptionGroup("SFTP copytree and rollback failed", [primary, rollback_error]) from None
            raise

        if not defer_cleanup:
            await self._cleanup_tree_backups(client, journal)
        return journal

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"CopyTree: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        async with self._client_lease(f"Failed to copy tree {src} to {dst}") as lease:
            await self._copytree_transaction(lease.client, source, destination, overwrite=overwrite)

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"MoveTree: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        if source == _ROOT:
            raise OSError("Cannot move root directory")
        if source == destination:
            return
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")
        async with self._client_lease(f"Failed to move tree {src} to {dst}") as lease:
            client = lease.client
            source_info = await self._lstat_or_none(client, source)
            if source_info is None or not source_info.is_dir:
                raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
            await self._strict_snapshot(client, source)
            destination_info = await self._lstat_or_none(client, destination)
            if destination_info is None:
                await self._mkdir(client, destination.parent, parents=True, exist_ok=True)
                await self._io(client.rename(self._remote_path(source), self._remote_path(destination)))
                return
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if not destination_info.is_dir:
                raise FileExistsError(f"Destination is not a directory: {destination.as_posix()}")

            journal = await self._copytree_transaction(
                client,
                source,
                destination,
                overwrite=True,
                defer_cleanup=True,
            )
            tombstone = source.with_name(f".storegate-movetree-source-{uuid.uuid4().hex}")
            try:
                await self._io(client.rename(self._remote_path(source), self._remote_path(tombstone)))
            except BaseException as primary:
                rollback_error: BaseException | None = None
                with anyio.CancelScope(shield=True):
                    try:
                        await self._rollback_tree(client, journal)
                    except BaseException as exc:
                        rollback_error = exc
                if rollback_error is not None:
                    raise BaseExceptionGroup("SFTP movetree and rollback failed", [primary, rollback_error]) from None
                raise
            await self._cleanup_tree_backups(client, journal)
            await self._rmtree(client, tombstone)
