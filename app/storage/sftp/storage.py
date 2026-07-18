import asyncio
import contextlib
import stat as stat_module
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
import asyncssh

from app.storage.abstract import AbstractStorage, BytesLike, FileInfo, PathLike, make_cache_identity
from app.utils import coalesce_chunks

from .config import SFTPConfig
from .pool import SFTPChannelLease, SFTPChannelPool

_ROOT = PurePosixPath("/")
_CONNECTION_ERRORS = (
    asyncssh.ConnectionLost,
    asyncssh.DisconnectError,
    asyncssh.SFTPConnectionLost,
    asyncssh.SFTPNoConnection,
)


@dataclass(slots=True)
class _TreeJournal:
    created_dirs: set[PurePosixPath] = field(default_factory=set)
    created_files: set[PurePosixPath] = field(default_factory=set)
    replaced_files: dict[PurePosixPath, PurePosixPath] = field(default_factory=dict)
    temporary_files: set[PurePosixPath] = field(default_factory=set)


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
    if isinstance(exc, asyncssh.HostKeyNotVerifiable):
        return OSError(f"{message}: SSH host key verification failed")
    if isinstance(exc, (asyncssh.Error, asyncssh.SFTPError)):
        return OSError(f"{message}: {exc}")
    return exc


@final
class SFTPStorage(AbstractStorage):
    """SFTP client backend with bounded channel reuse and staged writes."""

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
            root_prefix=self._canonical_root.as_posix(),
            username=config.username,
        )

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

    def _logical_from_remote(self, path: str | PurePosixPath) -> PurePosixPath:
        remote = PurePosixPath(path)
        if not remote.is_absolute():
            raise OSError(f"SFTP server returned a relative path: {path}")
        try:
            relative = remote.relative_to(self._canonical_root)
        except ValueError:
            raise OSError(f"SFTP server returned a path outside root_prefix: {path}") from None
        return _ROOT if relative == PurePosixPath(".") else PurePosixPath("/", relative)

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
                configured_kind = self._attrs_kind(configured_attrs, self._configured_root)
                if configured_kind == "symlink":
                    raise OSError(f"SFTP root_prefix must not be a symlink: {self._configured_root.as_posix()}")
                if configured_kind != "directory":
                    raise NotADirectoryError(f"SFTP root_prefix is not a directory: {self._configured_root.as_posix()}")
                resolved = await self._io(client.realpath(self._configured_root.as_posix()))
                if not isinstance(resolved, str):
                    raise OSError("SFTP server returned an invalid canonical root")
                canonical = PurePosixPath(resolved)
                if not canonical.is_absolute():
                    raise OSError("SFTP server returned a relative canonical root")
                canonical_attrs = await self._io(client.stat(canonical.as_posix()))
                if self._attrs_kind(canonical_attrs, canonical) != "directory":
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
        self.log.debug("Connected")

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
                return self._attrs_kind(attrs, self._canonical_root) == "directory"
        except BaseException:
            return False

    @staticmethod
    def _attrs_kind(attrs: asyncssh.SFTPAttrs, path: PurePosixPath) -> str:
        if attrs.type == asyncssh.FILEXFER_TYPE_REGULAR:
            return "file"
        if attrs.type == asyncssh.FILEXFER_TYPE_DIRECTORY:
            return "directory"
        if attrs.type == asyncssh.FILEXFER_TYPE_SYMLINK:
            return "symlink"
        permissions = attrs.permissions
        if permissions is not None:
            if stat_module.S_ISREG(permissions):
                return "file"
            if stat_module.S_ISDIR(permissions):
                return "directory"
            if stat_module.S_ISLNK(permissions):
                return "symlink"
        raise OSError(f"Unsupported SFTP entry type: {path.as_posix()}")

    @staticmethod
    def _timestamp(seconds: int | None, nanoseconds: int | None) -> datetime | None:
        if seconds is None:
            return None
        result = datetime.fromtimestamp(seconds, UTC)
        if nanoseconds:
            result += timedelta(microseconds=nanoseconds // 1000)
        return result

    def _file_info_from_attrs(self, logical: PurePosixPath, attrs: asyncssh.SFTPAttrs) -> FileInfo:
        kind = self._attrs_kind(attrs, logical)
        if kind == "symlink":
            raise OSError(f"Symbolic links are not supported: {logical.as_posix()}")
        is_dir = kind == "directory"
        return FileInfo(
            path=logical.as_posix(),
            name="" if logical == _ROOT else logical.name,
            is_dir=is_dir,
            size=0 if is_dir else max(attrs.size or 0, 0),
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
            return await self._io(client.stat(self._canonical_root.as_posix()))
        current = _ROOT
        for index, part in enumerate(logical.parts[1:]):
            current /= part
            final = index == len(logical.parts[1:]) - 1
            try:
                attrs = await self._io(client.lstat(self._remote_path(current)))
            except asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath:
                if allow_missing_final:
                    return None
                raise
            kind = self._attrs_kind(attrs, current)
            if kind == "symlink":
                raise OSError(f"Symbolic links are not supported: {current.as_posix()}")
            if not final and kind != "directory":
                raise NotADirectoryError(f"Not a directory: {current.as_posix()}")
        return attrs

    async def _stat_info(self, client: asyncssh.SFTPClient, path: PathLike) -> FileInfo:
        logical = self._logical_path(path)
        attrs = await self._lstat_components(client, logical)
        assert attrs is not None
        return self._file_info_from_attrs(logical, attrs)

    async def _stat_or_none(self, client: asyncssh.SFTPClient, logical: PurePosixPath) -> FileInfo | None:
        try:
            return await self._stat_info(client, logical)
        except asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath, FileNotFoundError:
            return None

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
            return info is not None and not info.is_dir

    @override
    async def is_dir(self, path: PathLike) -> bool:
        async with self._client_lease(f"Failed to check whether {path} is a directory") as lease:
            info = await self._stat_or_none(lease.client, self._logical_path(path))
            return info is not None and info.is_dir

    @staticmethod
    def _validate_filename(filename: object) -> str:
        if not isinstance(filename, str):
            raise OSError("SFTP server returned a non-text filename")
        if filename in {"", ".", ".."} or "\x00" in filename or "/" in filename or "\\" in filename:
            raise OSError(f"SFTP server returned an invalid filename: {filename!r}")
        return filename

    async def _scandir(self, client: asyncssh.SFTPClient, logical: PurePosixPath) -> list[FileInfo]:
        info = await self._stat_info(client, logical)
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
        entries: list[FileInfo] = []
        iterator = client.scandir(self._remote_path(logical)).__aiter__()
        while True:
            try:
                entry = await self._io(anext(iterator))
            except StopAsyncIteration:
                break
            if entry.filename in {".", ".."}:
                continue
            filename = self._validate_filename(entry.filename)
            entries.append(self._file_info_from_attrs(logical / filename, entry.attrs))
        entries.sort(key=lambda item: item.path)
        return entries

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        logical = self._logical_path(path)
        async with self._client_lease(f"Failed to iterate directory {path}") as lease:
            entries = await self._scandir(lease.client, logical)
        for entry in entries:
            yield entry

    async def _walk_snapshot(
        self,
        client: asyncssh.SFTPClient,
        path: PathLike,
    ) -> list[tuple[PurePosixPath, list[FileInfo], list[FileInfo]]]:
        root = self._logical_path(path)
        root_info = await self._stat_info(client, root)
        if not root_info.is_dir:
            raise NotADirectoryError(f"Not a directory: {root.as_posix()}")
        pending = [root]
        snapshot: list[tuple[PurePosixPath, list[FileInfo], list[FileInfo]]] = []
        while pending:
            current = pending.pop()
            entries = await self._scandir(client, current)
            directories = [entry for entry in entries if entry.is_dir]
            files = [entry for entry in entries if not entry.is_dir]
            snapshot.append((current, directories, files))
            pending.extend(PurePosixPath(entry.path) for entry in reversed(directories))
        return snapshot

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[tuple[str, list[FileInfo], list[FileInfo]]]:
        async with self._client_lease(f"Failed to walk directory {path}") as lease:
            snapshot = await self._walk_snapshot(lease.client, path)
        for current, directories, files in snapshot:
            yield current.as_posix(), directories, files

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

        existing = await self._stat_or_none(client, logical)
        if existing is not None:
            if not existing.is_dir or not exist_ok:
                raise FileExistsError(f"Path already exists: {logical.as_posix()}")
            return set()

        created: set[PurePosixPath] = set()
        if not parents:
            parent = await self._stat_or_none(client, logical.parent)
            if parent is None:
                raise FileNotFoundError(f"Parent directory does not exist: {logical.parent.as_posix()}")
            if not parent.is_dir:
                raise NotADirectoryError(f"Not a directory: {logical.parent.as_posix()}")
            await self._io(client.mkdir(self._remote_path(logical)))
            created.add(logical)
            return created

        current = _ROOT
        for part in logical.parts[1:]:
            current /= part
            info = await self._stat_or_none(client, current)
            if info is None:
                await self._io(client.mkdir(self._remote_path(current)))
                created.add(current)
            elif not info.is_dir:
                raise FileExistsError(f"Path component is a file: {current.as_posix()}")
        return created

    @override
    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        async with self._client_lease(f"Failed to create directory {path}") as lease:
            await self._mkdir(lease.client, path, parents=parents, exist_ok=exist_ok)

    @staticmethod
    def _temporary_path(target: PurePosixPath, kind: str) -> PurePosixPath:
        return target.with_name(f".{target.name}.storegate-{kind}-{uuid.uuid4().hex}")

    async def _remove_if_exists(self, client: asyncssh.SFTPClient, path: PurePosixPath) -> None:
        try:
            info = await self._stat_info(client, path)
        except asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath, FileNotFoundError:
            return
        if info.is_dir:
            await self._io(client.rmdir(self._remote_path(path)))
        else:
            await self._io(client.remove(self._remote_path(path)))

    async def _commit_staged_file(
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
                    "SFTP staged commit and rollback failed", [primary, *rollback_errors]
                ) from None
            raise
        await self._io(client.remove(self._remote_path(backup)))

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
                kind = self._attrs_kind(attrs, target)
                if kind == "directory":
                    raise IsADirectoryError(f"Destination is a directory: {target.as_posix()}")
                if kind != "file":
                    raise OSError(f"Unsupported destination type: {target.as_posix()}")
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {target.as_posix()}")
            await self._mkdir(client, target.parent, parents=True, exist_ok=True)
            temporary = self._temporary_path(target, "upload")
            handle: asyncssh.SFTPClientFile[bytes] | None = None
            try:
                handle = await self._io(client.open(self._remote_path(temporary), "wb", encoding=None))
                async for chunk in coalesce_chunks(stream, self._config.chunk_size):
                    await self._io(handle.write(chunk))
                await self._io(handle.close())
                handle = None
                await self._commit_staged_file(client, temporary, target, target_exists=target_exists)
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
            info = await self._stat_info(client, logical)
            if info.is_dir:
                raise IsADirectoryError(f"Path is a directory: {logical.as_posix()}")
            if offset >= info.size:
                return
            handle: asyncssh.SFTPClientFile[bytes] | None = None
            try:
                handle = await self._io(client.open(self._remote_path(logical), "rb", encoding=None))
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
            info = await self._stat_or_none(lease.client, logical)
            if info is None:
                if missing_ok:
                    return
                raise FileNotFoundError(f"Path does not exist: {logical.as_posix()}")
            if info.is_dir:
                raise IsADirectoryError(f"Path is a directory: {logical.as_posix()}")
            await self._io(lease.client.remove(self._remote_path(logical)))

    @override
    async def rmdir(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == _ROOT:
            raise OSError("Cannot remove root directory")
        async with self._client_lease(f"Failed to remove directory {path}") as lease:
            info = await self._stat_or_none(lease.client, logical)
            if info is None:
                raise FileNotFoundError(f"Path does not exist: {logical.as_posix()}")
            if not info.is_dir:
                raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
            if await self._scandir(lease.client, logical):
                raise OSError(f"Directory not empty: {logical.as_posix()}")
            await self._io(lease.client.rmdir(self._remote_path(logical)))

    async def _rmtree(self, client: asyncssh.SFTPClient, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == _ROOT:
            raise OSError("Cannot remove root directory")
        info = await self._stat_or_none(client, logical)
        if info is None:
            raise FileNotFoundError(f"Path does not exist: {logical.as_posix()}")
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
        snapshot = await self._walk_snapshot(client, logical)
        for _, _, files in snapshot:
            for file in files:
                await self._io(client.remove(self._remote_path(PurePosixPath(file.path))))
        for current, _, _ in reversed(snapshot):
            await self._io(client.rmdir(self._remote_path(current)))

    @override
    async def rmtree(self, path: PathLike) -> None:
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

    @override
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        if source == _ROOT:
            raise IsADirectoryError("Cannot copy root directory as a file")
        async with self._client_lease(f"Failed to copy {src} to {dst}") as lease:
            client = lease.client
            source_info = await self._stat_or_none(client, source)
            if source_info is None:
                raise FileNotFoundError(f"Source does not exist: {source.as_posix()}")
            if source_info.is_dir:
                raise IsADirectoryError(f"Source is a directory: {source.as_posix()}")
            if source == destination:
                if overwrite:
                    return
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            destination_info = await self._stat_or_none(client, destination)
            if destination_info is not None:
                if destination_info.is_dir:
                    raise IsADirectoryError(f"Destination is a directory: {destination.as_posix()}")
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            await self._mkdir(client, destination.parent, parents=True, exist_ok=True)
            temporary = self._temporary_path(destination, "copy")
            try:
                await self._copy_contents(client, source, temporary)
                await self._commit_staged_file(
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
        if source == _ROOT:
            raise IsADirectoryError("Cannot move root directory as a file")
        async with self._client_lease(f"Failed to move {src} to {dst}") as lease:
            client = lease.client
            source_info = await self._stat_or_none(client, source)
            if source_info is None:
                raise FileNotFoundError(f"Source does not exist: {source.as_posix()}")
            if source_info.is_dir:
                raise IsADirectoryError(f"Source is a directory: {source.as_posix()}")
            if source == destination:
                if overwrite:
                    return
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            destination_info = await self._stat_or_none(client, destination)
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
            await self._io(client.remove(self._remote_path(backup)))

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
        for temporary in list(journal.temporary_files):
            try:
                await self._remove_if_exists(client, temporary)
            except BaseException as exc:
                errors.append(exc)
        for created in sorted(journal.created_files, key=lambda item: len(item.parts), reverse=True):
            try:
                await self._remove_if_exists(client, created)
            except BaseException as exc:
                errors.append(exc)
        for target, backup in reversed(journal.replaced_files.items()):
            try:
                await self._remove_if_exists(client, target)
                await self._io(client.rename(self._remote_path(backup), self._remote_path(target)))
            except BaseException as exc:
                errors.append(exc)
        for directory in sorted(journal.created_dirs, key=lambda item: len(item.parts), reverse=True):
            try:
                await self._io(client.rmdir(self._remote_path(directory)))
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise BaseExceptionGroup("Failed to rollback SFTP tree operation", errors)

    async def _cleanup_tree_backups(self, client: asyncssh.SFTPClient, journal: _TreeJournal) -> None:
        errors: list[BaseException] = []
        for backup in journal.replaced_files.values():
            try:
                await self._remove_if_exists(client, backup)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise BaseExceptionGroup("Failed to clean SFTP tree backups", errors)
        journal.replaced_files.clear()

    async def _copytree_transaction(
        self,
        client: asyncssh.SFTPClient,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
        defer_cleanup: bool = False,
    ) -> _TreeJournal:
        source_info = await self._stat_or_none(client, source)
        if source_info is None or not source_info.is_dir:
            raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
        if source == destination:
            raise FileExistsError(f"Source and destination are the same: {source.as_posix()}")
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")
        snapshot = await self._walk_snapshot(client, source)
        destination_info = await self._stat_or_none(client, destination)
        if destination_info is not None:
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if not destination_info.is_dir:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")

        journal = _TreeJournal()
        try:
            journal.created_dirs.update(
                await self._mkdir(client, destination, parents=True, exist_ok=destination_info is not None)
            )
            for current, directories, files in snapshot:
                relative = current.relative_to(source)
                target_dir = destination if relative == PurePosixPath(".") else destination / relative
                for directory in directories:
                    target = target_dir / directory.name
                    target_info = await self._stat_or_none(client, target)
                    if target_info is None:
                        journal.created_dirs.update(await self._mkdir(client, target, parents=False, exist_ok=False))
                    elif not target_info.is_dir:
                        raise FileExistsError(f"Destination is a file: {target.as_posix()}")
                for file in files:
                    source_file = PurePosixPath(file.path)
                    target = target_dir / file.name
                    target_info = await self._stat_or_none(client, target)
                    temporary = self._temporary_path(target, "copytree")
                    journal.temporary_files.add(temporary)
                    await self._copy_contents(client, source_file, temporary)
                    if target_info is None:
                        await self._io(client.rename(self._remote_path(temporary), self._remote_path(target)))
                        journal.created_files.add(target)
                    else:
                        if target_info.is_dir:
                            raise IsADirectoryError(f"Destination is a directory: {target.as_posix()}")
                        backup = self._temporary_path(target, "copytree-backup")
                        await self._io(client.rename(self._remote_path(target), self._remote_path(backup)))
                        await self._io(client.rename(self._remote_path(temporary), self._remote_path(target)))
                        journal.replaced_files[target] = backup
                    journal.temporary_files.discard(temporary)
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
        async with self._client_lease(f"Failed to copy tree {src} to {dst}") as lease:
            await self._copytree_transaction(lease.client, source, destination, overwrite=overwrite)

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        if source == _ROOT:
            raise OSError("Cannot move root directory")
        if source == destination:
            return
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")
        async with self._client_lease(f"Failed to move tree {src} to {dst}") as lease:
            client = lease.client
            source_info = await self._stat_or_none(client, source)
            if source_info is None or not source_info.is_dir:
                raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
            destination_info = await self._stat_or_none(client, destination)
            if destination_info is None:
                await self._mkdir(client, destination.parent, parents=True, exist_ok=True)
                await self._io(client.rename(self._remote_path(source), self._remote_path(destination)))
                return
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if not destination_info.is_dir:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")

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
