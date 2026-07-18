import contextlib
import errno
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import final, override

import aioftp
import anyio
from aioftp.client import BasicListInfo, UnixListInfo

from app.log import escape_tag
from app.storage.abstract import (
    AbstractStorage,
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    UnsupportedOperationError,
    WalkEntry,
    make_cache_identity,
)
from app.utils import ExceptionTranslator, coalesce_chunks, flatten_exception_group

from .config import FTPConfig
from .pool import FTPClientLease, FTPClientPool

type FTPFacts = BasicListInfo | UnixListInfo | dict[str, str]

_UNSUPPORTED_ERRNO = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)

translator = ExceptionTranslator(
    bypass=OSError,
    catch=aioftp.AIOFTPException,
    default=OSError,
)


@translator.handles(aioftp.StatusCodeError)
def _(exc_group: ExceptionGroup[aioftp.StatusCodeError], msg: str) -> OSError:
    first = next(flatten_exception_group(exc_group))
    code = first.received_codes[-1]
    if code.matches("530"):
        return PermissionError(f"{msg}: {first}")
    return OSError(f"{msg}: {first}")


def _parse_mlsx_datetime(value: object) -> datetime | None:
    if not value:
        return None
    value = str(value)
    format_string = "%Y%m%d%H%M%S.%f" if "." in value else "%Y%m%d%H%M%S"
    try:
        return datetime.strptime(value, format_string).replace(tzinfo=UTC)
    except ValueError:
        return None


def _status_matches(exc: aioftp.StatusCodeError, pattern: str) -> bool:
    return exc.received_codes[-1].matches(pattern)


@final
class FTPStorage(AbstractStorage):
    """Plain FTP client storage backend.

    FTP has no portable no-follow metadata primitive. Explicit MLSD/MLST
    link and special facts are rejected or hidden according to the operation,
    but a server may instead report a followed target as a file or directory.
    In that case this client cannot detect the link and does not provide a
    client-side confinement guarantee.
    """

    def __init__(self, config: str | Path | FTPConfig) -> None:
        super().__init__()
        self._config = config if isinstance(config, FTPConfig) else FTPConfig.from_file(config)
        self._root_prefix = PurePosixPath(self._config.root_prefix)
        self._pool = self._new_pool()

    def _new_pool(self) -> FTPClientPool:
        return FTPClientPool(
            max_connections=self._config.max_connections,
            close_timeout=self._config.timeout,
            factory=self._connect_client,
            closer=self._close_client,
            logger=self.log,
        )

    @property
    @override
    def id(self) -> str:
        return f"ftp:{self._config.username}@{self._config.host}:{self._config.port}{self._config.root_prefix}"

    @property
    @override
    def cache_identity(self) -> str:
        return make_cache_identity(
            "ftp",
            encoding=self._config.encoding,
            host=self._config.host,
            port=self._config.port,
            root_prefix=self._config.root_prefix,
            username=self._config.username,
        )

    def _logical_path(self, path: PathLike) -> PurePosixPath:
        raw = PurePosixPath(path)
        if "\x00" in raw.as_posix():
            raise ValueError("FTP path must not contain NUL")
        if ".." in raw.parts:
            raise ValueError("FTP path must not contain '..' segments")
        return self.normalize_path(raw)

    def _remote_path(self, path: PathLike) -> str:
        logical = self._logical_path(path)
        relative = logical.relative_to("/")
        if relative == PurePosixPath("."):
            return self._root_prefix.as_posix()
        return (self._root_prefix / relative).as_posix()

    def _logical_from_remote(self, path: str | PurePosixPath) -> PurePosixPath:
        remote = PurePosixPath(path)
        if not remote.is_absolute():
            raise OSError(f"FTP server returned a relative path: {path}")
        try:
            relative = remote.relative_to(self._root_prefix)
        except ValueError:
            raise OSError(f"FTP server returned a path outside root_prefix: {path}") from None
        return PurePosixPath("/") if relative == PurePosixPath(".") else PurePosixPath("/", relative)

    @staticmethod
    def _entry_kind_from_facts(facts: FTPFacts) -> EntryKind | None:
        if "link_dst" in facts:
            return None
        entry_type = str(facts.get("type", "")).casefold()
        if entry_type == "file":
            return EntryKind.FILE
        if entry_type == "dir":
            return EntryKind.DIRECTORY
        return None

    @staticmethod
    def _unsupported_entry(logical_path: PurePosixPath, facts: FTPFacts) -> UnsupportedOperationError:
        entry_type = facts.get("type")
        link_detail = f", link_dst={facts.get("link_dst")!r}" if "link_dst" in facts else ""
        return UnsupportedOperationError(
            _UNSUPPORTED_ERRNO,
            f"Unsupported FTP entry type {entry_type!r}{link_detail}: {logical_path.as_posix()}",
        )

    def _new_client(self) -> aioftp.Client:
        return aioftp.Client(
            connection_timeout=self._config.timeout,
            encoding=self._config.encoding,
            path_timeout=self._config.timeout,
            socket_timeout=self._config.timeout,
        )

    async def _connect_client(self) -> aioftp.Client:
        client = self._new_client()
        try:
            await client.connect(self._config.host, self._config.port)
            await client.login(
                self._config.username,
                self._config.password.get_secret_value(),
            )
            facts = await client.stat(self._root_prefix)
            root_kind = self._entry_kind_from_facts(facts)
            if root_kind is None:
                raise self._unsupported_entry(PurePosixPath("/"), facts)
            if root_kind is not EntryKind.DIRECTORY:
                raise NotADirectoryError(f"FTP root_prefix is not a directory: {self._config.root_prefix}")
        except Exception:
            client.close()
            raise
        return client

    @staticmethod
    async def _close_client(client: aioftp.Client) -> None:
        try:
            await client.quit()
        except Exception:
            client.close()

    @asynccontextmanager
    async def _temporary_client(self) -> AsyncIterator[aioftp.Client]:
        client = await self._connect_client()
        try:
            yield client
        finally:
            with anyio.CancelScope(shield=True):
                await self._close_client(client)

    @staticmethod
    def _invalidates_client(exc: BaseException) -> bool:
        if isinstance(
            exc,
            (
                GeneratorExit,
                FileNotFoundError,
                FileExistsError,
                IsADirectoryError,
                NotADirectoryError,
                PermissionError,
                UnsupportedOperationError,
                ValueError,
                aioftp.StatusCodeError,
            ),
        ):
            return False
        if isinstance(exc, anyio.get_cancelled_exc_class()):
            return True
        return True

    @asynccontextmanager
    async def _client_lease(self) -> AsyncIterator[FTPClientLease]:
        async with self._pool.acquire() as lease:
            try:
                yield lease
            except BaseException as exc:
                if self._invalidates_client(exc):
                    lease.invalidate()
                raise

    @override
    @translator.wrap("Failed to connect to FTP server")
    async def connect(self) -> None:
        if self._pool.is_closed:
            self._pool = self._new_pool()
        await self._pool.start()
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
            async with self._client_lease() as lease:
                facts = await lease.client.stat(self._root_prefix)
                return self._entry_kind_from_facts(facts) is EntryKind.DIRECTORY
        except Exception:
            return False

    def _file_info_from_facts(self, logical_path: PurePosixPath, facts: FTPFacts) -> FileInfo:
        kind = self._entry_kind_from_facts(facts)
        if kind is None:
            raise self._unsupported_entry(logical_path, facts)

        try:
            size = int(facts.get("size", "0"))
        except ValueError:
            size = 0

        return FileInfo(
            path=logical_path.as_posix(),
            name="" if logical_path == PurePosixPath("/") else logical_path.name,
            kind=kind,
            size=0 if kind is EntryKind.DIRECTORY else max(0, size),
            modified=_parse_mlsx_datetime(facts.get("modify")),
            created=_parse_mlsx_datetime(facts.get("create")),
        )

    async def _stat_with_facts(self, client: aioftp.Client, path: PathLike) -> tuple[FileInfo, FTPFacts]:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            return FileInfo(path="/", name="", kind=EntryKind.DIRECTORY), {"type": "dir"}

        try:
            facts = await client.stat(self._remote_path(logical))
        except aioftp.StatusCodeError as exc:
            if _status_matches(exc, "550"):
                raise FileNotFoundError(f"FTP path not found: {logical.as_posix()}") from exc
            raise
        return self._file_info_from_facts(logical, facts), facts

    async def _stat(self, client: aioftp.Client, path: PathLike) -> FileInfo:
        info, _ = await self._stat_with_facts(client, path)
        return info

    async def _exists(self, client: aioftp.Client, path: PathLike) -> bool:
        try:
            await self._stat(client, path)
        except FileNotFoundError:
            return False
        return True

    @override
    @translator.wrap("Failed to stat {path}")
    async def stat(self, path: PathLike) -> FileInfo:
        async with self._client_lease() as lease:
            return await self._stat(lease.client, path)

    @override
    @translator.wrap("Failed to check existence of {path}")
    async def exists(self, path: PathLike) -> bool:
        async with self._client_lease() as lease:
            return await self._exists(lease.client, path)

    @override
    @translator.wrap("Failed to check whether {path} is a file")
    async def is_file(self, path: PathLike) -> bool:
        async with self._client_lease() as lease:
            try:
                info = await self._stat(lease.client, path)
            except FileNotFoundError:
                return False
            return info.kind is EntryKind.FILE

    @override
    @translator.wrap("Failed to check whether {path} is a directory")
    async def is_dir(self, path: PathLike) -> bool:
        async with self._client_lease() as lease:
            try:
                info = await self._stat(lease.client, path)
            except FileNotFoundError:
                return False
            return info.kind is EntryKind.DIRECTORY

    async def _list_raw(
        self,
        client: aioftp.Client,
        path: PathLike,
        *,
        validate: bool = True,
    ) -> list[tuple[PurePosixPath, FTPFacts]]:
        logical = self._logical_path(path)
        if validate:
            info = await self._stat(client, logical)
            if info.kind is not EntryKind.DIRECTORY:
                raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")

        try:
            entries = await client.list(self._remote_path(logical))
        except aioftp.StatusCodeError as exc:
            if _status_matches(exc, "550"):
                raise FileNotFoundError(f"FTP directory not found: {logical.as_posix()}") from exc
            raise

        return [(self._logical_from_remote(remote_path), facts) for remote_path, facts in entries]

    async def _list(
        self,
        client: aioftp.Client,
        path: PathLike,
        *,
        validate: bool = True,
        strict: bool = False,
    ) -> list[FileInfo]:
        result: list[FileInfo] = []
        for logical_path, facts in await self._list_raw(client, path, validate=validate):
            if self._entry_kind_from_facts(facts) is None:
                if strict:
                    raise self._unsupported_entry(logical_path, facts)
                self.log.debug(f"Skipping unsupported FTP entry type {facts.get("type")!r}: {logical_path.as_posix()}")
                continue
            result.append(self._file_info_from_facts(logical_path, facts))
        result.sort(key=lambda entry: entry.path)
        return result

    @override
    @translator.wrap_agen("Failed to iterate directory {path}")
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        async with self._client_lease() as lease:
            entries = await self._list(lease.client, path)
        for entry in entries:
            yield entry

    async def _walk_snapshot(
        self,
        client: aioftp.Client,
        path: PathLike,
        *,
        strict: bool,
    ) -> list[WalkEntry]:
        root = self._logical_path(path)
        info = await self._stat(client, root)
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {root.as_posix()}")

        result: list[WalkEntry] = []
        pending = [root]
        while pending:
            current = pending.pop()
            entries = tuple(await self._list(client, current, validate=False, strict=strict))
            result.append(WalkEntry(path=current.as_posix(), entries=entries))
            directories = [entry for entry in entries if entry.kind is EntryKind.DIRECTORY]
            pending.extend(PurePosixPath(entry.path) for entry in reversed(directories))
        return result

    @override
    @translator.wrap_agen("Failed to walk directory {path}")
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        async with self._client_lease() as lease:
            snapshot = await self._walk_snapshot(lease.client, path, strict=False)
        for entry in snapshot:
            yield entry

    async def _mkdir(
        self,
        client: aioftp.Client,
        path: PathLike,
        *,
        parents: bool,
        exist_ok: bool,
    ) -> list[PurePosixPath]:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            if exist_ok:
                return []
            raise FileExistsError("Root directory already exists")

        try:
            existing = await self._stat(client, logical)
        except FileNotFoundError:
            pass
        else:
            if existing.kind is not EntryKind.DIRECTORY:
                raise FileExistsError(f"Path is not a directory: {logical.as_posix()}")
            if exist_ok:
                return []
            raise FileExistsError(f"Directory already exists: {logical.as_posix()}")

        if not parents:
            parent_info = await self._stat(client, logical.parent)
            if parent_info.kind is not EntryKind.DIRECTORY:
                raise NotADirectoryError(f"Parent is not a directory: {logical.parent.as_posix()}")
            await client.make_directory(self._remote_path(logical), parents=False)
            return [logical]

        ancestors = [parent for parent in reversed(logical.parents) if parent != PurePosixPath("/")]
        ancestors.append(logical)
        created: list[PurePosixPath] = []
        for directory in ancestors:
            try:
                info = await self._stat(client, directory)
            except FileNotFoundError:
                await client.make_directory(self._remote_path(directory), parents=False)
                created.append(directory)
            else:
                if info.kind is not EntryKind.DIRECTORY:
                    raise FileExistsError(f"Path is not a directory: {directory.as_posix()}")
        return created

    @override
    @translator.wrap("Failed to create directory {path} (parents={parents}, exist_ok={exist_ok})")
    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        logical = self._logical_path(path)
        self.log.info(f"MkDir: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease() as lease:
            await self._mkdir(lease.client, path, parents=parents, exist_ok=exist_ok)

    async def _cleanup_partial_file(self, logical: PurePosixPath) -> None:
        with anyio.move_on_after(self._config.timeout, shield=True):
            with contextlib.suppress(Exception):
                async with self._temporary_client() as client:
                    await client.remove_file(self._remote_path(logical))

    @override
    @translator.wrap("Failed to upload stream to {remote_path} (overwrite={overwrite})")
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        logical = self._logical_path(remote_path)
        async with self._client_lease() as lease:
            client = lease.client
            existed = False
            try:
                info = await self._stat(client, logical)
            except FileNotFoundError:
                pass
            else:
                existed = True
                if info.kind is EntryKind.DIRECTORY:
                    raise IsADirectoryError(f"Is a directory: {logical.as_posix()}")
                if info.kind is not EntryKind.FILE:
                    raise UnsupportedOperationError(
                        _UNSUPPORTED_ERRNO, f"Unsupported FTP upload destination kind: {logical.as_posix()}"
                    )
                if not overwrite:
                    raise FileExistsError(f"File already exists: {logical.as_posix()}")

            self.log.info(f"Upload: <y>{escape_tag(logical.as_posix())}</y>")
            await self._mkdir(client, logical.parent, parents=True, exist_ok=True)
            try:
                async with client.upload_stream(self._remote_path(logical)) as writer:
                    async for chunk in coalesce_chunks(stream, self._config.chunk_size):
                        await writer.write(chunk)
            except BaseException:
                lease.invalidate()
                if not existed:
                    await self._cleanup_partial_file(logical)
                raise

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        try:
            if offset < 0:
                raise ValueError("offset must be non-negative")

            logical = self._logical_path(remote_path)
            async with self._pool.acquire() as lease:
                try:
                    info, facts = await self._stat_with_facts(lease.client, logical)
                    if info.kind is EntryKind.DIRECTORY:
                        raise IsADirectoryError(f"Is a directory: {logical.as_posix()}")
                    if info.kind is not EntryKind.FILE:
                        raise UnsupportedOperationError(
                            _UNSUPPORTED_ERRNO, f"Unsupported FTP download source kind: {logical.as_posix()}"
                        )
                    if "size" in facts and offset >= info.size:
                        return

                    self.log.debug(
                        f"Download: <y>{escape_tag(logical.as_posix())}</y> "
                        f"(<g>{info.size}</g> bytes, offset=<g>{offset}</g>)"
                    )
                    reader = await lease.client.download_stream(self._remote_path(logical), offset=offset)
                    try:
                        async for chunk in reader.iter_by_block(self._config.chunk_size):
                            yield chunk
                    finally:
                        try:
                            with anyio.CancelScope(shield=True):
                                await reader.finish()
                        except BaseException:
                            lease.invalidate()
                            raise
                except BaseException as exc:
                    if self._invalidates_client(exc):
                        lease.invalidate()
                    raise
        except OSError:
            raise
        except aioftp.StatusCodeError as exc:
            if _status_matches(exc, "530"):
                raise PermissionError(f"Failed to download {remote_path}: {exc}") from exc
            raise OSError(f"Failed to download {remote_path}: {exc}") from exc
        except aioftp.AIOFTPException as exc:
            raise OSError(f"Failed to download {remote_path}: {exc}") from exc

    @override
    @translator.wrap("Failed to unlink {path} (missing_ok={missing_ok})")
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            raise IsADirectoryError("Cannot unlink root directory")

        async with self._client_lease() as lease:
            try:
                info = await self._stat(lease.client, logical)
            except FileNotFoundError:
                if missing_ok:
                    return
                raise
            if info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {logical.as_posix()}")
            if info.kind is not EntryKind.FILE:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP unlink entry kind: {logical.as_posix()}"
                )
            self.log.info(f"Delete: <y>{escape_tag(logical.as_posix())}</y>")
            await lease.client.remove_file(self._remote_path(logical))

    async def _rmdir(self, client: aioftp.Client, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            raise OSError("Cannot remove root directory")

        info = await self._stat(client, logical)
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
        if await self._list_raw(client, logical, validate=False):
            raise OSError(f"Directory not empty: {logical.as_posix()}")
        await client.remove_directory(self._remote_path(logical))

    @override
    @translator.wrap("Failed to remove directory {path}")
    async def rmdir(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        self.log.info(f"Delete dir: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease() as lease:
            await self._rmdir(lease.client, path)

    async def _rmtree(self, client: aioftp.Client, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            raise OSError("Cannot remove root directory")

        info = await self._stat(client, logical)
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")

        snapshot = await self._walk_snapshot(client, logical, strict=True)
        for walk_entry in snapshot:
            for entry in walk_entry.entries:
                if entry.kind is EntryKind.FILE:
                    await client.remove_file(self._remote_path(entry.path))
                elif entry.kind is not EntryKind.DIRECTORY:
                    raise UnsupportedOperationError(
                        _UNSUPPORTED_ERRNO, f"Unsupported FTP tree entry kind: {entry.path}"
                    )
        for walk_entry in sorted(snapshot, key=lambda item: len(PurePosixPath(item.path).parts), reverse=True):
            await client.remove_directory(self._remote_path(walk_entry.path))

    @override
    @translator.wrap("Failed to remove directory tree {path}")
    async def rmtree(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        self.log.info(f"RmTree: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease() as lease:
            await self._rmtree(lease.client, path)

    async def _copy_stream(
        self,
        source_client: aioftp.Client,
        destination_client: aioftp.Client,
        source: PurePosixPath,
        destination: PurePosixPath,
    ) -> None:
        reader = await source_client.download_stream(self._remote_path(source))
        try:
            async with destination_client.upload_stream(self._remote_path(destination)) as writer:
                async for chunk in reader.iter_by_block(self._config.chunk_size):
                    await writer.write(chunk)
        finally:
            with anyio.CancelScope(shield=True):
                await reader.finish()

    async def _copy_file(
        self,
        source_client: aioftp.Client,
        destination_client: aioftp.Client,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool = True,
    ) -> bool:
        source_info = await self._stat(source_client, source)
        if source_info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {source.as_posix()}")
        if source_info.kind is not EntryKind.FILE:
            raise UnsupportedOperationError(
                _UNSUPPORTED_ERRNO, f"Unsupported FTP copy source kind: {source.as_posix()}"
            )

        destination_existed = False
        try:
            destination_info = await self._stat(source_client, destination)
        except FileNotFoundError:
            pass
        else:
            destination_existed = True
            if destination_info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {destination.as_posix()}")
            if destination_info.kind is not EntryKind.FILE:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copy destination kind: {destination.as_posix()}"
                )
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")

        await self._mkdir(source_client, destination.parent, parents=True, exist_ok=True)
        await self._copy_stream(source_client, destination_client, source, destination)
        return not destination_existed

    async def _reconcile_move_failure(
        self,
        source: PurePosixPath,
        destination: PurePosixPath,
        temporary: PurePosixPath,
        destination_existed: bool,
    ) -> None:
        """Restore source/destination from the actual state after a failed rename."""
        with anyio.CancelScope(shield=True):
            try:
                async with self._client_lease() as lease:

                    async def _exists(path: PurePosixPath) -> bool:
                        try:
                            await self._stat(lease.client, path)
                        except FileNotFoundError:
                            return False
                        return True

                    source_exists = await _exists(source)
                    destination_exists = await _exists(destination)
                    temporary_exists = await _exists(temporary)

                    if not source_exists and destination_exists:
                        await lease.client.rename(self._remote_path(destination), self._remote_path(source))
                        destination_exists = False
                    if destination_existed and temporary_exists and not destination_exists:
                        await lease.client.rename(self._remote_path(temporary), self._remote_path(destination))
                        temporary_exists = False
                    elif not destination_existed and temporary_exists:
                        await lease.client.remove_file(self._remote_path(temporary))
                        temporary_exists = False
                    if temporary_exists:
                        await lease.client.remove_file(self._remote_path(temporary))
            except BaseException:
                self.log.exception(f"Failed to reconcile move state: {source} → {destination}")

    async def _move_backup_exists(self, temporary: PurePosixPath) -> bool | None:
        with anyio.CancelScope(shield=True):
            try:
                async with self._client_lease() as lease:
                    return await self._exists(lease.client, temporary)
            except BaseException:
                self.log.exception(f"Failed to inspect move backup: {temporary}")
                return None

    @override
    @translator.wrap("Failed to move {src} → {dst}")
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"Move: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        if source == PurePosixPath("/"):
            raise OSError("Cannot move root directory")

        failure: BaseException | None = None
        temporary: PurePosixPath | None = None
        destination_existed = False
        async with self._client_lease() as lease:
            source_info = await self._stat(lease.client, source)
            if source_info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {source.as_posix()}")
            if source_info.kind is not EntryKind.FILE:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP move source kind: {source.as_posix()}"
                )
            if source == destination:
                if not overwrite:
                    raise FileExistsError(f"Source and destination are the same: {source.as_posix()}")
                return
            try:
                destination_info = await self._stat(lease.client, destination)
            except FileNotFoundError:
                pass
            else:
                destination_existed = True
                if destination_info.kind is EntryKind.DIRECTORY:
                    raise IsADirectoryError(f"Destination is a directory: {destination.as_posix()}")
                if destination_info.kind is not EntryKind.FILE:
                    raise UnsupportedOperationError(
                        _UNSUPPORTED_ERRNO, f"Unsupported FTP move destination kind: {destination.as_posix()}"
                    )
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {destination.as_posix()}")

            await self._mkdir(lease.client, destination.parent, parents=True, exist_ok=True)
            temporary = destination.parent / f".storegate-move-{uuid.uuid4().hex}"
            staged = False
            try:
                if destination_existed:
                    await lease.client.rename(self._remote_path(destination), self._remote_path(temporary))
                    staged = True
                await lease.client.rename(self._remote_path(source), self._remote_path(destination))
            except BaseException as exc:
                lease.invalidate()
                failure = exc
            else:
                if staged:
                    try:
                        with anyio.CancelScope(shield=True):
                            await lease.client.remove_file(self._remote_path(temporary))
                    except BaseException as exc:
                        lease.invalidate()
                        failure = exc

        if failure is not None:
            assert temporary is not None
            backup_exists = await self._move_backup_exists(temporary)
            if backup_exists is True:
                await self._reconcile_move_failure(source, destination, temporary, destination_existed)
                raise failure
            if backup_exists is None:
                raise failure
            self.log.warning(f"Move backup cleanup response lost after commit: {temporary}")

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"Copy: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        if source == PurePosixPath("/"):
            raise IsADirectoryError("Cannot copy root directory as a file")
        async with self._client_lease() as lease:
            source_info = await self._stat(lease.client, source)
            if source_info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {source.as_posix()}")
            if source_info.kind is not EntryKind.FILE:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copy source kind: {source.as_posix()}"
                )
            if source == destination:
                if overwrite:
                    return
                raise FileExistsError(f"Source and destination are the same: {source.as_posix()}")
            try:
                destination_info = await self._stat(lease.client, destination)
            except FileNotFoundError:
                destination_existed = False
            else:
                destination_existed = True
                if destination_info.kind is EntryKind.DIRECTORY:
                    raise IsADirectoryError(f"Destination is a directory: {destination.as_posix()}")
                if destination_info.kind is not EntryKind.FILE:
                    raise UnsupportedOperationError(
                        _UNSUPPORTED_ERRNO, f"Unsupported FTP copy destination kind: {destination.as_posix()}"
                    )
            try:
                async with self._temporary_client() as destination_client:
                    await self._copy_file(lease.client, destination_client, source, destination, overwrite=overwrite)
            except BaseException:
                if not destination_existed:
                    await self._cleanup_partial_file(destination)
                raise

    @staticmethod
    def _is_descendant(path: PurePosixPath, parent: PurePosixPath) -> bool:
        if path == parent:
            return False
        try:
            path.relative_to(parent)
        except ValueError:
            return False
        return True

    async def _rollback_copytree(
        self,
        client: aioftp.Client,
        created_files: set[PurePosixPath],
        created_dirs: set[PurePosixPath],
        backups: dict[PurePosixPath, PurePosixPath],
    ) -> None:
        failures: list[BaseException] = []
        for file in sorted(created_files, key=lambda path: len(path.parts), reverse=True):
            try:
                if await self._exists(client, file):
                    await client.remove_file(self._remote_path(file))
            except BaseException as error:
                failures.append(error)
        for target, backup in backups.items():
            try:
                if await self._exists(client, target):
                    await client.remove_file(self._remote_path(target))
                if await self._exists(client, backup):
                    await client.rename(self._remote_path(backup), self._remote_path(target))
            except BaseException as error:
                failures.append(error)
        for directory in sorted(created_dirs, key=lambda path: len(path.parts), reverse=True):
            try:
                if await self._exists(client, directory):
                    await client.remove_directory(self._remote_path(directory))
            except BaseException as error:
                failures.append(error)
        if not failures:
            return
        if len(failures) == 1:
            raise failures[0]
        raise BaseExceptionGroup("Failed to roll back FTP copytree", failures)

    async def _rollback_copytree_with_fallback(
        self,
        source_client: aioftp.Client,
        created_files: set[PurePosixPath],
        created_dirs: set[PurePosixPath],
        backups: dict[PurePosixPath, PurePosixPath],
    ) -> None:
        try:
            await self._rollback_copytree(source_client, created_files, created_dirs, backups)
        except BaseException as first_error:
            try:
                async with self._temporary_client() as cleanup_client:
                    await self._rollback_copytree(cleanup_client, created_files, created_dirs, backups)
            except BaseException as second_error:
                if isinstance(first_error, Exception) and isinstance(second_error, Exception):
                    raise BaseExceptionGroup("FTP copytree rollback failed", [first_error, second_error]) from None
                raise first_error from None

    async def _preflight_copytree_destination(
        self,
        client: aioftp.Client,
        snapshot: list[WalkEntry],
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> None:
        for walk_entry in snapshot:
            current = PurePosixPath(walk_entry.path)
            relative = current.relative_to(source)
            target_dir = destination if relative == PurePosixPath(".") else destination / relative
            for entry in walk_entry.entries:
                target = target_dir / entry.name
                try:
                    target_info = await self._stat(client, target)
                except FileNotFoundError:
                    continue
                if entry.kind is EntryKind.DIRECTORY:
                    if target_info.kind is not EntryKind.DIRECTORY:
                        raise FileExistsError(f"Destination is not a directory: {target.as_posix()}")
                    continue
                if entry.kind is EntryKind.FILE:
                    if target_info.kind is EntryKind.DIRECTORY:
                        raise IsADirectoryError(f"Destination is a directory: {target.as_posix()}")
                    if target_info.kind is not EntryKind.FILE:
                        raise UnsupportedOperationError(
                            _UNSUPPORTED_ERRNO,
                            f"Unsupported FTP copytree destination kind: {target.as_posix()}",
                        )
                    if not overwrite:
                        raise FileExistsError(f"Destination already exists: {target.as_posix()}")
                    continue
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copytree source kind: {entry.path}"
                )

    async def _copytree(
        self,
        source_client: aioftp.Client,
        destination_client: aioftp.Client,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> None:
        try:
            source_info = await self._stat(source_client, source)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {source.as_posix()}") from None
        if source_info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
        if source == destination:
            raise FileExistsError(f"Source and destination are the same: {source.as_posix()}")
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")

        snapshot = await self._walk_snapshot(source_client, source, strict=True)
        try:
            destination_info = await self._stat(source_client, destination)
        except FileNotFoundError:
            destination_info = None
        if destination_info is not None:
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if destination_info.kind is EntryKind.FILE:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")
            if destination_info.kind is not EntryKind.DIRECTORY:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copytree destination kind: {destination.as_posix()}"
                )
        await self._preflight_copytree_destination(
            source_client,
            snapshot,
            source,
            destination,
            overwrite=overwrite,
        )

        created_files: set[PurePosixPath] = set()
        created_dirs: set[PurePosixPath] = set()
        backups: dict[PurePosixPath, PurePosixPath] = {}
        try:
            created_dirs.update(await self._mkdir(source_client, destination, parents=True, exist_ok=True))
            for walk_entry in snapshot:
                current = PurePosixPath(walk_entry.path)
                relative = current.relative_to(source)
                target_dir = destination if relative == PurePosixPath(".") else destination / relative
                for entry in walk_entry.entries:
                    target = target_dir / entry.name
                    if entry.kind is EntryKind.DIRECTORY:
                        created_dirs.update(await self._mkdir(source_client, target, parents=False, exist_ok=True))
                        continue
                    if entry.kind is not EntryKind.FILE:
                        raise UnsupportedOperationError(
                            _UNSUPPORTED_ERRNO, f"Unsupported FTP copytree source kind: {entry.path}"
                        )
                    if await self._exists(source_client, target):
                        backup = target.with_name(f".storegate-copytree-{uuid.uuid4().hex}-{target.name}")
                        await source_client.rename(self._remote_path(target), self._remote_path(backup))
                        backups[target] = backup
                    else:
                        created_files.add(target)
                    await self._copy_file(source_client, destination_client, PurePosixPath(entry.path), target)
        except BaseException as primary:
            rollback_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await self._rollback_copytree_with_fallback(source_client, created_files, created_dirs, backups)
                except BaseException as error:
                    rollback_error = error
            if rollback_error is not None and isinstance(primary, Exception) and isinstance(rollback_error, Exception):
                raise BaseExceptionGroup("FTP copytree and rollback failed", [primary, rollback_error]) from None
            if rollback_error is not None and isinstance(rollback_error, anyio.get_cancelled_exc_class()):
                raise rollback_error from None
            raise
        cleanup_errors: list[BaseException] = []
        with anyio.CancelScope(shield=True):
            for backup in backups.values():
                try:
                    await source_client.remove_file(self._remote_path(backup))
                except BaseException as error:
                    cleanup_errors.append(error)
        if cleanup_errors:
            if len(cleanup_errors) == 1:
                raise cleanup_errors[0]
            raise BaseExceptionGroup("Failed to clean FTP copytree backups", cleanup_errors)

    @override
    @translator.wrap("Failed to copy tree {src} → {dst} (overwrite={overwrite})")
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"CopyTree: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        async with self._client_lease() as lease, self._temporary_client() as destination_client:
            await self._copytree(lease.client, destination_client, source, destination, overwrite=overwrite)

    @override
    @translator.wrap("Failed to move tree {src} → {dst} (overwrite={overwrite})")
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"MoveTree: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        if source == PurePosixPath("/"):
            raise OSError("Cannot move root directory")
        if source == destination:
            return
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")

        async with self._client_lease() as lease:
            try:
                source_info = await self._stat(lease.client, source)
            except FileNotFoundError:
                raise NotADirectoryError(f"Not a directory: {source.as_posix()}") from None
            if source_info.kind is not EntryKind.DIRECTORY:
                raise NotADirectoryError(f"Not a directory: {source.as_posix()}")

            try:
                destination_info = await self._stat(lease.client, destination)
            except FileNotFoundError:
                destination_info = None

            if destination_info is None:
                await self._walk_snapshot(lease.client, source, strict=True)
                await self._mkdir(lease.client, destination.parent, parents=True, exist_ok=True)
                await lease.client.rename(self._remote_path(source), self._remote_path(destination))
                return
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if destination_info.kind is EntryKind.FILE:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")
            if destination_info.kind is not EntryKind.DIRECTORY:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP movetree destination kind: {destination.as_posix()}"
                )

            async with self._temporary_client() as destination_client:
                await self._copytree(lease.client, destination_client, source, destination, overwrite=True)
            await self._rmtree(lease.client, source)
