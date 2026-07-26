import contextlib
import errno
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import override

import aioftp
import anyio
from aioftp.client import BasicListInfo, UnixListInfo

from storegate.log import escape_tag
from storegate.storage.abstract import (
    AbstractStorage,
    EntryKind,
    FileInfo,
    PathLike,
    UnsupportedOperationError,
    WalkEntry,
    make_namespace_identity,
)
from storegate.utils import ExceptionTranslator

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
def _(exc: aioftp.StatusCodeError, msg: str) -> OSError:
    code = exc.received_codes[-1]
    if code.matches("530"):
        return PermissionError(f"{msg}: {exc}")
    return OSError(f"{msg}: {exc}")


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


class FTPStorageBase(AbstractStorage):
    """State, lifecycle and remote-path primitives shared by the FTPStorage mixins.

    Splitting the operation mixins out of ``FTPStorage`` keeps each transaction
    in its own module; they cooperate only through the members defined here, so
    this class is the whole contract between them.
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
    def display_id(self) -> str:
        return f"ftp:{self._config.username}@{self._config.host}:{self._config.port}{self._config.root_prefix}"

    @property
    @override
    def namespace_identity(self) -> str:
        return make_namespace_identity(
            "ftp",
            encoding=self._config.encoding,
            host=self._config.host,
            port=self._config.port,
            root_prefix=self._config.root_prefix,
            username=self._config.username,
        )

    def _logical_path(self, path: PathLike) -> PurePosixPath:
        return self.normalize_path(path)

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

    async def _cleanup_partial_file(self, logical: PurePosixPath) -> None:
        with anyio.move_on_after(self._config.timeout, shield=True):
            with contextlib.suppress(Exception):
                async with self._temporary_client() as client:
                    await client.remove_file(self._remote_path(logical))

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

    @staticmethod
    def _is_descendant(path: PurePosixPath, parent: PurePosixPath) -> bool:
        if path == parent:
            return False
        try:
            path.relative_to(parent)
        except ValueError:
            return False
        return True
