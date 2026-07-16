import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import final, override

import aioftp
import anyio
from aioftp.client import BasicListInfo, UnixListInfo

from app.log import escape_tag
from app.storage.abstract import AbstractStorage, BytesLike, FileInfo, PathLike, make_cache_identity
from app.utils import ExceptionTranslator, coalesce_chunks, flatten_exception_group

from .config import FTPConfig

type FTPFacts = BasicListInfo | UnixListInfo | dict[str, str]

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


def _parse_mlsx_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    format_string = "%Y%m%d%H%M%S.%f" if "." in value else "%Y%m%d%H%M%S"
    try:
        return datetime.strptime(value, format_string).replace(tzinfo=UTC)
    except ValueError:
        return None


def _status_matches(exc: aioftp.StatusCodeError, pattern: str) -> bool:
    return exc.received_codes[-1].matches(pattern)


@final
class FTPStorage(AbstractStorage):
    """Plain FTP client storage backend."""

    _client: aioftp.Client | None = None

    def __init__(self, config: str | Path | FTPConfig) -> None:
        super().__init__()
        self._config = config if isinstance(config, FTPConfig) else FTPConfig.from_file(config)
        self._lock = anyio.Lock()
        self._root_prefix = PurePosixPath(self._config.root_prefix)

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

    def _ensure_client(self) -> aioftp.Client:
        if self._client is None:
            raise RuntimeError("FTP client is not connected")
        return self._client

    def _new_client(self) -> aioftp.Client:
        return aioftp.Client(
            connection_timeout=self._config.timeout,
            encoding=self._config.encoding,
            path_timeout=self._config.timeout,
            socket_timeout=self._config.timeout,
        )

    async def _connect_client(self, *, validate_root: bool) -> aioftp.Client:
        client = self._new_client()
        try:
            await client.connect(self._config.host, self._config.port)
            await client.login(
                self._config.username,
                self._config.password.get_secret_value(),
            )
            if validate_root:
                facts = await client.stat(self._root_prefix)
                if facts.get("type") != "dir":
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
    async def _aux_client(self) -> AsyncIterator[aioftp.Client]:
        client = await self._connect_client(validate_root=False)
        try:
            yield client
        finally:
            with anyio.CancelScope(shield=True):
                await self._close_client(client)

    @override
    @translator.wrap("Failed to connect to FTP server")
    async def connect(self) -> None:
        if self._client is not None:
            return
        client = await self._connect_client(validate_root=True)
        self._client = client
        self.log.info(f"Connected to <c>{escape_tag(self._config.host)}</c>:<c>{self._config.port}</c>")

    @override
    async def close(self) -> None:
        async with self._lock:
            client = self._client
            self._client = None
            if client is not None:
                await self._close_client(client)
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            async with self._lock:
                facts = await self._ensure_client().stat(self._root_prefix)
                return facts.get("type") == "dir"
        except Exception:
            return False

    def _file_info_from_facts(self, logical_path: PurePosixPath, facts: FTPFacts) -> FileInfo:
        entry_type = facts.get("type")
        if entry_type not in {"dir", "file"}:
            raise OSError(f"Unsupported FTP entry type {entry_type!r}: {logical_path.as_posix()}")

        try:
            size = int(facts.get("size", "0"))
        except ValueError:
            size = 0

        return FileInfo(
            path=logical_path.as_posix(),
            name="" if logical_path == PurePosixPath("/") else logical_path.name,
            is_dir=entry_type == "dir",
            size=0 if entry_type == "dir" else max(0, size),
            modified=_parse_mlsx_datetime(facts.get("modify")),
            created=_parse_mlsx_datetime(facts.get("create")),
        )

    async def _stat_with_facts_unlocked(self, path: PathLike) -> tuple[FileInfo, FTPFacts]:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            return FileInfo(path="/", name="", is_dir=True), {"type": "dir"}

        try:
            facts = await self._ensure_client().stat(self._remote_path(logical))
        except aioftp.StatusCodeError as exc:
            if _status_matches(exc, "550"):
                raise FileNotFoundError(f"FTP path not found: {logical.as_posix()}") from exc
            raise
        return self._file_info_from_facts(logical, facts), facts

    async def _stat_unlocked(self, path: PathLike) -> FileInfo:
        info, _ = await self._stat_with_facts_unlocked(path)
        return info

    async def _exists_unlocked(self, path: PathLike) -> bool:
        try:
            await self._stat_unlocked(path)
        except FileNotFoundError:
            return False
        return True

    @override
    @translator.wrap("Failed to stat {path}")
    async def stat(self, path: PathLike) -> FileInfo:
        async with self._lock:
            return await self._stat_unlocked(path)

    @override
    @translator.wrap("Failed to check existence of {path}")
    async def exists(self, path: PathLike) -> bool:
        async with self._lock:
            return await self._exists_unlocked(path)

    @override
    @translator.wrap("Failed to check whether {path} is a file")
    async def is_file(self, path: PathLike) -> bool:
        async with self._lock:
            try:
                info = await self._stat_unlocked(path)
            except FileNotFoundError:
                return False
            return not info.is_dir

    @override
    @translator.wrap("Failed to check whether {path} is a directory")
    async def is_dir(self, path: PathLike) -> bool:
        async with self._lock:
            try:
                info = await self._stat_unlocked(path)
            except FileNotFoundError:
                return False
            return info.is_dir

    async def _list_unlocked(self, path: PathLike, *, validate: bool = True) -> list[FileInfo]:
        logical = self._logical_path(path)
        if validate:
            info = await self._stat_unlocked(logical)
            if not info.is_dir:
                raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")

        try:
            entries = await self._ensure_client().list(self._remote_path(logical))
        except aioftp.StatusCodeError as exc:
            if _status_matches(exc, "550"):
                raise FileNotFoundError(f"FTP directory not found: {logical.as_posix()}") from exc
            raise

        result = [
            self._file_info_from_facts(self._logical_from_remote(remote_path), facts) for remote_path, facts in entries
        ]
        result.sort(key=lambda entry: entry.path)
        return result

    @override
    @translator.wrap_agen("Failed to iterate directory {path}")
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        async with self._lock:
            entries = await self._list_unlocked(path)
        for entry in entries:
            yield entry

    async def _walk_snapshot_unlocked(
        self,
        path: PathLike,
    ) -> list[tuple[PurePosixPath, list[FileInfo], list[FileInfo]]]:
        root = self._logical_path(path)
        info = await self._stat_unlocked(root)
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {root.as_posix()}")

        result: list[tuple[PurePosixPath, list[FileInfo], list[FileInfo]]] = []
        pending = [root]
        while pending:
            current = pending.pop()
            entries = await self._list_unlocked(current, validate=False)
            dirs = [entry for entry in entries if entry.is_dir]
            files = [entry for entry in entries if not entry.is_dir]
            result.append((current, dirs, files))
            pending.extend(PurePosixPath(entry.path) for entry in reversed(dirs))
        return result

    @override
    @translator.wrap_agen("Failed to walk directory {path}")
    async def walk(self, path: PathLike) -> AsyncGenerator[tuple[str, list[FileInfo], list[FileInfo]]]:
        async with self._lock:
            snapshot = await self._walk_snapshot_unlocked(path)
        for current, dirs, files in snapshot:
            yield current.as_posix(), dirs, files

    async def _mkdir_unlocked(
        self,
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
            existing = await self._stat_unlocked(logical)
        except FileNotFoundError:
            pass
        else:
            if not existing.is_dir:
                raise FileExistsError(f"Path is a file: {logical.as_posix()}")
            if exist_ok:
                return []
            raise FileExistsError(f"Directory already exists: {logical.as_posix()}")

        if not parents:
            parent_info = await self._stat_unlocked(logical.parent)
            if not parent_info.is_dir:
                raise NotADirectoryError(f"Parent is not a directory: {logical.parent.as_posix()}")
            await self._ensure_client().make_directory(self._remote_path(logical), parents=False)
            return [logical]

        ancestors = [parent for parent in reversed(logical.parents) if parent != PurePosixPath("/")]
        ancestors.append(logical)
        created: list[PurePosixPath] = []
        for directory in ancestors:
            try:
                info = await self._stat_unlocked(directory)
            except FileNotFoundError:
                await self._ensure_client().make_directory(self._remote_path(directory), parents=False)
                created.append(directory)
            else:
                if not info.is_dir:
                    raise FileExistsError(f"Path is a file: {directory.as_posix()}")
        return created

    @override
    @translator.wrap("Failed to create directory {path} (parents={parents}, exist_ok={exist_ok})")
    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        async with self._lock:
            await self._mkdir_unlocked(path, parents=parents, exist_ok=exist_ok)

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
        async with self._lock:
            existed = False
            try:
                info = await self._stat_unlocked(logical)
            except FileNotFoundError:
                pass
            else:
                existed = True
                if info.is_dir:
                    raise IsADirectoryError(f"Is a directory: {logical.as_posix()}")
                if not overwrite:
                    raise FileExistsError(f"File already exists: {logical.as_posix()}")

            await self._mkdir_unlocked(logical.parent, parents=True, exist_ok=True)
            try:
                async with self._ensure_client().upload_stream(self._remote_path(logical)) as writer:
                    async for chunk in coalesce_chunks(stream, self._config.chunk_size):
                        await writer.write(chunk)
            except Exception:
                if not existed:
                    with contextlib.suppress(Exception):
                        await self._ensure_client().remove_file(self._remote_path(logical))
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
            async with self._lock:
                info, facts = await self._stat_with_facts_unlocked(logical)
                if info.is_dir:
                    raise IsADirectoryError(f"Is a directory: {logical.as_posix()}")
                if "size" in facts and offset >= info.size:
                    return

                reader = await self._ensure_client().download_stream(self._remote_path(logical), offset=offset)
                try:
                    async for chunk in reader.iter_by_block(self._config.chunk_size):
                        yield chunk
                finally:
                    with anyio.CancelScope(shield=True):
                        await reader.finish()
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

        async with self._lock:
            try:
                info = await self._stat_unlocked(logical)
            except FileNotFoundError:
                if missing_ok:
                    return
                raise
            if info.is_dir:
                raise IsADirectoryError(f"Is a directory: {logical.as_posix()}")
            await self._ensure_client().remove_file(self._remote_path(logical))

    @override
    @translator.wrap("Failed to remove directory {path}")
    async def rmdir(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            raise OSError("Cannot remove root directory")

        async with self._lock:
            info = await self._stat_unlocked(logical)
            if not info.is_dir:
                raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")
            if await self._list_unlocked(logical, validate=False):
                raise OSError(f"Directory not empty: {logical.as_posix()}")
            await self._ensure_client().remove_directory(self._remote_path(logical))

    async def _rmtree_unlocked(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        if logical == PurePosixPath("/"):
            raise OSError("Cannot remove root directory")

        info = await self._stat_unlocked(logical)
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {logical.as_posix()}")

        snapshot = await self._walk_snapshot_unlocked(logical)
        client = self._ensure_client()
        for _, _, files in snapshot:
            for file in files:
                await client.remove_file(self._remote_path(file.path))
        for directory, _, _ in sorted(snapshot, key=lambda item: len(item[0].parts), reverse=True):
            await client.remove_directory(self._remote_path(directory))

    @override
    @translator.wrap("Failed to remove directory tree {path}")
    async def rmtree(self, path: PathLike) -> None:
        async with self._lock:
            await self._rmtree_unlocked(path)

    async def _copy_stream_unlocked(
        self,
        source: PurePosixPath,
        destination: PurePosixPath,
        destination_client: aioftp.Client,
    ) -> None:
        async with (
            self._ensure_client().download_stream(self._remote_path(source)) as reader,
            destination_client.upload_stream(self._remote_path(destination)) as writer,
        ):
            async for chunk in reader.iter_by_block(self._config.chunk_size):
                await writer.write(chunk)

    async def _copy_file_unlocked(
        self,
        source: PurePosixPath,
        destination: PurePosixPath,
        destination_client: aioftp.Client,
    ) -> bool:
        source_info = await self._stat_unlocked(source)
        if source_info.is_dir:
            raise IsADirectoryError(f"Is a directory: {source.as_posix()}")

        destination_existed = False
        try:
            destination_info = await self._stat_unlocked(destination)
        except FileNotFoundError:
            pass
        else:
            destination_existed = True
            if destination_info.is_dir:
                raise IsADirectoryError(f"Is a directory: {destination.as_posix()}")

        await self._mkdir_unlocked(destination.parent, parents=True, exist_ok=True)
        await self._copy_stream_unlocked(source, destination, destination_client)
        return not destination_existed

    @override
    @translator.wrap("Failed to move {src} → {dst}")
    async def move(self, src: PathLike, dst: PathLike) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        if source == PurePosixPath("/"):
            raise OSError("Cannot move root directory")

        async with self._lock:
            source_info = await self._stat_unlocked(source)
            if source_info.is_dir:
                raise IsADirectoryError(f"Is a directory: {source.as_posix()}")
            if await self._exists_unlocked(destination):
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            await self._mkdir_unlocked(destination.parent, parents=True, exist_ok=True)
            await self._ensure_client().rename(self._remote_path(source), self._remote_path(destination))

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(self, src: PathLike, dst: PathLike) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        if source == PurePosixPath("/"):
            raise IsADirectoryError("Cannot copy root directory as a file")
        if source == destination:
            raise FileExistsError(f"Source and destination are the same: {source.as_posix()}")

        async with self._lock:
            destination_existed = await self._exists_unlocked(destination)
            try:
                async with self._aux_client() as destination_client:
                    await self._copy_file_unlocked(source, destination, destination_client)
            except Exception:
                if not destination_existed:
                    with contextlib.suppress(Exception):
                        await self._ensure_client().remove_file(self._remote_path(destination))
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

    async def _rollback_copytree_unlocked(
        self,
        created_files: set[PurePosixPath],
        created_dirs: set[PurePosixPath],
    ) -> None:
        client = self._ensure_client()
        for file in sorted(created_files, key=lambda path: len(path.parts), reverse=True):
            with contextlib.suppress(Exception):
                await client.remove_file(self._remote_path(file))
        for directory in sorted(created_dirs, key=lambda path: len(path.parts), reverse=True):
            with contextlib.suppress(Exception):
                await client.remove_directory(self._remote_path(directory))

    async def _copytree_unlocked(
        self,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> None:
        try:
            source_info = await self._stat_unlocked(source)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {source.as_posix()}") from None
        if not source_info.is_dir:
            raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
        if source == destination:
            raise FileExistsError(f"Source and destination are the same: {source.as_posix()}")
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")

        snapshot = await self._walk_snapshot_unlocked(source)
        try:
            destination_info = await self._stat_unlocked(destination)
        except FileNotFoundError:
            destination_info = None
        if destination_info is not None:
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if not destination_info.is_dir:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")

        created_files: set[PurePosixPath] = set()
        created_dirs: set[PurePosixPath] = set()
        try:
            async with self._aux_client() as destination_client:
                created_dirs.update(await self._mkdir_unlocked(destination, parents=True, exist_ok=True))
                for current, dirs, files in snapshot:
                    relative = current.relative_to(source)
                    target_dir = destination if relative == PurePosixPath(".") else destination / relative
                    for directory in dirs:
                        target = target_dir / directory.name
                        created_dirs.update(await self._mkdir_unlocked(target, parents=False, exist_ok=True))
                    for file in files:
                        target = target_dir / file.name
                        existed = await self._exists_unlocked(target)
                        if not existed:
                            created_files.add(target)
                        await self._copy_file_unlocked(
                            PurePosixPath(file.path),
                            target,
                            destination_client,
                        )
        except Exception:
            with anyio.CancelScope(shield=True):
                await self._rollback_copytree_unlocked(created_files, created_dirs)
            raise

    @override
    @translator.wrap("Failed to copy tree {src} → {dst} (overwrite={overwrite})")
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        async with self._lock:
            await self._copytree_unlocked(source, destination, overwrite=overwrite)

    @override
    @translator.wrap("Failed to move tree {src} → {dst} (overwrite={overwrite})")
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        if source == PurePosixPath("/"):
            raise OSError("Cannot move root directory")
        if source == destination:
            return
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")

        async with self._lock:
            try:
                source_info = await self._stat_unlocked(source)
            except FileNotFoundError:
                raise NotADirectoryError(f"Not a directory: {source.as_posix()}") from None
            if not source_info.is_dir:
                raise NotADirectoryError(f"Not a directory: {source.as_posix()}")

            try:
                destination_info = await self._stat_unlocked(destination)
            except FileNotFoundError:
                destination_info = None

            if destination_info is None:
                await self._mkdir_unlocked(destination.parent, parents=True, exist_ok=True)
                await self._ensure_client().rename(self._remote_path(source), self._remote_path(destination))
                return
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if not destination_info.is_dir:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")

            await self._copytree_unlocked(source, destination, overwrite=True)
            await self._rmtree_unlocked(source)
