import errno
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from pathlib import Path, PurePosixPath
from typing import final, override
from urllib.parse import urlparse

import anyio

from app.log import escape_tag
from app.storage.abstract import (
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
from app.utils import ExceptionTranslator, coalesce_chunks, flatten_exception_group

from .client import AsyncDavClient, DavClientError, DavConfig, DavHttpStatusError, DavResource
from .utils import dav_resource_to_file_info, href_to_storage_path

translator = ExceptionTranslator(
    bypass=OSError,
    catch=DavClientError,
    default=OSError,
)

_DAV_CAPABILITIES = StorageCapabilities()
_UNSUPPORTED_ERRNO = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)


def _unsupported_entry(path: PathLike) -> UnsupportedOperationError:
    return UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"Unsupported WebDAV resource: {path}")


@translator.handles(DavHttpStatusError)
def _(exc_group: ExceptionGroup[DavHttpStatusError], msg: str) -> OSError:
    first = next(flatten_exception_group(exc_group))
    return {
        404: FileNotFoundError,
        403: PermissionError,
        412: FileExistsError,  # Overwrite: F precondition failed
        423: PermissionError,  # Locked
    }.get(first.status_code, OSError)(f"{msg}: {first}")


@final
class DavStorage(AbstractStorage):
    """WebDAV client storage backend.

    Core WebDAV exposes ordinary files and collections but has no standard
    symlink inspection or creation primitives. Non-collection extension
    resource types are treated as unsupported rather than regular files.
    Servers that follow links and report only the resolved target remain
    outside client-side detection; confinement then depends on the server.
    """

    _client: AsyncDavClient | None = None
    _config: DavConfig

    def __init__(self, config: str | Path | DavConfig) -> None:
        super().__init__()
        self._config = config if isinstance(config, DavConfig) else DavConfig.from_file(config)

    @property
    @override
    def id(self) -> str:
        parsed = urlparse(self._config.base_url)
        return f"dav:{parsed.netloc}{self._config.root_prefix}"

    @property
    @override
    def cache_identity(self) -> str:
        parsed = urlparse(self._config.base_url)
        return make_cache_identity(
            "dav",
            base_path=parsed.path,
            hostname=parsed.hostname,
            port=parsed.port,
            root_prefix=self._config.root_prefix,
            scheme=parsed.scheme,
        )

    @property
    @override
    def capabilities(self) -> StorageCapabilities:
        return _DAV_CAPABILITIES

    @property
    def _url_prefix(self) -> str:
        """Server path prefix shared by every href (base_url path + root_prefix)."""
        parsed = urlparse(self._config.base_url)
        return f"{parsed.path}{self._config.root_prefix}"

    def _remote_path(self, path: PathLike) -> str:
        np = self.normalize_path(path)
        rel = np.relative_to("/")
        return rel.as_posix() if rel != PurePosixPath(".") else ""

    def _ensure_client(self) -> AsyncDavClient:
        if self._client is None:
            raise RuntimeError("Client is not connected.")
        return self._client

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        if self._client is not None:
            retained = self._client
            with anyio.CancelScope(shield=True):
                await retained.__aexit__(None, None, None)
                if self._client is retained:
                    self._client = None
        client = AsyncDavClient(self._config)
        self._client = client
        try:
            await client.__aenter__()
            if not await self.ping():
                raise RuntimeError("Failed to connect to WebDAV server. Please check your configuration.")
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await client.__aexit__(None, None, None)
                except BaseException as secondary:
                    cleanup_error = secondary
                else:
                    self._client = None
            if cleanup_error is not None:
                self._client = client
                raise BaseExceptionGroup("WebDAV connection rollback failed", [primary, cleanup_error]) from None
            raise
        self.log.info(f"Connected to <c>{escape_tag(self._config.base_url)}</c>")

    @override
    async def close(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.propfind("", depth=0)
        except Exception:
            return False
        else:
            return True

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @override
    @translator.wrap("Failed to stat {path}")
    async def stat(self, path: PathLike) -> FileInfo:
        np = self.normalize_path(path)
        rel = self._remote_path(path)
        if rel == "":
            return FileInfo(path=np.as_posix(), name="", kind=EntryKind.DIRECTORY, size=0)

        client = self._ensure_client()
        resources = await client.propfind(rel, depth=0)
        if not resources:
            raise FileNotFoundError(f"Object not found: {path}")
        resource = resources[0]
        storage_path = href_to_storage_path(resource.href, self._url_prefix)
        return dav_resource_to_file_info(resource, storage_path)

    @override
    @translator.wrap("Failed to check existence of {path}")
    async def exists(self, path: PathLike) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    @override
    @translator.wrap("Failed to check if path is a file: {path}")
    async def is_file(self, path: PathLike) -> bool:
        try:
            info = await self.stat(path)
        except FileNotFoundError:
            return False
        return info.kind is EntryKind.FILE

    @override
    @translator.wrap("Failed to check if path is a directory: {path}")
    async def is_dir(self, path: PathLike) -> bool:
        if self._remote_path(path) == "":
            return True
        try:
            info = await self.stat(path)
        except FileNotFoundError:
            return False
        return info.is_dir

    # ------------------------------------------------------------------
    # Directory
    # ------------------------------------------------------------------

    @override
    @translator.wrap("Failed to create directory {path} (parents={parents}, exist_ok={exist_ok})")
    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        np = self.normalize_path(path)
        rel = self._remote_path(path)
        if rel == "":
            if exist_ok:
                return
            raise FileExistsError("Root directory already exists")

        client = self._ensure_client()

        # Conflict check: existing file or directory.
        try:
            info = await self.stat(path)
        except FileNotFoundError:
            pass
        else:
            if info.kind is not EntryKind.DIRECTORY:
                raise FileExistsError(f"Path is a file: {path}")
            if exist_ok:
                return
            raise FileExistsError(f"Directory already exists: {path}")

        # Parent directory.
        parent = np.parent
        if self._remote_path(parent) != "":
            if parents:
                await self.mkdir(parent.as_posix(), parents=True, exist_ok=True)
            elif not await self.is_dir(parent.as_posix()):
                raise FileNotFoundError(f"Parent directory not found: {parent.as_posix()}")

        try:
            await client.mkcol(rel)
        except DavHttpStatusError as exc:
            if exc.status_code == 405:
                raise FileExistsError(f"Directory already exists: {path}") from exc
            if exc.status_code == 409:
                raise FileNotFoundError(f"Parent directory not found: {path}") from exc
            raise
        self.log.info(f"MkDir: <y>{escape_tag(rel)}</y>")

    async def _raw_directory_entries(self, path: PathLike) -> tuple[tuple[DavResource, str], ...]:
        rel = self._remote_path(path)
        resources = await self._ensure_client().propfind(rel, depth=1)
        entries: list[tuple[DavResource, str]] = []
        for resource in resources:
            storage_path = href_to_storage_path(resource.href, self._url_prefix)
            if storage_path == rel or storage_path == "":
                continue
            entries.append((resource, storage_path))
        return tuple(entries)

    async def _scan_directory(self, path: PathLike, *, strict: bool) -> tuple[FileInfo, ...]:
        entries: list[FileInfo] = []
        for resource, storage_path in await self._raw_directory_entries(path):
            try:
                entry = dav_resource_to_file_info(resource, storage_path)
            except UnsupportedOperationError:
                if strict:
                    raise
                self.log.debug(
                    f"Skipping unsupported WebDAV resource <y>{escape_tag(storage_path)}</y>: "
                    f"{resource.resource_types!r}"
                )
                continue
            entries.append(entry)
        return tuple(sorted(entries, key=lambda entry: entry.path))

    async def _strict_walk_snapshot(self, path: PathLike, *, root: FileInfo | None = None) -> tuple[WalkEntry, ...]:
        if root is None:
            root = await self.stat(path)
        if root.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")

        pending = [self.normalize_path(path)]
        snapshot: list[WalkEntry] = []
        while pending:
            current = pending.pop()
            entries = await self._scan_directory(current, strict=True)
            snapshot.append(WalkEntry(path=current.as_posix(), entries=entries))
            directories = [entry for entry in entries if entry.kind is EntryKind.DIRECTORY]
            pending.extend(PurePosixPath(entry.path) for entry in reversed(directories))
        return tuple(snapshot)

    @override
    @translator.wrap_agen("Failed to iterate directory {path}")
    async def iterdir(self, path: PathLike) -> AsyncIterator[FileInfo]:
        for entry in await self._scan_directory(path, strict=False):
            yield entry

    @override
    @translator.wrap_agen("Failed to walk directory {path}")
    async def walk(self, path: PathLike) -> AsyncIterator[WalkEntry]:
        root = await self.stat(path)
        if root.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")

        normalized = self.normalize_path(path)
        entries = await self._scan_directory(normalized, strict=False)
        yield WalkEntry(path=normalized.as_posix(), entries=entries)
        for entry in entries:
            if entry.kind is EntryKind.DIRECTORY:
                async for child in self.walk(entry.path):
                    yield child

    @override
    async def _is_dir_empty(self, path: PathLike) -> bool:
        return not await self._raw_directory_entries(path)

    # ------------------------------------------------------------------
    # Upload / Download
    # ------------------------------------------------------------------

    @override
    @translator.wrap("Failed to upload stream to {remote_path} (overwrite={overwrite})")
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        remote_path = self.normalize_path(remote_path)
        try:
            info = await self.stat(remote_path)
        except FileNotFoundError:
            pass
        else:
            if info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            if info.kind is not EntryKind.FILE:
                raise _unsupported_entry(remote_path)
            if not overwrite:
                raise FileExistsError(f"File already exists: {remote_path}")

        client = self._ensure_client()
        rel = self._remote_path(remote_path)
        self.log.info(f"Upload: <y>{escape_tag(rel)}</y>")
        await self.mkdir(remote_path.parent.as_posix(), parents=True, exist_ok=True)

        chunk_iter = aiter(coalesce_chunks(stream, self._config.chunk_size))
        first_chunk = await anext(chunk_iter, None)
        if first_chunk is None:
            await client.put(rel, b"")
            return

        async def _chained() -> AsyncGenerator[bytes]:
            yield first_chunk
            async for chunk in chunk_iter:
                yield chunk

        await client.put(rel, _chained())

    @override
    @translator.wrap_agen("Failed to download stream from {remote_path} (offset={offset})")
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        info = await self.stat(remote_path)
        if info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {remote_path}")
        if info.kind is not EntryKind.FILE:
            raise _unsupported_entry(remote_path)
        if info.size > 0 and offset >= info.size:
            return

        client = self._ensure_client()
        rel = self._remote_path(remote_path)
        async with client.stream_get(rel, range_start=offset or None) as response:
            async for chunk in response.aiter_bytes():
                yield chunk

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    @override
    @translator.wrap("Failed to unlink {path} (missing_ok={missing_ok})")
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        rel = self._remote_path(path)
        if rel == "":
            raise IsADirectoryError(f"Is a directory: {path}")

        try:
            info = await self.stat(path)
        except FileNotFoundError:
            if not missing_ok:
                raise
            return

        if info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {path}")
        if info.kind is not EntryKind.FILE:
            raise _unsupported_entry(path)

        self.log.info(f"Delete: <y>{escape_tag(rel)}</y>")
        await self._ensure_client().delete(rel)

    @override
    @translator.wrap("Failed to remove directory {path}")
    async def rmdir(self, path: PathLike) -> None:
        rel = self._remote_path(path)
        if rel == "":
            raise OSError(f"Cannot remove root: {path}")

        try:
            info = await self.stat(path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Directory not found: {path}") from None
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        if not await self._is_dir_empty(path):
            raise OSError(f"Directory not empty: {path}")

        self.log.info(f"Delete dir: <y>{escape_tag(rel)}</y>")
        # WebDAV DELETE is recursive, but we pre-checked emptiness above.
        await self._ensure_client().delete(rel)

    @override
    @translator.wrap("Failed to remove directory tree {path}")
    async def rmtree(self, path: PathLike) -> None:
        rel = self._remote_path(path)
        if rel == "":
            raise OSError(f"Cannot remove root: {path}")

        try:
            info = await self.stat(path)
        except FileNotFoundError:
            return

        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")

        await self._strict_walk_snapshot(path, root=info)

        self.log.info(f"RmTree: <y>{escape_tag(rel)}</y>")
        # WebDAV DELETE on a collection is naturally recursive.
        await self._ensure_client().delete(rel)

    async def _reconcile_failed_move(
        self,
        client: AsyncDavClient,
        src_rel: str,
        dst_rel: str,
        backup_rel: str,
    ) -> None:
        """Restore both paths after an ambiguous fallback MOVE result."""

        async def _exists(path: str) -> bool:
            try:
                await self.stat(path)
            except FileNotFoundError:
                return False
            return True

        with anyio.CancelScope(shield=True):
            try:
                for _attempt in range(4):
                    backup_exists = await _exists(backup_rel)
                    source_exists = await _exists(src_rel)
                    destination_exists = await _exists(dst_rel)
                    if source_exists and destination_exists:
                        if not backup_exists:
                            return
                        try:
                            await self._cleanup_move_backup(client, backup_rel)
                        except BaseException:
                            self.log.exception(f"Failed to delete move backup: {backup_rel}")
                            return
                        return
                    if not source_exists and destination_exists:
                        try:
                            await client.move(dst_rel, src_rel, overwrite=True)
                        except BaseException as error:
                            self.log.warning(f"Failed to restore move source; re-probing: {error!r}")
                            continue
                        continue
                    if backup_exists and not destination_exists:
                        try:
                            await client.move(backup_rel, dst_rel, overwrite=True)
                        except BaseException as error:
                            self.log.warning(f"Failed to restore move destination; re-probing: {error!r}")
                            continue
                        continue
                    return
                self.log.error(f"Failed to reconcile move state: {src_rel} → {dst_rel}")
            except BaseException:
                self.log.exception(f"Failed to reconcile move state: {src_rel} → {dst_rel}")

    async def _cleanup_move_backup(self, client: AsyncDavClient, backup_rel: str) -> None:
        last_error: BaseException | None = None
        with anyio.CancelScope(shield=True):
            for _attempt in range(2):
                try:
                    await client.delete(backup_rel)
                except BaseException as exc:
                    last_error = exc
                try:
                    await self.stat(backup_rel)
                except FileNotFoundError:
                    return
                except BaseException as exc:
                    last_error = exc

        message = f"Failed to delete move backup: {backup_rel}"
        if last_error is None:
            raise OSError(message)
        raise OSError(message) from last_error

    # ------------------------------------------------------------------
    # Move / Copy
    # ------------------------------------------------------------------

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_rel = self._remote_path(src)
        if src_rel == "":
            raise OSError(f"Cannot move root: {src}")
        try:
            source_info = await self.stat(src)
        except FileNotFoundError:
            raise FileNotFoundError(f"Source not found: {src}") from None
        if source_info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")
        if source_info.kind is not EntryKind.FILE:
            raise _unsupported_entry(src)

        dst_rel = self._remote_path(dst)
        if self.normalize_path(src) == self.normalize_path(dst):
            if not overwrite:
                raise FileExistsError(f"Source and destination are the same: {src}")
            return

        client = self._ensure_client()
        if await self.is_dir(dst):
            raise IsADirectoryError(f"Destination is a directory: {dst}")
        await self.mkdir(self.normalize_path(dst).parent.as_posix(), parents=True, exist_ok=True)
        try:
            await client.move(src_rel, dst_rel, overwrite=overwrite)
        except DavHttpStatusError as exc:
            if exc.status_code == 404:
                raise FileNotFoundError(f"Source not found: {src}") from exc
            if exc.status_code not in (409, 412):
                raise OSError(f"Failed to move {src} → {dst}: {exc}") from exc
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {dst}") from exc

        else:
            return
        backup_rel = f"{dst_rel}.storegate-move-backup-{uuid.uuid4().hex}"
        try:
            await client.move(dst_rel, backup_rel, overwrite=False)
        except BaseException as stage_exc:
            await self._reconcile_failed_move(client, src_rel, dst_rel, backup_rel)
            if isinstance(stage_exc, DavHttpStatusError):
                if stage_exc.status_code == 404:
                    try:
                        await client.move(src_rel, dst_rel, overwrite=True)
                    except DavHttpStatusError as retry_exc:
                        if retry_exc.status_code == 404:
                            raise FileNotFoundError(f"Source not found: {src}") from retry_exc
                        raise OSError(f"Failed to move {src} → {dst}: {retry_exc}") from retry_exc
                    return
                raise OSError(f"Failed to stage destination for move {src} → {dst}: {stage_exc}") from stage_exc
            raise

        try:
            await client.move(src_rel, dst_rel, overwrite=True)
        except BaseException as retry_exc:
            await self._reconcile_failed_move(client, src_rel, dst_rel, backup_rel)
            if isinstance(retry_exc, DavHttpStatusError):
                if retry_exc.status_code == 404:
                    raise FileNotFoundError(f"Source not found: {src}") from retry_exc
                raise OSError(f"Failed to move {src} → {dst}: {retry_exc}") from retry_exc
            raise

        await self._cleanup_move_backup(client, backup_rel)

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_rel = self._remote_path(src)
        if src_rel == "":
            raise IsADirectoryError(f"Cannot copy root: {src}")
        try:
            info = await self.stat(src)
        except FileNotFoundError:
            raise FileNotFoundError(f"Source not found: {src}") from None
        if info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")
        if info.kind is not EntryKind.FILE:
            raise _unsupported_entry(src)
        if self.normalize_path(src) == self.normalize_path(dst):
            if not overwrite:
                raise FileExistsError(f"Source and destination are the same: {src}")
            return
        dst_rel = self._remote_path(dst)
        if await self.is_dir(dst):
            raise IsADirectoryError(f"Destination is a directory: {dst}")
        await self.mkdir(self.normalize_path(dst).parent.as_posix(), parents=True, exist_ok=True)
        try:
            await self._ensure_client().copy(src_rel, dst_rel, overwrite=overwrite)
        except DavHttpStatusError as exc:
            if exc.status_code == 404:
                raise FileNotFoundError(f"Source not found: {src}") from exc
            if exc.status_code in (409, 412) and not overwrite:
                raise FileExistsError(f"Destination already exists: {dst}") from exc
            raise OSError(f"Failed to copy {src} → {dst}: {exc}") from exc

    @override
    @translator.wrap("Failed to copy tree {src} → {dst} (overwrite={overwrite})")
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_np = self.normalize_path(src)
        dst_np = self.normalize_path(dst)

        try:
            info = await self.stat(src)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {src}") from None
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        snapshot = await self._strict_walk_snapshot(src_np, root=info)

        client = self._ensure_client()
        self.log.info(f"CopyTree: <y>{escape_tag(src_np.as_posix())}</y> → <y>{escape_tag(dst_np.as_posix())}</y>")

        # Try a single server-side COPY with Depth: infinity first.
        try:
            await client.copy(
                self._remote_path(src),
                self._remote_path(dst),
                overwrite=overwrite,
                depth="infinity",
            )
        except DavHttpStatusError as exc:
            if exc.status_code not in (403, 405, 409, 501):
                raise
            # Server does not support recursive COPY — fall back below.
        else:
            return

        await self._copytree_fallback(src_np, dst_np, overwrite, snapshot=snapshot)

    @override
    @translator.wrap("Failed to move tree {src} → {dst} (overwrite={overwrite})")
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        try:
            info = await self.stat(src)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {src}") from None
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        await self._strict_walk_snapshot(src, root=info)

        client = self._ensure_client()
        await self.mkdir(self.normalize_path(dst).parent.as_posix(), parents=True, exist_ok=True)
        src_rel = self._remote_path(src)
        dst_rel = self._remote_path(dst)
        self.log.info(f"MoveTree: <y>{escape_tag(src_rel)}</y> → <y>{escape_tag(dst_rel)}</y>")
        try:
            await client.move(src_rel, dst_rel, overwrite=overwrite)
        except DavHttpStatusError as exc:
            if exc.status_code not in (403, 405, 501):
                if exc.status_code == 404:
                    raise FileNotFoundError(f"Source not found: {src}") from exc
                raise OSError(f"Failed to move tree: {src} → {dst}: {exc}") from exc
            # Server does not support recursive MOVE — fall back to copy + rmtree.
            await self.copytree(src, dst, overwrite=overwrite)
            await self.rmtree(src)

    async def _copytree_fallback(
        self,
        src_np: PurePosixPath,
        dst_np: PurePosixPath,
        overwrite: bool,
        *,
        snapshot: tuple[WalkEntry, ...] | None = None,
    ) -> None:
        """Walk-and-copy fallback with per-target destination backups."""
        client = self._ensure_client()
        _ = overwrite
        if snapshot is None:
            snapshot = await self._strict_walk_snapshot(src_np)
        for walk_entry in snapshot:
            for entry in walk_entry.entries:
                if entry.kind not in {EntryKind.FILE, EntryKind.DIRECTORY}:
                    raise _unsupported_entry(entry.path)
        backups: dict[PurePosixPath, PurePosixPath] = {}
        created: set[PurePosixPath] = set()
        created_dirs: set[PurePosixPath] = set()

        async def stage_target(path: PurePosixPath) -> None:
            if path in backups or path in created:
                return
            try:
                info = await self.stat(path)
            except FileNotFoundError:
                created.add(path)
                return
            if info.kind is EntryKind.DIRECTORY:
                return
            if info.kind is not EntryKind.FILE:
                raise _unsupported_entry(path)
            backup = PurePosixPath(f"{path.as_posix()}.storegate-copytree-backup-{uuid.uuid4().hex}")
            await client.move(self._remote_path(path), self._remote_path(backup), overwrite=False)
            backups[path] = backup

        try:
            if not await self.exists(dst_np):
                await self.mkdir(dst_np.as_posix(), parents=True, exist_ok=False)
                created_dirs.add(dst_np)
            for walk_entry in snapshot:
                relative = PurePosixPath(walk_entry.path).relative_to(src_np)
                target_dir = dst_np if relative == PurePosixPath(".") else dst_np / relative
                for directory in (entry for entry in walk_entry.entries if entry.kind is EntryKind.DIRECTORY):
                    target = target_dir / directory.name
                    if not await self.exists(target):
                        await self.mkdir(target.as_posix(), parents=True, exist_ok=False)
                        created_dirs.add(target)
                for file in (entry for entry in walk_entry.entries if entry.kind is EntryKind.FILE):
                    target = target_dir / file.name
                    await stage_target(target)
                    await self.copy(file.path, target.as_posix(), overwrite=True)
        except BaseException as primary:
            rollback_errors: list[BaseException] = []
            with anyio.CancelScope(shield=True):
                for path in created:
                    try:
                        await self.unlink(path, missing_ok=True)
                    except BaseException as error:
                        rollback_errors.append(error)
                for path, backup in backups.items():
                    try:
                        await client.move(self._remote_path(backup), self._remote_path(path), overwrite=True)
                    except BaseException as error:
                        rollback_errors.append(error)
                for directory in sorted(created_dirs, key=lambda path: len(path.parts), reverse=True):
                    try:
                        await self.rmdir(directory)
                    except BaseException as error:
                        rollback_errors.append(error)
            if rollback_errors:
                if isinstance(primary, Exception) and all(isinstance(error, Exception) for error in rollback_errors):
                    raise BaseExceptionGroup(
                        "Failed to copy tree and restore destination", [primary, *rollback_errors]
                    ) from None
                if isinstance(rollback_errors[0], anyio.get_cancelled_exc_class()):
                    raise
            raise OSError(f"Failed to copy tree: {src_np} → {dst_np}") from primary
        cleanup_errors: list[BaseException] = []
        with anyio.CancelScope(shield=True):
            for backup in backups.values():
                try:
                    await self.unlink(backup)
                except BaseException as error:
                    cleanup_errors.append(error)
        if cleanup_errors:
            if len(cleanup_errors) == 1:
                raise cleanup_errors[0]
            raise BaseExceptionGroup("Failed to clean copytree backups", cleanup_errors)
