import contextlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from pathlib import Path, PurePosixPath
from typing import final, override
from urllib.parse import urlparse

import anyio

from app.log import escape_tag
from app.storage.abstract import AbstractStorage, BytesLike, FileInfo, PathLike
from app.utils import ExceptionTranslator, coalesce_chunks, flatten_exception_group

from .dav_client import AsyncDavClient, DavClientError, DavConfig, DavHttpStatusError
from .utils import dav_resource_to_file_info, href_to_storage_path

translator = ExceptionTranslator(
    bypass=OSError,
    catch=DavClientError,
    default=OSError,
)


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

    Accesses a remote WebDAV server (Nextcloud, ownCloud, Apache mod_dav,
    wsgidav, ...) via HTTP and exposes it as an :class:`AbstractStorage`.
    Symmetric with :class:`S3Storage`: a self-built async HTTP client
    (``dav_client/``) wrapped by a storage adapter.
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
        self._client = AsyncDavClient(self._config)
        await self._client.__aenter__()
        if not await self.ping():
            raise RuntimeError("Failed to connect to WebDAV server. Please check your configuration.")
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
            return FileInfo(path=np.as_posix(), name="", is_dir=True, size=0)

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
        return not info.is_dir

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
            if not info.is_dir:
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

    @override
    @translator.wrap_agen("Failed to iterate directory {path}")
    async def iterdir(self, path: PathLike) -> AsyncIterator[FileInfo]:
        rel = self._remote_path(path)
        client = self._ensure_client()
        resources = await client.propfind(rel, depth=1)
        for resource in resources:
            storage_path = href_to_storage_path(resource.href, self._url_prefix)
            if storage_path == rel or storage_path == "":
                continue  # skip the collection itself
            yield dav_resource_to_file_info(resource, storage_path)

    @override
    @translator.wrap_agen("Failed to walk directory {path}")
    async def walk(self, path: PathLike) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        dirs: list[FileInfo] = []
        files: list[FileInfo] = []
        async for entry in self.iterdir(path):
            (dirs if entry.is_dir else files).append(entry)

        yield self.normalize_path(path).as_posix(), dirs, files
        for dir in dirs:
            async for sp, sd, sf in self.walk(dir.path):
                yield sp, sd, sf

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
            if info.is_dir:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
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
        if info.is_dir:
            raise IsADirectoryError(f"Is a directory: {remote_path}")
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

        if info.is_dir:
            raise IsADirectoryError(f"Is a directory: {path}")

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
            return  # silent success, aligns with S3

        if not info.is_dir:
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

        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {path}")

        self.log.info(f"RmTree: <y>{escape_tag(rel)}</y>")
        # WebDAV DELETE on a collection is naturally recursive.
        await self._ensure_client().delete(rel)

    # ------------------------------------------------------------------
    # Move / Copy
    # ------------------------------------------------------------------

    @override
    async def move(self, src: PathLike, dst: PathLike) -> None:
        src_rel = self._remote_path(src)
        if src_rel == "":
            raise OSError(f"Cannot move root: {src}")
        dst_rel = self._remote_path(dst)

        client = self._ensure_client()
        self.log.info(f"Move: <y>{escape_tag(src_rel)}</y> → <y>{escape_tag(dst_rel)}</y>")
        await self.mkdir(self.normalize_path(dst).parent.as_posix(), parents=True, exist_ok=True)
        try:
            await client.move(src_rel, dst_rel, overwrite=True)
        except DavHttpStatusError as exc:
            if exc.status_code == 404:
                raise FileNotFoundError(f"Source not found: {src}") from exc
            if exc.status_code == 412:
                # Server refused to overwrite the destination — delete it then retry.
                try:
                    await client.delete(dst_rel)
                except DavHttpStatusError as del_exc:
                    if del_exc.status_code != 404:
                        raise OSError(f"Failed to move {src} → {dst}: {del_exc}") from del_exc
                await client.move(src_rel, dst_rel, overwrite=True)
            else:
                raise OSError(f"Failed to move {src} → {dst}: {exc}") from exc

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(self, src: PathLike, dst: PathLike) -> None:
        src_rel = self._remote_path(src)
        if src_rel == "":
            raise IsADirectoryError(f"Cannot copy root: {src}")

        try:
            info = await self.stat(src)
        except FileNotFoundError:
            raise FileNotFoundError(f"Source not found: {src}") from None

        if info.is_dir:
            raise IsADirectoryError(f"Is a directory: {src}")

        dst_rel = self._remote_path(dst)
        client = self._ensure_client()
        await self.mkdir(self.normalize_path(dst).parent.as_posix(), parents=True, exist_ok=True)
        self.log.info(f"Copy: <y>{escape_tag(src_rel)}</y> → <y>{escape_tag(dst_rel)}</y>")
        await client.copy(src_rel, dst_rel, overwrite=True)

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_np = self.normalize_path(src)
        dst_np = self.normalize_path(dst)

        try:
            info = await self.stat(src)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {src}") from None
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

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

        await self._copytree_fallback(src_np, dst_np, overwrite)

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        try:
            info = await self.stat(src)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {src}") from None
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

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

    async def _copytree_fallback(self, src_np: PurePosixPath, dst_np: PurePosixPath, overwrite: bool) -> None:
        """Walk-and-copy fallback for servers without recursive COPY."""
        try:
            await self.mkdir(dst_np.as_posix(), parents=True, exist_ok=overwrite)
            async for sp, sd, sf in self.walk(src_np):
                rel = PurePosixPath(sp).relative_to(src_np)
                dst_dir = dst_np if rel == PurePosixPath(".") else dst_np / rel
                for d in sd:
                    await self.mkdir((dst_dir / d.name).as_posix(), exist_ok=True)
                for f in sf:
                    await self.copy(PurePosixPath(f.path).as_posix(), (dst_dir / f.name).as_posix())
        except Exception as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to copy tree: <y>{escape_tag(src_np.as_posix())}</y>"
                f" → <y>{escape_tag(dst_np.as_posix())}</y> — <r>{escape_tag(repr(exc))}</r>"
            )
            try:
                with anyio.CancelScope(shield=True):
                    with contextlib.suppress(Exception):
                        await self.rmtree(dst_np)
            except Exception:
                self.log.exception(f"Failed to rollback destination: <y>{escape_tag(dst_np.as_posix())}</y>")
            raise OSError(f"Failed to copy tree: {src_np} → {dst_np}") from exc
