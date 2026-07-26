from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from pathlib import PurePosixPath
from typing import final, override

from storegate.log import escape_tag
from storegate.storage.abstract import (
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    WalkEntry,
    validate_download_offset,
)
from storegate.utils import coalesce_chunks

from ._base import (
    _DAV_CAPABILITIES as _DAV_CAPABILITIES,
)
from ._base import (
    _RECURSIVE_OP_FALLBACK_STATUSES as _RECURSIVE_OP_FALLBACK_STATUSES,
)
from ._base import (
    _UNSUPPORTED_ERRNO as _UNSUPPORTED_ERRNO,
)
from ._base import DavHttpStatusError
from ._base import (
    _unsupported_entry as _unsupported_entry,
)
from ._base import (
    translator as translator,
)
from ._move import DavMoveMixin
from ._tree import DavTreeMixin
from .utils import dav_resource_to_file_info, href_to_storage_path


@final
class DavStorage(DavTreeMixin, DavMoveMixin):
    """WebDAV client storage backend.

    Core WebDAV exposes ordinary files and collections but has no standard
    symlink inspection or creation primitives. Non-collection extension
    resource types are treated as unsupported rather than regular files.
    Servers that follow links and report only the resolved target remain
    outside client-side detection; confinement then depends on the server.

    The whole-tree and single-entry move/copy transactions live in
    :mod:`._tree` and :mod:`._move`; shared state, lifecycle and the directory
    primitives live in :mod:`._base`.
    """

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

        # Iterative depth-first: each directory costs exactly one Depth:1
        # PROPFIND. Recursing through the public generator would re-stat every
        # level and bound the walk by the Python stack.
        pending = [self.normalize_path(path)]
        while pending:
            current = pending.pop()
            entries = await self._scan_directory(current, strict=False)
            yield WalkEntry(path=current.as_posix(), entries=entries)
            directories = [entry for entry in entries if entry.kind is EntryKind.DIRECTORY]
            pending.extend(PurePosixPath(entry.path) for entry in reversed(directories))

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
        offset = validate_download_offset(offset)
        info = await self.stat(remote_path)
        if info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {remote_path}")
        if info.kind is not EntryKind.FILE:
            raise _unsupported_entry(remote_path)
        if offset >= info.size:
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
        """Delete an empty collection.

        WebDAV has no non-recursive collection delete, so emptiness is checked
        with a Depth:1 PROPFIND before issuing DELETE. That check and the DELETE
        are separate requests: an entry created in between is removed by the
        recursive DELETE without error. Backends with a native non-recursive
        rmdir (local, SFTP, FTP) do not have this window.
        """
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
        # WebDAV DELETE is recursive; emptiness was pre-checked above, but see
        # the docstring for the race this leaves open.
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
