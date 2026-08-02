from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from pathlib import PurePosixPath
from typing import final, override

import aioftp
import anyio

from storegate.log import escape_tag
from storegate.storage.abstract import (
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    UnsupportedOperationError,
    WalkEntry,
    validate_download_offset,
)
from storegate.utils import coalesce_chunks

from ._base import _UNSUPPORTED_ERRNO as _UNSUPPORTED_ERRNO
from ._base import FTPFacts as FTPFacts
from ._base import _parse_mlsx_datetime as _parse_mlsx_datetime
from ._base import _status_matches as _status_matches
from ._base import translator as translator
from ._move import FTPMoveMixin
from ._tree import FTPTreeMixin


@final
class FTPStorage(FTPTreeMixin, FTPMoveMixin):
    """Plain FTP client storage backend.

    FTP has no portable no-follow metadata primitive. Explicit MLSD/MLST
    link and special facts are rejected or hidden according to the operation,
    but a server may instead report a followed target as a file or directory.
    In that case this client cannot detect the link and does not provide a
    client-side confinement guarantee.

    The whole-tree transactions live in :mod:`._tree` and the staged single-file
    move in :mod:`._move`; shared state, lifecycle and remote-path primitives
    live in :mod:`._base`.
    """

    @override
    @translator.wrap("Failed to stat {path}")
    async def stat(self, path: PathLike) -> FileInfo:
        logical = self._logical_path(path)
        async with self._client_lease() as lease:
            return await self._stat(lease.client, logical)

    @override
    @translator.wrap("Failed to check existence of {path}")
    async def exists(self, path: PathLike) -> bool:
        logical = self._logical_path(path)
        async with self._client_lease() as lease:
            return await self._exists(lease.client, logical)

    @override
    @translator.wrap("Failed to check whether {path} is a file")
    async def is_file(self, path: PathLike) -> bool:
        logical = self._logical_path(path)
        async with self._client_lease() as lease:
            try:
                info = await self._stat(lease.client, logical)
            except FileNotFoundError:
                return False
            return info.kind is EntryKind.FILE

    @override
    @translator.wrap("Failed to check whether {path} is a directory")
    async def is_dir(self, path: PathLike) -> bool:
        logical = self._logical_path(path)
        async with self._client_lease() as lease:
            try:
                info = await self._stat(lease.client, logical)
            except FileNotFoundError:
                return False
            return info.kind is EntryKind.DIRECTORY

    @override
    @translator.wrap_agen("Failed to iterate directory {path}")
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        logical = self._logical_path(path)
        async with self._client_lease() as lease:
            entries = await self._list(lease.client, logical)
        for entry in entries:
            yield entry

    @override
    @translator.wrap_agen("Failed to walk directory {path}")
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        logical = self._logical_path(path)
        async with self._client_lease() as lease:
            snapshot = await self._walk_snapshot(lease.client, logical, strict=False)
        for entry in snapshot:
            yield entry

    @override
    @translator.wrap("Failed to create directory {path} (parents={parents}, exist_ok={exist_ok})")
    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        logical = self._logical_path(path)
        self.log.info(f"MkDir: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease() as lease:
            await self._mkdir(lease.client, path, parents=parents, exist_ok=exist_ok)

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
            offset = validate_download_offset(offset)
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

    @override
    @translator.wrap("Failed to remove directory tree {path}")
    async def rmtree(self, path: PathLike) -> None:
        logical = self._logical_path(path)
        self.log.info(f"RmTree: <y>{escape_tag(logical.as_posix())}</y>")
        async with self._client_lease() as lease:
            await self._rmtree(lease.client, path)
