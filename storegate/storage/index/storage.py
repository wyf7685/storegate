import functools
import hashlib
from collections.abc import AsyncGenerator
from pathlib import PurePosixPath
from typing import final, override

import anyio

from storegate.log import escape_tag

from ..abstract import (
    EntryKind,
    FileInfo,
    PathLike,
    WalkEntry,
    validate_download_offset,
    validate_same_path_file_operation,
)
from ._base import (
    BLOCK_SIZE as BLOCK_SIZE,
)
from ._base import (
    CHUNKS_INDEX_FILE as CHUNKS_INDEX_FILE,
)
from ._base import (
    DEFAULT_LOCK_LEASE as DEFAULT_LOCK_LEASE,
)
from ._base import (
    DEFAULT_LOCK_TIMEOUT as DEFAULT_LOCK_TIMEOUT,
)
from ._base import (
    MAX_CONCURRENT_UPLOADS as MAX_CONCURRENT_UPLOADS,
)
from ._base import (
    MIN_LOCK_LEASE as MIN_LOCK_LEASE,
)
from ._base import (
    RESERVED_SUFFIXES as RESERVED_SUFFIXES,
)
from ._guard import lstat_private_entry, lstat_private_entry_or_none
from ._tree import IndexTreeMixin
from ._upload import IndexUploadMixin
from .models import FileMeta as FileMeta
from .ref import hash_to_path


@final
class IndexStorage(IndexUploadMixin, IndexTreeMixin):
    """Chunked, reference-counted storage over an index and a chunk backend.

    Large files are split into ``block_size`` blocks, deduplicated by SHA-256
    and reference counted, so identical blocks are stored once. The upload and
    whole-tree transactions live in :mod:`._upload` and :mod:`._tree`; shared
    state, lifecycle and locking live in :mod:`._base`.
    """

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        offset = validate_download_offset(offset)
        remote_path = self.normalize_path(remote_path)
        _colored_path = f"<y>{escape_tag(remote_path)}</y>"
        self.log.debug(f"Download starting: {_colored_path}{f" (offset=<g>{offset}</g>)" if offset else ""}")

        async with self._lock_index(remote_path):
            meta = await self._get_file_meta(remote_path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {remote_path}")

            # --- locate the chunk where offset falls ---
            chunk_offset = 0  # byte position at start of current chunk
            target_idx = 0
            within_offset = offset  # will be refined once target chunk is found

            if offset > 0:
                target_idx = -1
                for idx, chunk_hash in enumerate(meta.chunks):
                    bin_path = hash_to_path(chunk_hash, "bin")
                    chunk_info = await lstat_private_entry_or_none(self._chunks, bin_path, label="chunk data")
                    if chunk_info is None:
                        raise FileNotFoundError(f"Chunk #{idx + 1} {chunk_hash} not found for file {remote_path}")
                    if chunk_info.kind is EntryKind.DIRECTORY:
                        raise IsADirectoryError(f"Chunk #{idx + 1} {chunk_hash} is a directory for file {remote_path}")
                    chunk_size = chunk_info.size
                    if chunk_offset + chunk_size > offset:
                        target_idx = idx
                        within_offset = offset - chunk_offset
                        break
                    chunk_offset += chunk_size

                if target_idx == -1:
                    # offset is beyond the file end — nothing to yield
                    self.log.debug(f"Offset <g>{offset}</g> beyond file end for {_colored_path}")
                    return

            # --- download from the target chunk onward ---
            total_size = chunk_offset  # bytes actually yielded (starts from chunk_offset for counters)
            file_start = anyio.current_time()

            for idx in range(target_idx, len(meta.chunks)):
                chunk_hash = meta.chunks[idx]
                is_target_chunk = bool(idx == target_idx and offset > 0)

                self.log.debug(
                    f"Downloading Chunk #{idx + 1} <c>{chunk_hash[:8]}</c> for {_colored_path}"
                    f"{" (target, skip <g>" + str(within_offset) + "</g>)" if is_target_chunk else ""}"
                )
                bin_path = hash_to_path(chunk_hash, "bin")
                async with self._refs.temp_ref(chunk_hash):
                    chunk_info = await lstat_private_entry_or_none(self._chunks, bin_path, label="chunk data")
                    if chunk_info is None:
                        raise FileNotFoundError(f"Chunk #{idx + 1} {chunk_hash} not found for file {remote_path}")
                    if chunk_info.kind is EntryKind.DIRECTORY:
                        raise IsADirectoryError(f"Chunk #{idx + 1} {chunk_hash} is a directory for file {remote_path}")
                    hasher = hashlib.sha256()
                    chunk_size = 0
                    local_skip = within_offset if is_target_chunk else 0
                    chunk_start = anyio.current_time()

                    async for chunk in self._chunks.download_stream(bin_path):
                        hasher.update(chunk)
                        chunk_size += len(chunk)

                        if local_skip > 0:
                            if local_skip >= len(chunk):
                                local_skip -= len(chunk)
                                continue
                            chunk = chunk[local_skip:]
                            local_skip = 0

                        yield chunk

                    chunk_elapsed = anyio.current_time() - chunk_start
                    actual_hash = hasher.hexdigest()
                    if actual_hash != chunk_hash:
                        self.log.error(
                            f"Chunk hash mismatch for <c>{chunk_hash[:8]}</c> (got <r>{actual_hash[:8]}</r>) "
                            f"for Chunk #{idx + 1} of {_colored_path}"
                        )
                        raise ValueError(
                            f"Chunk hash mismatch for {chunk_hash} (got {actual_hash}) "
                            f"for Chunk #{idx + 1} of {remote_path}"
                        )
                    self.log.debug(
                        f"Downloaded Chunk #{idx + 1} <c>{chunk_hash[:8]}</c> for {_colored_path} "
                        f"(<g>{chunk_size}</g> bytes, <g>{chunk_elapsed:.2f}</g> s)"
                    )
                    total_size += chunk_size

            file_elapsed = anyio.current_time() - file_start
            if offset:
                self.log.info(
                    f"Download complete (offset <g>{offset}</g>): {_colored_path} "
                    f"(<g>{total_size - chunk_offset}</g> bytes in <g>{len(meta.chunks) - target_idx}</g> chunks, "
                    f"<g>{file_elapsed:.2f}</g> s)"
                )
            else:
                self.log.info(
                    f"Download complete: {_colored_path} "
                    f"(<g>{total_size}</g> bytes in <g>{len(meta.chunks)}</g> chunks, "
                    f"<g>{file_elapsed:.2f}</g> s)"
                )

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        path = self.normalize_path(path)
        _colored_path = f"<y>{escape_tag(path)}</y>"

        info = await lstat_private_entry_or_none(self._index, path, label="metadata entry")
        if info is not None and info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {path}")

        async with self._lock_index(path):
            meta = await self._get_file_meta(path)
            if meta is None:
                if missing_ok:
                    return
                raise FileNotFoundError(f"File not found: {path}")
            async with self._lock_chunks(meta.chunks):
                # Metadata is the authoritative pointer, so it is dropped first. The
                # reverse order leaves a readable file whose chunks were already
                # reaped when the delete fails — every later download_stream then
                # raises "Chunk not found". A leaked chunk is recoverable; dangling
                # metadata is silent data loss.
                await self._index.unlink(path)
                # The file is already gone for readers; finish the refcount bookkeeping
                # even under cancellation so chunks are not stranded.
                with anyio.CancelScope(shield=True):
                    async with anyio.create_task_group() as tg:
                        for chunk_hash in meta.chunks:
                            tg.start_soon(self._refs.decref, chunk_hash, path)
        self.log.info(f"Deleted: {_colored_path} (<g>{meta.info.size}</g> bytes, <g>{len(meta.chunks)}</g> chunks)")

    @override
    async def rmdir(self, path: PathLike) -> None:
        path = self.normalize_path(path)
        try:
            info = await lstat_private_entry(self._index, path, label="index directory")
        except FileNotFoundError as error:
            raise FileNotFoundError(f"Directory not found: {path}") from error
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        if not await self._is_dir_empty(path):
            raise OSError(f"Directory not empty: {path}")
        # Strong-mode release leaves tombstones at ``{child}.lock``. They are
        # invisible to public iterdir, but still occupy the index directory.
        await self._purge_private_directory_residue(path)
        await self._index.rmdir(path)

    async def _purge_private_directory_residue(self, path: PurePosixPath) -> None:
        """Remove internal lock tombstones so empty public dirs can rmdir."""
        async for entry in self._index.iterdir(path):
            entry_path = self.normalize_path(entry.path)
            if entry.kind is not EntryKind.FILE:
                raise OSError(f"Directory not empty: {path}")
            if await self._read_is_tombstone(entry_path):
                await self._index.unlink(entry_path, missing_ok=True)
                continue
            # Active lock files or other private residue still block removal.
            raise OSError(f"Directory not empty: {path}")

    @override
    async def move(
        self,
        src: PathLike,
        dst: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        self._reject_reserved(src, dst)
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            source_info = await lstat_private_entry(self._index, src, label="source entry")
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = source_info.kind
        if validate_same_path_file_operation(
            src,
            dst,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source file not found: {src}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")

        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        async with self._lock_indexes(src, dst):
            src_meta = await self._get_file_meta(src)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst)
            if dst_meta is not None and not overwrite:
                raise FileExistsError(f"Destination file already exists: {dst}")

            src_hashes = set(src_meta.chunks)
            old_hashes = set(dst_meta.chunks) if dst_meta is not None else set()
            old_only = old_hashes - src_hashes
            async with self._lock_chunks(src_hashes | old_hashes):
                guards = await self._refs.add_rollback_guards(old_only)
                try:
                    try:
                        async with anyio.create_task_group() as tg:
                            for chunk_hash in src_meta.chunks:
                                tg.start_soon(self._refs.transref, chunk_hash, (src, dst))
                        new_meta = FileMeta(
                            info=src_meta.info.at_path(dst),
                            chunks=src_meta.chunks.copy(),
                        )
                        await self._index.mkdir(dst.parent, parents=True, exist_ok=True)
                        await self._index.upload_bytes(new_meta.model_dump_json().encode(), dst, overwrite=True)
                    except BaseException:
                        with anyio.CancelScope(shield=True):
                            async with anyio.create_task_group() as tg:
                                for chunk_hash in src_meta.chunks:
                                    if chunk_hash in old_hashes:
                                        tg.start_soon(self._refs.incref, chunk_hash, src)
                                    else:
                                        pfunc = functools.partial(
                                            self._refs.transref, chunk_hash, (dst, src), missing_ok=True
                                        )
                                        tg.start_soon(pfunc)
                            await self._refs.release_rollback_guards(guards)
                        raise

                    try:
                        for chunk_hash in old_only:
                            await self._refs.decref(chunk_hash, dst)
                        # Removing the source is the commit point, not an epilogue: it must
                        # sit inside the rollback arm below. Outside it, a failure here would
                        # leave a readable src whose chunks are only referenced by dst, so a
                        # later unlink(dst) reaps the chunks and silently guts src.
                        await self._index.unlink(src)
                    except BaseException:
                        with anyio.CancelScope(shield=True):
                            # src metadata is restored unconditionally: the unlink above may
                            # have landed before raising, and re-uploading identical bytes is
                            # idempotent when it did not.
                            await self._index.upload_bytes(src_meta.model_dump_json().encode(), src, overwrite=True)
                            if dst_meta is not None:
                                await self._index.upload_bytes(dst_meta.model_dump_json().encode(), dst, overwrite=True)
                            else:
                                # dst did not exist before the move, so the metadata written
                                # at the start of this transaction has to go with it.
                                await self._index.unlink(dst, missing_ok=True)
                            async with anyio.create_task_group() as tg:
                                for chunk_hash in old_only:
                                    tg.start_soon(self._refs.incref, chunk_hash, dst)
                                for chunk_hash in src_meta.chunks:
                                    if chunk_hash in old_hashes:
                                        tg.start_soon(self._refs.incref, chunk_hash, src)
                                    else:
                                        pfunc = functools.partial(
                                            self._refs.transref, chunk_hash, (dst, src), missing_ok=True
                                        )
                                        tg.start_soon(pfunc)
                            await self._refs.release_rollback_guards(guards)
                        raise
                    with anyio.CancelScope(shield=True):
                        await self._refs.release_rollback_guards(guards)
                except BaseException:  # noqa: TRY203
                    raise

    @override
    async def copy(
        self,
        src: PathLike,
        dst: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        self._reject_reserved(src, dst)
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            source_info = await lstat_private_entry(self._index, src, label="source entry")
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = source_info.kind
        if validate_same_path_file_operation(
            src,
            dst,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source file not found: {src}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")

        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        async with self._lock_indexes(src, dst):
            src_meta = await self._get_file_meta(src)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst)
            if dst_meta is not None and not overwrite:
                raise FileExistsError(f"Destination file already exists: {dst}")

            src_hashes = set(src_meta.chunks)
            old_hashes = set(dst_meta.chunks) if dst_meta is not None else set()
            old_only = old_hashes - src_hashes
            async with self._lock_chunks(src_hashes | old_hashes):
                guards = await self._refs.add_rollback_guards(old_only)
                try:
                    try:
                        async with anyio.create_task_group() as tg:
                            for chunk_hash in src_meta.chunks:
                                if chunk_hash not in old_hashes:
                                    tg.start_soon(self._refs.incref, chunk_hash, dst)
                        new_meta = FileMeta(
                            info=src_meta.info.at_path(dst),
                            chunks=src_meta.chunks.copy(),
                        )
                        await self._index.mkdir(dst.parent, parents=True, exist_ok=True)
                        await self._index.upload_bytes(new_meta.model_dump_json().encode(), dst, overwrite=True)
                    except BaseException:
                        with anyio.CancelScope(shield=True):
                            async with anyio.create_task_group() as tg:
                                for chunk_hash in src_meta.chunks:
                                    if chunk_hash not in old_hashes:
                                        tg.start_soon(self._refs.decref, chunk_hash, dst)
                            await self._refs.release_rollback_guards(guards)
                        raise

                    try:
                        for chunk_hash in old_only:
                            await self._refs.decref(chunk_hash, dst)
                    except BaseException:
                        with anyio.CancelScope(shield=True):
                            if dst_meta is not None:
                                await self._index.upload_bytes(dst_meta.model_dump_json().encode(), dst, overwrite=True)
                            async with anyio.create_task_group() as tg:
                                for chunk_hash in old_only:
                                    tg.start_soon(self._refs.incref, chunk_hash, dst)
                                for chunk_hash in src_meta.chunks:
                                    if chunk_hash not in old_hashes:
                                        tg.start_soon(self._refs.decref, chunk_hash, dst)
                            await self._refs.release_rollback_guards(guards)
                        raise
                    with anyio.CancelScope(shield=True):
                        await self._refs.release_rollback_guards(guards)
                except BaseException:  # noqa: TRY203
                    raise

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        self._reject_reserved(path)
        path = self.normalize_path(path)
        await lstat_private_entry_or_none(self._index, path, label="index entry")
        await self._index.mkdir(path, parents=parents, exist_ok=exist_ok)

    @override
    async def rmtree(self, path: PathLike) -> None:
        path = self.normalize_path(path)
        root_info = await lstat_private_entry(self._index, path, label="tree root")
        if root_info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        _colored_path = f"<y>{escape_tag(path)}</y>"
        self.log.info(f"RmTree: {_colored_path}")

        metas, relative_directories, tombstone_locks = await self._collect_tree(path)
        # Unlink user files first. Strong release writes a tombstone at ``{path}.lock``,
        # which is the same object rmtree would otherwise delete for residual cleanup.
        # Concurrent tombstone deletion races active holders (renewal/release CAS) and
        # surfaces as LockLeaseLostError under ordinary contract cleanup.
        async with anyio.create_task_group() as tg:
            for file_path in metas:
                tg.start_soon(self.unlink, file_path)
        residual_locks = {self.normalize_path(lock_path) for lock_path in tombstone_locks}
        residual_locks.update(self.normalize_path(f"{file_path}.lock") for file_path in metas)
        async with anyio.create_task_group() as tg:
            for lock_path in residual_locks:

                async def _unlink_lock(target: PurePosixPath = lock_path) -> None:
                    await self._index.unlink(target, missing_ok=True)

                tg.start_soon(_unlink_lock)
        for relative in sorted(relative_directories, key=lambda item: len(item.parts), reverse=True):
            await self._index.rmdir(path / relative)
        await self._index.rmdir(path)
        self.log.info(
            f"RmTree complete: {_colored_path} (<g>{len(metas) + len(relative_directories)}</g> entries removed)"
        )

    @override
    async def exists(self, path: PathLike) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    @override
    async def is_file(self, path: PathLike) -> bool:
        try:
            return (await self.stat(path)).kind is EntryKind.FILE
        except FileNotFoundError:
            return False

    @override
    async def is_dir(self, path: PathLike) -> bool:
        try:
            return (await self.stat(path)).kind is EntryKind.DIRECTORY
        except FileNotFoundError:
            return False

    @override
    async def is_symlink(self, path: PathLike) -> bool:
        try:
            await self.lstat(path)
        except FileNotFoundError:
            return False
        return False

    @override
    async def lstat(self, path: PathLike) -> FileInfo:
        return await self.stat(path)

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        path = self.normalize_path(path)
        try:
            info = await lstat_private_entry(self._index, path, label="index entry")
        except FileNotFoundError as error:
            raise FileNotFoundError(f"File not found: {path}") from error
        if info.kind is EntryKind.DIRECTORY:
            return info.at_path(path)
        meta = await self._get_file_meta(path)
        if meta is None:
            raise FileNotFoundError(f"File not found: {path}")
        return meta.info.at_path(path)

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        path = self.normalize_path(path)
        root_info = await lstat_private_entry(self._index, path, label="directory root")
        if root_info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        async for entry in self._index.iterdir(path):
            entry_path = self.normalize_path(entry.path)
            match entry.kind:
                case EntryKind.DIRECTORY:
                    yield entry.at_path(entry_path)
                case EntryKind.FILE:
                    meta = await self._get_file_meta(entry_path)
                    if meta is not None:
                        yield meta.info.at_path(entry_path)
                case EntryKind.SYMLINK:
                    await lstat_private_entry(self._index, entry_path, label="directory entry")

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        path = self.normalize_path(path)
        root_info = await lstat_private_entry(self._index, path, label="walk root")
        if root_info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")

        async def _fetch_meta(entry_path: PurePosixPath, entries: list[FileInfo]) -> None:
            meta = await self._get_file_meta(entry_path)
            if meta is not None:
                entries.append(meta.info.at_path(entry_path))

        async for underlying_entry in self._index.walk(path):
            entries: list[FileInfo] = []
            async with anyio.create_task_group() as tg:
                for entry in underlying_entry.entries:
                    entry_path = self.normalize_path(entry.path)
                    match entry.kind:
                        case EntryKind.DIRECTORY:
                            entries.append(entry.at_path(entry_path))
                        case EntryKind.FILE:
                            tg.start_soon(_fetch_meta, entry_path, entries)
                        case EntryKind.SYMLINK:
                            await lstat_private_entry(self._index, entry_path, label="walk entry")
            entries.sort(key=lambda entry: entry.path)
            yield WalkEntry(path=self.normalize_path(underlying_entry.path).as_posix(), entries=tuple(entries))

    @override
    async def list_(self, path: PathLike) -> list[FileInfo]:
        entries = [entry async for entry in self.iterdir(path)]
        entries.sort(key=lambda entry: entry.path)
        return entries
