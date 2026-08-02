from __future__ import annotations

import contextlib
from pathlib import PurePosixPath
from typing import NoReturn, override

import anyio

from storegate.log import escape_tag

from ..abstract import EntryKind, PathLike, validate_same_path_tree_operation
from ._base import IndexStorageBase
from ._guard import download_private_file, lstat_private_entry, lstat_private_entry_or_none
from .lock import _is_tombstone
from .models import FileMeta


class IndexTreeMixin(IndexStorageBase):
    """Whole-tree copy/move as a single reversible transaction.

    ``_apply_tree_transaction`` snapshots the source, records every destination
    file it is about to overwrite plus every directory it creates, and pins the
    outgoing destination chunks behind rollback guards. Any failure restores
    that snapshot and re-raises primary-first, so a partially applied tree never
    surfaces as a plain error the caller might retry into.
    """

    async def _read_is_tombstone(self, entry_path: PurePosixPath) -> bool:
        """Return True when *entry_path* is a recognised clean-release tombstone."""
        try:
            data = await download_private_file(self._index, entry_path, label="tree entry")
            return _is_tombstone(data)
        except (FileNotFoundError, OSError):
            return False

    async def _collect_tree(
        self, root: PurePosixPath
    ) -> tuple[dict[PurePosixPath, FileMeta], list[PurePosixPath], list[PurePosixPath]]:
        metas: dict[PurePosixPath, FileMeta] = {}
        relatives: list[PurePosixPath] = []
        tombstone_locks: list[PurePosixPath] = []
        async for walk_entry in self._index.walk(root):
            for entry in walk_entry.entries:
                entry_path = self.normalize_path(entry.path)
                match entry.kind:
                    case EntryKind.DIRECTORY:
                        relatives.append(entry_path.relative_to(root))
                    case EntryKind.FILE:
                        try:
                            meta = await self._get_file_meta(entry_path)
                        except OSError:
                            meta = None
                        if meta is not None:
                            metas[entry_path] = meta
                        elif await self._read_is_tombstone(entry_path):
                            tombstone_locks.append(entry_path)
                        # Non-metadata, non-tombstone files (e.g. active lock files)
                        # are silently skipped.
                    case EntryKind.SYMLINK:
                        await lstat_private_entry(self._index, entry_path, label="tree entry")
        return metas, relatives, tombstone_locks

    @staticmethod
    def _raise_tree_failure(primary: BaseException, rollback_error: BaseException | None) -> NoReturn:
        if rollback_error is None:
            raise primary
        if isinstance(primary, Exception) and isinstance(rollback_error, Exception):
            raise BaseExceptionGroup("Tree transaction and rollback failed", [primary, rollback_error]) from None
        if isinstance(rollback_error, anyio.get_cancelled_exc_class()):
            raise rollback_error
        raise primary

    async def _ensure_tree_directory(self, directory: PurePosixPath, created: set[PurePosixPath]) -> None:
        if directory == PurePosixPath("/"):
            return
        info = await lstat_private_entry_or_none(self._index, directory, label="tree directory")
        if info is not None:
            if info.kind is not EntryKind.DIRECTORY:
                raise FileExistsError(f"Path is a file: {directory}")
            return
        await self._ensure_tree_directory(directory.parent, created)
        await self._index.mkdir(directory, parents=False, exist_ok=False)
        created.add(directory)

    async def _restore_tree_file(self, path: PurePosixPath, meta: FileMeta) -> None:
        current = await self._get_file_meta(path)
        if current is not None:
            current_hashes = set(current.chunks)
            expected_hashes = set(meta.chunks)
            for chunk_hash in current_hashes - expected_hashes:
                async with self._lock_chunk(chunk_hash):
                    await self._refs.decref(chunk_hash, path)
        await self._index.mkdir(path.parent, parents=True, exist_ok=True)
        await self._index.upload_bytes(meta.model_dump_json().encode(), path, overwrite=True)
        for chunk_hash in meta.chunks:
            async with self._lock_chunk(chunk_hash):
                refs = await self._refs.load_refs(chunk_hash) or set()
                if self.normalize_path(path).as_posix() not in refs:
                    await self._refs.incref(chunk_hash, path)

    async def _restore_tree_transaction(
        self,
        source_root: PurePosixPath,
        source_metas: dict[PurePosixPath, FileMeta],
        source_dirs: list[PurePosixPath],
        destination_metas: dict[PurePosixPath, FileMeta],
        destination_paths: set[PurePosixPath],
        created_destination_dirs: set[PurePosixPath],
        *,
        move: bool,
    ) -> None:
        if move:
            recreated_source_dirs: set[PurePosixPath] = set()
            for directory in sorted(
                [source_root, *(source_root / relative for relative in source_dirs)],
                key=lambda path: len(path.parts),
            ):
                await self._ensure_tree_directory(directory, recreated_source_dirs)
            for path, meta in source_metas.items():
                await self._restore_tree_file(path, meta)

        for path in destination_paths:
            meta = destination_metas.get(path)
            if meta is None:
                await self.unlink(path, missing_ok=True)
            else:
                await self._restore_tree_file(path, meta)
        for directory in sorted(created_destination_dirs, key=lambda path: len(path.parts), reverse=True):
            with contextlib.suppress(OSError, FileNotFoundError):
                await self._index.rmdir(directory)

    async def _apply_tree_transaction(
        self,
        src: PurePosixPath,
        dst: PurePosixPath,
        *,
        move: bool,
    ) -> tuple[int, int]:
        source_metas, source_dirs, _tombstone_locks = await self._collect_tree(src)
        destination_paths = {dst.joinpath(path.relative_to(src)) for path in source_metas}
        destination_metas: dict[PurePosixPath, FileMeta] = {}
        for path in destination_paths:
            info = await lstat_private_entry_or_none(self._index, path, label="tree destination")
            if info is not None and info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Destination is a directory: {path}")
            if meta := await self._get_file_meta(path):
                destination_metas[path] = meta

        old_destination_chunks = {chunk_hash for meta in destination_metas.values() for chunk_hash in meta.chunks}
        guards = await self._refs.add_tree_rollback_guards(old_destination_chunks)
        created_destination_dirs: set[PurePosixPath] = set()
        try:
            for directory in sorted(
                [dst, *(dst / relative for relative in source_dirs)], key=lambda path: len(path.parts)
            ):
                await self._ensure_tree_directory(directory, created_destination_dirs)
            for source_path in sorted(source_metas, key=lambda path: path.as_posix()):
                destination_path = dst.joinpath(source_path.relative_to(src))
                if move:
                    await self.move(source_path, destination_path, overwrite=True)
                else:
                    await self.copy(source_path, destination_path, overwrite=True)
            if move:
                # Clean up lock tombstones before rmdir; validate payload, not suffix.
                tombstone_orphans: list[PurePosixPath] = []
                for relative in sorted(
                    [src] + [src / relative for relative in source_dirs],
                    key=lambda path: len(path.parts),
                    reverse=True,
                ):
                    async for entry in self._index.iterdir(relative):
                        entry_path = relative / entry.name
                        if await self._read_is_tombstone(entry_path):
                            tombstone_orphans.append(entry_path)
                for path in tombstone_orphans:
                    await self._index.unlink(path)
                for relative in sorted(source_dirs, key=lambda path: len(path.parts), reverse=True):
                    await self._index.rmdir(src / relative)
                await self._index.rmdir(src)
        except BaseException as primary:
            rollback_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await self._restore_tree_transaction(
                        src,
                        source_metas,
                        source_dirs,
                        destination_metas,
                        destination_paths,
                        created_destination_dirs,
                        move=move,
                    )
                except BaseException as error:
                    rollback_error = error
                try:
                    await self._refs.release_tree_rollback_guards(guards)
                except BaseException as error:
                    if rollback_error is None:
                        rollback_error = error
            self._raise_tree_failure(primary, rollback_error)
        else:
            await self._refs.release_tree_rollback_guards(guards)
        return len(source_metas), len(source_dirs)

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        self._reject_reserved(src, dst)
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            src_info = await lstat_private_entry(self._index, src, label="tree root")
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = src_info.kind
        if validate_same_path_tree_operation(
            src,
            dst,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {src}")
        dst_info = await lstat_private_entry_or_none(self._index, dst, label="tree destination")
        if not overwrite and dst_info is not None:
            raise FileExistsError(f"Destination already exists: {dst}")
        if dst.is_relative_to(src):
            raise ValueError("Destination must not be inside the source tree")
        async with self._lock_indexes(f"{src}.tree", f"{dst}.tree"):
            files, directories = await self._apply_tree_transaction(src, dst, move=False)
        self.log.info(
            f"CopyTree complete: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
            f"(<g>{files}</g> files, <g>{directories}</g> dirs)"
        )

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        self._reject_reserved(src, dst)
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            src_info = await lstat_private_entry(self._index, src, label="tree root")
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = src_info.kind
        if validate_same_path_tree_operation(
            src,
            dst,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {src}")
        dst_info = await lstat_private_entry_or_none(self._index, dst, label="tree destination")
        if not overwrite and dst_info is not None:
            raise FileExistsError(f"Destination already exists: {dst}")
        if dst.is_relative_to(src):
            raise ValueError("Destination must not be inside the source tree")
        async with self._lock_indexes(f"{src}.tree", f"{dst}.tree"):
            files, directories = await self._apply_tree_transaction(src, dst, move=True)
        self.log.info(
            f"MoveTree complete: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
            f"(<g>{files}</g> files, <g>{directories}</g> dirs)"
        )
