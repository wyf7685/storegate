import uuid
from pathlib import PurePosixPath
from typing import override

import anyio

from storegate.log import escape_tag
from storegate.storage.abstract import (
    EntryKind,
    PathLike,
    WalkEntry,
    validate_same_path_tree_operation,
)

from ._base import (
    _RECURSIVE_OP_FALLBACK_STATUSES,
    DavHttpStatusError,
    DavStorageBase,
    _unsupported_entry,
    translator,
)


class DavTreeMixin(DavStorageBase):
    """Whole-tree copy/move, with the explicit walk-and-copy fallback.

    ``copytree``/``movetree`` first attempt a single server-side Depth:infinity
    COPY/MOVE and fall back to ``_copytree_fallback`` for the statuses in
    ``_RECURSIVE_OP_FALLBACK_STATUSES``. The fallback backs up every destination
    file it overwrites and every collection it creates, restoring them on any
    failure so a partially applied tree never surfaces as a plain error.
    """

    @override
    @translator.wrap("Failed to copy tree {src} → {dst} (overwrite={overwrite})")
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_np = self.normalize_path(src)
        dst_np = self.normalize_path(dst)

        try:
            info = await self.stat(src)
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = info.kind
        if validate_same_path_tree_operation(
            src_np,
            dst_np,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is not EntryKind.DIRECTORY:
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
            if exc.status_code not in _RECURSIVE_OP_FALLBACK_STATUSES:
                raise
            # 403/405/501: server refuses recursive COPY. 409 (RFC 4918 §9.8.5):
            # an intermediate collection is missing. Both are resolved by the
            # walk fallback, which creates every destination directory itself.
        else:
            return

        await self._copytree_fallback(src_np, dst_np, overwrite, snapshot=snapshot)

    @override
    @translator.wrap("Failed to move tree {src} → {dst} (overwrite={overwrite})")
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_np = self.normalize_path(src)
        dst_np = self.normalize_path(dst)

        try:
            info = await self.stat(src)
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = info.kind
        if validate_same_path_tree_operation(
            src_np,
            dst_np,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        await self._strict_walk_snapshot(src_np, root=info)

        client = self._ensure_client()
        await self.mkdir(dst_np.parent.as_posix(), parents=True, exist_ok=True)
        src_rel = self._remote_path(src)
        dst_rel = self._remote_path(dst)
        self.log.info(f"MoveTree: <y>{escape_tag(src_rel)}</y> → <y>{escape_tag(dst_rel)}</y>")
        try:
            await client.move(src_rel, dst_rel, overwrite=overwrite)
        except DavHttpStatusError as exc:
            if exc.status_code not in _RECURSIVE_OP_FALLBACK_STATUSES:
                if exc.status_code == 404:
                    raise FileNotFoundError(f"Source not found: {src}") from exc
                raise OSError(f"Failed to move tree: {src} → {dst}: {exc}") from exc
            # Same fallback set as copytree: the copy + rmtree path creates every
            # destination collection explicitly, so it also resolves a 409.
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
