import itertools
import uuid
from collections.abc import AsyncIterable, Iterable
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import override

import anyio

from storegate.log import escape_tag
from storegate.storage.abstract import (
    EntryKind,
    FileInfo,
    PathLike,
    validate_same_path_tree_operation,
)

from ._base import COPYTREE_MAX_WORKERS, S3StorageBase, translator
from .client import AsyncS3Client
from .utils import serialize_file_info


class S3TreeMixin(S3StorageBase):
    """Whole-tree transactions: rmtree, copytree and movetree."""

    @override
    @translator.wrap("Failed to remove directory tree {path}")
    async def rmtree(self, path: PathLike) -> None:
        key = self._remote_path_to_key(path)
        client = self._ensure_client()
        self.log.info(f"RmTree: <y>{escape_tag(key)}</y>")

        if not await self.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        # 先收集再删除：避免删除操作干扰 walk 的 list_objects 分页迭代
        file_keys: list[str] = []
        dir_paths: list[str] = []
        async for walked in self.walk(path):
            file_keys.extend(
                self._remote_path_to_key(entry.path) for entry in walked.entries if entry.kind is EntryKind.FILE
            )
            dir_paths.extend(entry.path for entry in walked.entries if entry.kind is EntryKind.DIRECTORY)

        # Batch file deletion. S3 has no tree delete, so this is inherently
        # non-atomic and a mid-way failure cannot be rolled back -- the objects
        # already deleted are gone. Name the keys that survived instead of
        # letting the caller guess which half of the tree is left.
        deleted_files = 0
        for batch in itertools.batched(file_keys, 100):
            if not batch:
                continue
            try:
                await client.delete_objects(batch)
            except BaseException as error:
                survivors = sorted(file_keys[deleted_files:])
                message = (
                    f"Partially removed directory tree {key}: deleted {deleted_files} of "
                    f"{len(file_keys)} files; {len(survivors)} remain: {survivors}"
                )
                raise OSError(message) from error
            deleted_files += len(batch)

        # 自底向上删除目录标记对象
        deleted_dirs = 0
        for dir_path in reversed(dir_paths):
            dir_key = self._dir_key(dir_path)
            if dir_key is not None and await client.head_object(key=dir_key) is not None:
                await client.delete_object(key=dir_key)
                deleted_dirs += 1

        # 删除根路径自身的标记对象
        root_dir_key = self._dir_key(path)
        if root_dir_key is not None and await client.head_object(key=root_dir_key) is not None:
            await client.delete_object(key=root_dir_key)
            deleted_dirs += 1

        self.log.info(
            f"RmTree complete: <y>{escape_tag(key)}</y> (<g>{deleted_files}</g> files, <g>{deleted_dirs}</g> dirs)"
        )

    async def _cleanup_copytree_backups(self, client: AsyncS3Client, backup_keys: Iterable[str]) -> None:
        pending = set(backup_keys)
        if not pending:
            return

        last_error: BaseException | None = None
        with anyio.CancelScope(shield=True):
            for _attempt in range(2):
                try:
                    await client.delete_objects(pending)
                except BaseException as exc:
                    last_error = exc

                remaining: set[str] = set()
                for backup_key in pending:
                    try:
                        if await client.head_object(key=backup_key) is not None:
                            remaining.add(backup_key)
                    except BaseException as exc:
                        last_error = exc
                        remaining.add(backup_key)
                if not remaining:
                    return
                pending = remaining

        message = f"Failed to remove copytree backups: {sorted(pending)}"
        if last_error is None:
            raise OSError(message)
        raise OSError(message) from last_error

    async def _stage_copytree_targets(
        self, client: AsyncS3Client, target_keys: set[str]
    ) -> tuple[dict[str, str], set[str]]:
        backups: dict[str, str] = {}
        created_targets: set[str] = set()
        backup_prefix = f".storegate-copytree-backup-{uuid.uuid4().hex}/"
        try:
            for target_key in sorted(target_keys):
                if await client.head_object(key=target_key) is None:
                    created_targets.add(target_key)
                else:
                    backup_key = f"{backup_prefix}{target_key}"
                    await client.put_object_copy(target_key, backup_key)
                    backups[target_key] = backup_key
        except BaseException:
            try:
                await self._cleanup_copytree_backups(client, backups.values())
            except BaseException as cleanup_exc:
                raise OSError(f"Failed to clean staged copytree backups: {cleanup_exc}") from cleanup_exc
            raise
        return backups, created_targets

    async def _rollback_copytree_targets(
        self,
        client: AsyncS3Client,
        backups: dict[str, str],
        created_targets: set[str],
    ) -> None:
        errors: list[str] = []
        first_error: BaseException | None = None
        restored_backup_keys: set[str] = set()
        with anyio.CancelScope(shield=True):
            if created_targets:
                try:
                    await client.delete_objects(created_targets)
                except BaseException as exc:
                    errors.append(f"Failed to remove newly created copytree targets: {exc}")
                    first_error = exc
            for target_key, backup_key in backups.items():
                try:
                    await client.put_object_copy(backup_key, target_key)
                except BaseException as exc:
                    errors.append(f"Failed to restore copytree target {target_key}: {exc}")
                    if first_error is None:
                        first_error = exc
                else:
                    restored_backup_keys.add(backup_key)
            try:
                await self._cleanup_copytree_backups(client, restored_backup_keys)
            except BaseException as exc:
                errors.append(f"Failed to clean restored copytree backups: {exc}")
                if first_error is None:
                    first_error = exc
        if errors:
            message = "Failed to roll back copytree: " + "; ".join(errors)
            if first_error is None:
                raise OSError(message)
            raise OSError(message) from first_error

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            source_info = await self.stat(src)
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = source_info.kind
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
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        client = self._ensure_client()
        # walk 收集源树所有文件和目录
        file_paths: list[PurePosixPath] = []
        dir_paths: list[PurePosixPath] = []
        async for walked in self.walk(src):
            file_paths.extend(
                self.normalize_path(entry.path) for entry in walked.entries if entry.kind is EntryKind.FILE
            )
            dir_paths.extend(
                self.normalize_path(entry.path) for entry in walked.entries if entry.kind is EntryKind.DIRECTORY
            )

        self.log.info(
            f"CopyTree: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
            f"(<g>{len(file_paths)}</g> files, <g>{len(dir_paths)}</g> subdirs)"
        )

        # 创建目标端所有目录标记 (按深度排序, 自上而下)
        all_dst_dirs: set[PurePosixPath] = {dst}
        for dir_path in dir_paths:
            all_dst_dirs.add(dst.joinpath(dir_path.relative_to(src)))
        for src_file in file_paths:
            all_dst_dirs.add(dst.joinpath(src_file.relative_to(src)).parent)

        dir_keys = {dir_key for directory in all_dst_dirs if (dir_key := self._dir_key(directory)) is not None}
        file_keys = {self._remote_path_to_key(dst.joinpath(src_file.relative_to(src))) for src_file in file_paths}
        backups, created_targets = await self._stage_copytree_targets(client, dir_keys | file_keys)

        dir_created = datetime.now(UTC)

        try:
            for directory in sorted(all_dst_dirs, key=lambda path: len(path.parts)):
                if dir_key := self._dir_key(directory):
                    info = FileInfo(
                        path=directory.as_posix(),
                        name=directory.name,
                        kind=EntryKind.DIRECTORY,
                        size=0,
                        modified=dir_created,
                        created=dir_created,
                    )
                    await client.put_object(key=dir_key, data=serialize_file_info(info))

            async def copy_worker(
                paths: AsyncIterable[tuple[PurePosixPath, PurePosixPath]],
            ) -> None:
                async for src_file, dst_file in paths:
                    await self.copy(src_file, dst_file)

            send, recv = anyio.create_memory_object_stream[tuple[PurePosixPath, PurePosixPath]](COPYTREE_MAX_WORKERS)
            async with anyio.create_task_group() as tg, send:
                for _ in range(COPYTREE_MAX_WORKERS):
                    tg.start_soon(copy_worker, recv.clone())
                recv.close()
                for src_file in file_paths:
                    await send.send((src_file, dst.joinpath(src_file.relative_to(src))))
        except BaseException as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to copy tree: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
                f"— <r>{escape_tag(repr(exc))}</r>"
            )
            try:
                await self._rollback_copytree_targets(client, backups, created_targets)
            except BaseException as rollback_exc:
                if isinstance(exc, Exception) and isinstance(rollback_exc, Exception):
                    raise BaseExceptionGroup(
                        f"Failed to copy tree: {src} → {dst}; rollback also failed",
                        [exc, rollback_exc],
                    ) from None
                if isinstance(rollback_exc, anyio.get_cancelled_exc_class()):
                    raise
                raise exc from None
            if isinstance(exc, Exception):
                raise OSError(f"Failed to copy tree: {src} → {dst}") from exc
            raise

        await self._cleanup_copytree_backups(client, backups.values())

    @override
    @translator.wrap("Failed to move tree {src} → {dst} (overwrite={overwrite})")
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            source_info = await self.stat(src)
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = source_info.kind
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
        await self.copytree(src, dst, overwrite=overwrite)
        await self.rmtree(src)
