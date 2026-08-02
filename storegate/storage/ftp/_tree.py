from __future__ import annotations

import uuid
from pathlib import PurePosixPath
from typing import override

import aioftp
import anyio

from storegate.log import escape_tag
from storegate.storage.abstract import (
    EntryKind,
    PathLike,
    UnsupportedOperationError,
    WalkEntry,
    validate_same_path_file_operation,
    validate_same_path_tree_operation,
)

from ._base import _UNSUPPORTED_ERRNO, FTPStorageBase, translator


class FTPTreeMixin(FTPStorageBase):
    """Whole-tree copy/move plus the single-file copy they share.

    ``_copytree`` records every destination file it creates and backs up every
    file it is about to overwrite, so any failure rolls the destination back to
    its pre-copy state before re-raising primary-first.
    """

    async def _copy_stream(
        self,
        source_client: aioftp.Client,
        destination_client: aioftp.Client,
        source: PurePosixPath,
        destination: PurePosixPath,
    ) -> None:
        reader = await source_client.download_stream(self._remote_path(source))
        try:
            async with destination_client.upload_stream(self._remote_path(destination)) as writer:
                async for chunk in reader.iter_by_block(self._config.chunk_size):
                    await writer.write(chunk)
        finally:
            with anyio.CancelScope(shield=True):
                await reader.finish()

    async def _copy_file(
        self,
        source_client: aioftp.Client,
        destination_client: aioftp.Client,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool = True,
    ) -> bool:
        source_info = await self._stat(source_client, source)
        if source_info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {source.as_posix()}")
        if source_info.kind is not EntryKind.FILE:
            raise UnsupportedOperationError(
                _UNSUPPORTED_ERRNO, f"Unsupported FTP copy source kind: {source.as_posix()}"
            )

        destination_existed = False
        try:
            destination_info = await self._stat(source_client, destination)
        except FileNotFoundError:
            pass
        else:
            destination_existed = True
            if destination_info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {destination.as_posix()}")
            if destination_info.kind is not EntryKind.FILE:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copy destination kind: {destination.as_posix()}"
                )
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")

        await self._mkdir(source_client, destination.parent, parents=True, exist_ok=True)
        await self._copy_stream(source_client, destination_client, source, destination)
        return not destination_existed

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"Copy: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        async with self._client_lease() as lease:
            source_info = await self._stat(lease.client, source)
            if validate_same_path_file_operation(
                source,
                destination,
                source_kind=source_info.kind,
                overwrite=overwrite,
            ):
                return
            if source_info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {source.as_posix()}")
            if source_info.kind is not EntryKind.FILE:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copy source kind: {source.as_posix()}"
                )
            try:
                destination_info = await self._stat(lease.client, destination)
            except FileNotFoundError:
                destination_existed = False
            else:
                destination_existed = True
                if destination_info.kind is EntryKind.DIRECTORY:
                    raise IsADirectoryError(f"Destination is a directory: {destination.as_posix()}")
                if destination_info.kind is not EntryKind.FILE:
                    raise UnsupportedOperationError(
                        _UNSUPPORTED_ERRNO, f"Unsupported FTP copy destination kind: {destination.as_posix()}"
                    )
            try:
                async with self._temporary_client() as destination_client:
                    await self._copy_file(lease.client, destination_client, source, destination, overwrite=overwrite)
            except BaseException:
                if not destination_existed:
                    await self._cleanup_partial_file(destination)
                raise

    async def _rollback_copytree(
        self,
        client: aioftp.Client,
        created_files: set[PurePosixPath],
        created_dirs: set[PurePosixPath],
        backups: dict[PurePosixPath, PurePosixPath],
    ) -> None:
        failures: list[BaseException] = []
        for file in sorted(created_files, key=lambda path: len(path.parts), reverse=True):
            try:
                if await self._exists(client, file):
                    await client.remove_file(self._remote_path(file))
            except BaseException as error:
                failures.append(error)
        for target, backup in backups.items():
            try:
                if await self._exists(client, target):
                    await client.remove_file(self._remote_path(target))
                if await self._exists(client, backup):
                    await client.rename(self._remote_path(backup), self._remote_path(target))
            except BaseException as error:
                failures.append(error)
        for directory in sorted(created_dirs, key=lambda path: len(path.parts), reverse=True):
            try:
                if await self._exists(client, directory):
                    await client.remove_directory(self._remote_path(directory))
            except BaseException as error:
                failures.append(error)
        if not failures:
            return
        if len(failures) == 1:
            raise failures[0]
        raise BaseExceptionGroup("Failed to roll back FTP copytree", failures)

    async def _rollback_copytree_with_fallback(
        self,
        source_client: aioftp.Client,
        created_files: set[PurePosixPath],
        created_dirs: set[PurePosixPath],
        backups: dict[PurePosixPath, PurePosixPath],
    ) -> None:
        try:
            await self._rollback_copytree(source_client, created_files, created_dirs, backups)
        except BaseException as first_error:
            try:
                async with self._temporary_client() as cleanup_client:
                    await self._rollback_copytree(cleanup_client, created_files, created_dirs, backups)
            except BaseException as second_error:
                if isinstance(first_error, Exception) and isinstance(second_error, Exception):
                    raise BaseExceptionGroup("FTP copytree rollback failed", [first_error, second_error]) from None
                raise first_error from None

    async def _preflight_copytree_destination(
        self,
        client: aioftp.Client,
        snapshot: list[WalkEntry],
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> None:
        for walk_entry in snapshot:
            current = PurePosixPath(walk_entry.path)
            relative = current.relative_to(source)
            target_dir = destination if relative == PurePosixPath(".") else destination / relative
            for entry in walk_entry.entries:
                target = target_dir / entry.name
                try:
                    target_info = await self._stat(client, target)
                except FileNotFoundError:
                    continue
                if entry.kind is EntryKind.DIRECTORY:
                    if target_info.kind is not EntryKind.DIRECTORY:
                        raise FileExistsError(f"Destination is not a directory: {target.as_posix()}")
                    continue
                if entry.kind is EntryKind.FILE:
                    if target_info.kind is EntryKind.DIRECTORY:
                        raise IsADirectoryError(f"Destination is a directory: {target.as_posix()}")
                    if target_info.kind is not EntryKind.FILE:
                        raise UnsupportedOperationError(
                            _UNSUPPORTED_ERRNO,
                            f"Unsupported FTP copytree destination kind: {target.as_posix()}",
                        )
                    if not overwrite:
                        raise FileExistsError(f"Destination already exists: {target.as_posix()}")
                    continue
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copytree source kind: {entry.path}"
                )

    async def _copytree(
        self,
        source_client: aioftp.Client,
        destination_client: aioftp.Client,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> None:
        try:
            source_info = await self._stat(source_client, source)
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = source_info.kind
        if validate_same_path_tree_operation(
            source,
            destination,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {source.as_posix()}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")

        snapshot = await self._walk_snapshot(source_client, source, strict=True)
        try:
            destination_info = await self._stat(source_client, destination)
        except FileNotFoundError:
            destination_info = None
        if destination_info is not None:
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if destination_info.kind is EntryKind.FILE:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")
            if destination_info.kind is not EntryKind.DIRECTORY:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP copytree destination kind: {destination.as_posix()}"
                )
        await self._preflight_copytree_destination(
            source_client,
            snapshot,
            source,
            destination,
            overwrite=overwrite,
        )

        created_files: set[PurePosixPath] = set()
        created_dirs: set[PurePosixPath] = set()
        backups: dict[PurePosixPath, PurePosixPath] = {}
        try:
            created_dirs.update(await self._mkdir(source_client, destination, parents=True, exist_ok=True))
            for walk_entry in snapshot:
                current = PurePosixPath(walk_entry.path)
                relative = current.relative_to(source)
                target_dir = destination if relative == PurePosixPath(".") else destination / relative
                for entry in walk_entry.entries:
                    target = target_dir / entry.name
                    if entry.kind is EntryKind.DIRECTORY:
                        created_dirs.update(await self._mkdir(source_client, target, parents=False, exist_ok=True))
                        continue
                    if entry.kind is not EntryKind.FILE:
                        raise UnsupportedOperationError(
                            _UNSUPPORTED_ERRNO, f"Unsupported FTP copytree source kind: {entry.path}"
                        )
                    if await self._exists(source_client, target):
                        backup = target.with_name(f".storegate-copytree-{uuid.uuid4().hex}-{target.name}")
                        await source_client.rename(self._remote_path(target), self._remote_path(backup))
                        backups[target] = backup
                    else:
                        created_files.add(target)
                    await self._copy_file(source_client, destination_client, PurePosixPath(entry.path), target)
        except BaseException as primary:
            rollback_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await self._rollback_copytree_with_fallback(source_client, created_files, created_dirs, backups)
                except BaseException as error:
                    rollback_error = error
            if rollback_error is not None and isinstance(primary, Exception) and isinstance(rollback_error, Exception):
                raise BaseExceptionGroup("FTP copytree and rollback failed", [primary, rollback_error]) from None
            if rollback_error is not None and isinstance(rollback_error, anyio.get_cancelled_exc_class()):
                raise rollback_error from None
            raise
        cleanup_errors: list[BaseException] = []
        with anyio.CancelScope(shield=True):
            for backup in backups.values():
                try:
                    await source_client.remove_file(self._remote_path(backup))
                except BaseException as error:
                    cleanup_errors.append(error)
        if cleanup_errors:
            if len(cleanup_errors) == 1:
                raise cleanup_errors[0]
            raise BaseExceptionGroup("Failed to clean FTP copytree backups", cleanup_errors)

    @override
    @translator.wrap("Failed to copy tree {src} → {dst} (overwrite={overwrite})")
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"CopyTree: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        async with self._client_lease() as lease, self._temporary_client() as destination_client:
            await self._copytree(lease.client, destination_client, source, destination, overwrite=overwrite)

    @override
    @translator.wrap("Failed to move tree {src} → {dst} (overwrite={overwrite})")
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"MoveTree: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")
        async with self._client_lease() as lease:
            try:
                source_info = await self._stat(lease.client, source)
            except FileNotFoundError:
                source_kind = None
            else:
                source_kind = source_info.kind
            if validate_same_path_tree_operation(
                source,
                destination,
                source_kind=source_kind,
                overwrite=overwrite,
            ):
                return
            if source_kind is None:
                raise FileNotFoundError(f"Source not found: {source.as_posix()}")
            if source_kind is not EntryKind.DIRECTORY:
                raise NotADirectoryError(f"Not a directory: {source.as_posix()}")
            if source == PurePosixPath("/"):
                raise OSError("Cannot move root directory")
            if self._is_descendant(destination, source):
                raise ValueError("Destination must not be inside the source tree")

            try:
                destination_info = await self._stat(lease.client, destination)
            except FileNotFoundError:
                destination_info = None

            if destination_info is None:
                await self._walk_snapshot(lease.client, source, strict=True)
                await self._mkdir(lease.client, destination.parent, parents=True, exist_ok=True)
                await lease.client.rename(self._remote_path(source), self._remote_path(destination))
                return
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {destination.as_posix()}")
            if destination_info.kind is EntryKind.FILE:
                raise FileExistsError(f"Destination is a file: {destination.as_posix()}")
            if destination_info.kind is not EntryKind.DIRECTORY:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP movetree destination kind: {destination.as_posix()}"
                )

            async with self._temporary_client() as destination_client:
                await self._copytree(lease.client, destination_client, source, destination, overwrite=True)
            await self._rmtree(lease.client, source)
