import uuid
from typing import override

import anyio

from storegate.storage.abstract import (
    EntryKind,
    PathLike,
    validate_same_path_file_operation,
)

from ._base import (
    AsyncDavClient,
    DavHttpStatusError,
    DavStorageBase,
    _unsupported_entry,
    translator,
)


class DavMoveMixin(DavStorageBase):
    """Single-entry move/copy with destination staging and reconciliation.

    ``move`` stages an existing destination aside under a backup name before
    retrying, then either cleans the backup up or hands the ambiguous outcome to
    ``_reconcile_failed_move``, which re-probes and restores both paths.
    """

    @override
    @translator.wrap("Failed to move {src} → {dst} (overwrite={overwrite})")
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_np = self.normalize_path(src)
        dst_np = self.normalize_path(dst)
        src_rel = self._remote_path(src_np)
        dst_rel = self._remote_path(dst_np)
        if src_rel == "" and src_np != dst_np:
            raise OSError(f"Cannot move root: {src}")
        try:
            source_info = await self.stat(src)
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = source_info.kind
        if validate_same_path_file_operation(
            src_np,
            dst_np,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")
        if source_kind is not EntryKind.FILE:
            raise _unsupported_entry(src)

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
        src_np = self.normalize_path(src)
        dst_np = self.normalize_path(dst)
        src_rel = self._remote_path(src_np)
        dst_rel = self._remote_path(dst_np)
        if src_rel == "":
            raise IsADirectoryError(f"Cannot copy root: {src}")
        try:
            source_info = await self.stat(src)
        except FileNotFoundError:
            source_kind = None
        else:
            source_kind = source_info.kind
        if validate_same_path_file_operation(
            src_np,
            dst_np,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")
        if source_kind is not EntryKind.FILE:
            raise _unsupported_entry(src)
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
