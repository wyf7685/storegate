import uuid
from pathlib import PurePosixPath
from typing import override

import anyio

from storegate.log import escape_tag
from storegate.storage.abstract import (
    EntryKind,
    PathLike,
    UnsupportedOperationError,
    validate_same_path_file_operation,
)

from ._base import _UNSUPPORTED_ERRNO, FTPStorageBase, translator


class FTPMoveMixin(FTPStorageBase):
    """Single-file move staged through a temporary backup of the destination.

    A rename that fails after the destination was staged away leaves the pair in
    an unknown state; ``_move_backup_exists`` probes it and
    ``_reconcile_move_failure`` restores whichever side actually survived.
    """

    async def _reconcile_move_failure(
        self,
        source: PurePosixPath,
        destination: PurePosixPath,
        temporary: PurePosixPath,
        destination_existed: bool,
    ) -> None:
        """Restore source/destination from the actual state after a failed rename."""
        with anyio.CancelScope(shield=True):
            try:
                async with self._client_lease() as lease:

                    async def _exists(path: PurePosixPath) -> bool:
                        try:
                            await self._stat(lease.client, path)
                        except FileNotFoundError:
                            return False
                        return True

                    source_exists = await _exists(source)
                    destination_exists = await _exists(destination)
                    temporary_exists = await _exists(temporary)

                    if not source_exists and destination_exists:
                        await lease.client.rename(self._remote_path(destination), self._remote_path(source))
                        destination_exists = False
                    if destination_existed and temporary_exists and not destination_exists:
                        await lease.client.rename(self._remote_path(temporary), self._remote_path(destination))
                        temporary_exists = False
                    elif not destination_existed and temporary_exists:
                        await lease.client.remove_file(self._remote_path(temporary))
                        temporary_exists = False
                    if temporary_exists:
                        await lease.client.remove_file(self._remote_path(temporary))
            except BaseException:
                self.log.exception(f"Failed to reconcile move state: {source} → {destination}")

    async def _move_backup_exists(self, temporary: PurePosixPath) -> bool | None:
        with anyio.CancelScope(shield=True):
            try:
                async with self._client_lease() as lease:
                    return await self._exists(lease.client, temporary)
            except BaseException:
                self.log.exception(f"Failed to inspect move backup: {temporary}")
                return None

    @override
    @translator.wrap("Failed to move {src} → {dst}")
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._logical_path(src)
        destination = self._logical_path(dst)
        self.log.info(f"Move: <y>{escape_tag(source.as_posix())}</y> → <y>{escape_tag(destination.as_posix())}</y>")

        failure: BaseException | None = None
        temporary: PurePosixPath | None = None
        destination_existed = False
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
                    _UNSUPPORTED_ERRNO, f"Unsupported FTP move source kind: {source.as_posix()}"
                )
            try:
                destination_info = await self._stat(lease.client, destination)
            except FileNotFoundError:
                pass
            else:
                destination_existed = True
                if destination_info.kind is EntryKind.DIRECTORY:
                    raise IsADirectoryError(f"Destination is a directory: {destination.as_posix()}")
                if destination_info.kind is not EntryKind.FILE:
                    raise UnsupportedOperationError(
                        _UNSUPPORTED_ERRNO, f"Unsupported FTP move destination kind: {destination.as_posix()}"
                    )
                if not overwrite:
                    raise FileExistsError(f"Destination already exists: {destination.as_posix()}")

            await self._mkdir(lease.client, destination.parent, parents=True, exist_ok=True)
            temporary = destination.parent / f".storegate-move-{uuid.uuid4().hex}"
            staged = False
            try:
                if destination_existed:
                    await lease.client.rename(self._remote_path(destination), self._remote_path(temporary))
                    staged = True
                await lease.client.rename(self._remote_path(source), self._remote_path(destination))
            except BaseException as exc:
                lease.invalidate()
                failure = exc
            else:
                if staged:
                    try:
                        with anyio.CancelScope(shield=True):
                            await lease.client.remove_file(self._remote_path(temporary))
                    except BaseException as exc:
                        lease.invalidate()
                        failure = exc

        if failure is not None:
            assert temporary is not None
            backup_exists = await self._move_backup_exists(temporary)
            if backup_exists is True:
                await self._reconcile_move_failure(source, destination, temporary, destination_existed)
                raise failure
            if backup_exists is None:
                # The probe itself failed, so reconcile cannot run: the previous
                # destination content may survive only under the temporary name.
                # Say so -- otherwise the caller sees a plain move failure and has
                # no idea a backup is stranded.
                self.log.error(
                    f"Could not determine move backup state for <y>{escape_tag(temporary)}</y>; "
                    f"destination <y>{escape_tag(destination)}</y> may survive only under that "
                    f"temporary name and was not reconciled"
                )
                raise failure
            self.log.warning(f"Move backup cleanup response lost after commit: {temporary}")
