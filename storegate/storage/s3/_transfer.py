import uuid
from typing import override

import anyio
import anyio.lowlevel

from storegate.log import escape_tag
from storegate.storage.abstract import (
    EntryKind,
    PathLike,
    validate_same_path_file_operation,
)

from ._base import COPY_MULTIPART_THRESHOLD, UPLOAD_CHUNK_SIZE, S3StorageBase, translator
from .client import AsyncS3Client, CompletedPart


class S3TransferMixin(S3StorageBase):
    """Single-object move and copy, including the multipart server-side copy."""

    async def _rollback_move_destination(
        self,
        client: AsyncS3Client,
        src_key: str,
        dst_key: str,
        backup_key: str | None,
        *,
        copy_completed: bool,
    ) -> None:
        """Restore a move's source and destination, preserving rollback failures."""
        failures: list[BaseException] = []
        with anyio.CancelScope(shield=True):
            if copy_completed:
                try:
                    if await client.head_object(key=src_key) is None:
                        await client.put_object_copy(dst_key, src_key)
                except BaseException as error:
                    failures.append(error)
            try:
                if backup_key is not None:
                    await client.put_object_copy(backup_key, dst_key)
                    await client.delete_object(key=backup_key)
                elif copy_completed:
                    await client.delete_object(key=dst_key)
            except BaseException as error:
                failures.append(error)
        if not failures:
            return
        if len(failures) == 1:
            raise failures[0]
        raise BaseExceptionGroup("Failed to roll back move destination", failures)

    @staticmethod
    def _raise_move_failure(primary: BaseException, rollback_error: BaseException | None) -> None:
        if rollback_error is None:
            raise primary
        if isinstance(primary, Exception) and isinstance(rollback_error, Exception):
            raise BaseExceptionGroup("Move and rollback failed", [primary, rollback_error]) from None
        if isinstance(rollback_error, anyio.get_cancelled_exc_class()):
            raise rollback_error
        raise primary

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            source_info = await self.stat(src)
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
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")

        src_key = self._remote_path_to_key(src)
        dst_key = self._remote_path_to_key(dst)
        client = self._ensure_client()
        if await self.is_dir(dst):
            raise IsADirectoryError(f"Destination is a directory: {dst}")
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        backup_key: str | None = None
        if overwrite and await client.head_object(key=dst_key) is not None:
            backup_key = f"{dst_key}.storegate-move-backup-{uuid.uuid4().hex}"
            await client.put_object_copy(dst_key, backup_key)

        try:
            await self.copy(src, dst, overwrite=overwrite)
        except BaseException as primary:
            rollback_error: BaseException | None = None
            if backup_key is not None:
                try:
                    await self._rollback_move_destination(client, src_key, dst_key, backup_key, copy_completed=False)
                except BaseException as error:
                    rollback_error = error
            self._raise_move_failure(primary, rollback_error)

        try:
            await client.delete_object(key=src_key)
        except BaseException as error:
            rollback_error: BaseException | None = None
            try:
                await self._rollback_move_destination(client, src_key, dst_key, backup_key, copy_completed=True)
            except BaseException as rollback_failure:
                rollback_error = rollback_failure
            primary = OSError(f"Failed to delete source after move: {src}")
            primary.__cause__ = error
            self._raise_move_failure(primary, rollback_error)

        if backup_key is not None:
            with anyio.CancelScope(shield=True):
                try:
                    await client.delete_object(key=backup_key)
                except BaseException:
                    self.log.exception(f"Failed to delete move backup: {backup_key}")

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            source_info = await self.stat(src)
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
            raise FileNotFoundError(f"Source not found: {src}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")

        src_key = self._remote_path_to_key(src)
        dst_key = self._remote_path_to_key(dst)
        client = self._ensure_client()
        head = await client.head_object(key=src_key)
        if head is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if await self.is_dir(dst):
            raise IsADirectoryError(f"Destination is a directory: {dst}")
        if not overwrite and await client.head_object(key=dst_key) is not None:
            raise FileExistsError(f"Destination already exists: {dst}")
        if head.content_length <= COPY_MULTIPART_THRESHOLD:
            await client.put_object_copy(src_key, dst_key)
        else:
            await self._copy_multipart(src_key, dst_key, head.content_length)

    async def _copy_multipart(self, src_key: str, dst_key: str, src_size: int) -> None:
        """Server-side copy via multipart upload for objects above the threshold."""
        client = self._ensure_client()
        num_parts = (src_size + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE
        self.log.debug(
            f"Multipart copy: <y>{escape_tag(src_key)}</y> → <y>{escape_tag(dst_key)}</y> "
            f"(<g>{src_size}</g> bytes in <g>{num_parts}</g> parts)"
        )
        try:
            upload_id = await client.create_multipart_upload(dst_key)
        except Exception as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to create multipart upload: <y>{escape_tag(dst_key)}</y> — <r>{escape_tag(repr(exc))}</r>"
            )
            raise OSError(f"Failed to create multipart upload: {dst_key}") from exc

        try:
            parts: list[CompletedPart] = []
            offset = 0
            part_number = 1

            while offset < src_size:
                end = min(offset + UPLOAD_CHUNK_SIZE - 1, src_size - 1)
                result = await client.upload_part_copy(
                    source_key=src_key,
                    target_key=dst_key,
                    upload_id=upload_id,
                    part_number=part_number,
                    byte_range=(offset, end),
                )
                parts.append({"PartNumber": part_number, "ETag": result.etag})
                offset = end + 1
                part_number += 1
                await anyio.lowlevel.checkpoint()

            await client.complete_multipart_upload(dst_key, upload_id, parts)
            self.log.debug(f"Multipart copy complete: <y>{escape_tag(dst_key)}</y>")
        except Exception as exc:
            try:
                with anyio.CancelScope(shield=True):
                    await client.abort_multipart_upload(dst_key, upload_id)
            except Exception:
                self.log.exception(f"Failed to abort multipart upload after failed copy: <y>{escape_tag(dst_key)}</y>")
            raise OSError(f"Failed to copy object: {src_key} → {dst_key}") from exc
