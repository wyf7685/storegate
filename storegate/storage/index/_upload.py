import hashlib
from collections.abc import AsyncIterable
from datetime import UTC, datetime
from typing import NoReturn, override

import anyio
import anyio.lowlevel
from anyio.streams.memory import MemoryObjectReceiveStream

from storegate.log import escape_tag

from ..abstract import BytesLike, EntryKind, FileInfo, PathLike
from ._base import IndexStorageBase
from ._guard import download_private_file, lstat_private_entry_or_none
from .models import FileMeta
from .ref import hash_to_path


class IndexUploadMixin(IndexStorageBase):
    """Chunked upload transaction: staging, commit and rollback.

    Every mutation performed before the metadata write is recorded so that a
    failure or cancellation can undo it: ``newly_added_refs`` for chunk
    references this upload created, ``staged_bins`` for chunk payloads it wrote,
    and ``guards`` for rollback guards pinning the chunks of the previous
    revision. The metadata write is the commit point.
    """

    async def _save_chunk_worker(
        self,
        recv: MemoryObjectReceiveStream[tuple[str, bytes, PathLike]],
        newly_added_refs: set[str],
        staged_bins: set[str],
    ) -> None:
        """Worker: pull blocks from channel, save or reuse existing chunks."""
        async for chunk_hash, data, remote_path in recv:
            bin_path = hash_to_path(chunk_hash, "bin")

            async with self._lock_chunk(chunk_hash):
                chunk_info = await lstat_private_entry_or_none(self._chunks, bin_path, label="chunk data")
                if chunk_info is None:
                    # Record before the cancellable upload so commit-before-return still cleans up.
                    staged_bins.add(chunk_hash)
                    start = anyio.current_time()
                    await self._chunks.upload_bytes(data, bin_path)
                    elapsed = anyio.current_time() - start
                    self.log.debug(
                        f"Chunk <c>{chunk_hash[:8]}</c> uploaded (<g>{len(data)}</g> bytes, <g>{elapsed:.2f}</g> s)"
                    )
                elif chunk_info.kind is EntryKind.DIRECTORY:
                    raise IsADirectoryError(f"Chunk data path is a directory: {bin_path}")
                else:
                    self.log.debug(f"Chunk <c>{chunk_hash[:8]}</c> already exists, skipping upload")
                if await self._refs.incref(chunk_hash, remote_path):
                    newly_added_refs.add(chunk_hash)

    @staticmethod
    def _raise_upload_failure(primary: BaseException, cleanup_errors: list[BaseException]) -> NoReturn:
        if not cleanup_errors:
            raise primary
        raise BaseExceptionGroup("Index upload and rollback failed", [primary, *cleanup_errors]) from None

    async def _cleanup_staged_bins(self, staged_bins: set[str]) -> None:
        for chunk_hash in staged_bins:
            refs = await self._refs.load_refs(chunk_hash)
            if refs:
                continue
            bin_path = hash_to_path(chunk_hash, "bin")
            info = await lstat_private_entry_or_none(self._chunks, bin_path, label="chunk data")
            if info is None:
                continue
            if info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Chunk data path is a directory: {bin_path}")
            await self._chunks.unlink(bin_path)

    async def _rollback_upload_transaction(
        self,
        remote_path: PathLike,
        newly_added_refs: set[str],
        staged_bins: set[str],
        guards: dict[str, str],
    ) -> list[BaseException]:
        cleanup_errors: list[BaseException] = []
        with anyio.CancelScope(shield=True):
            if newly_added_refs:
                try:
                    async with self._lock_chunks(newly_added_refs), anyio.create_task_group() as tg:
                        for chunk_hash in newly_added_refs:
                            tg.start_soon(self._refs.decref, chunk_hash, remote_path)
                except BaseException as error:
                    cleanup_errors.append(error)
            if staged_bins:
                try:
                    async with self._lock_chunks(staged_bins):
                        await self._cleanup_staged_bins(staged_bins)
                except BaseException as error:
                    cleanup_errors.append(error)
            if guards:
                try:
                    async with self._lock_chunks(guards):
                        await self._refs.release_rollback_guards(guards)
                except BaseException as error:
                    cleanup_errors.append(error)
        return cleanup_errors

    async def _release_upload_guards(
        self,
        guards: dict[str, str],
    ) -> None:
        if not guards:
            return
        async with self._lock_chunks(guards):
            await self._refs.release_rollback_guards(guards)
        guards.clear()

    async def _metadata_matches_upload(self, remote_path: PathLike, expected: bytes) -> bool:
        try:
            current = await download_private_file(self._index, remote_path, label="metadata entry")
        except FileNotFoundError:
            return False
        return current == expected

    async def _post_commit_upload_cleanup(
        self,
        remote_path: PathLike,
        *,
        old_only: set[str],
        guards: dict[str, str],
    ) -> None:
        """Remove old-only refs and always attempt guard release afterward."""
        primary: BaseException | None = None
        cleanup_errors: list[BaseException] = []

        if old_only:
            try:
                async with self._lock_chunks(old_only), anyio.create_task_group() as tg:
                    for chunk_hash in old_only:
                        tg.start_soon(self._refs.decref, chunk_hash, remote_path)
            except BaseException as error:
                primary = error

        if guards:
            try:
                await self._release_upload_guards(guards)
            except BaseException as error:
                if primary is None:
                    primary = error
                else:
                    cleanup_errors.append(error)

        if primary is not None:
            self._raise_upload_failure(primary, cleanup_errors)

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        self._reject_reserved(remote_path)
        remote_path = self.normalize_path(remote_path)

        try:
            info = await self.stat(remote_path)
        except FileNotFoundError:
            pass
        else:
            if info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            if not overwrite:
                raise FileExistsError(f"File already exists: {remote_path}")

        _colored_path = f"<y>{escape_tag(remote_path)}</y>"
        self.log.info(f"Upload starting: {_colored_path}")

        chunk_hashes: list[str] = []
        total_size = 0
        newly_added_refs: set[str] = set()
        staged_bins: set[str] = set()
        guards: dict[str, str] = {}
        max_workers = self._max_concurrent_uploads

        async with self._lock_index(remote_path):
            # load old metadata
            old_meta = await self._get_file_meta(remote_path)
            old_hashes = set(old_meta.chunks) if old_meta is not None else set()

            # protect old chunks before any mutation that could drop them
            if old_hashes:
                async with self._lock_chunks(old_hashes):
                    guards = await self._refs.add_rollback_guards(old_hashes)

            send, recv = anyio.create_memory_object_stream[tuple[str, bytes, PathLike]](max_workers * 2)

            try:
                async with anyio.create_task_group() as tg, send:
                    for worker_idx in range(max_workers):
                        self.log.debug(f"Starting chunk upload worker #{worker_idx + 1}")
                        tg.start_soon(self._save_chunk_worker, recv.clone(), newly_added_refs, staged_bins)
                    recv.close()

                    buffer = bytearray()
                    hasher = hashlib.sha256()

                    async for chunk in stream:
                        total_size += len(chunk)

                        # 若未达阈值：缓冲并继续
                        if len(buffer) + len(chunk) < self._block_size:
                            buffer.extend(chunk)
                            hasher.update(chunk)
                            continue

                        # 当前 chunk 跨越 block_size 边界，需要拆分
                        chunk_mv = memoryview(chunk)
                        offset = 0
                        while offset < len(chunk_mv):
                            remaining = self._block_size - len(buffer)
                            take = min(remaining, len(chunk_mv) - offset)

                            hasher.update(chunk_mv[offset : offset + take])
                            buffer.extend(chunk_mv[offset : offset + take])
                            offset += take

                            # buffer 恰好填满一个 block
                            if len(buffer) == self._block_size:
                                chunk_hash = hasher.hexdigest()
                                chunk_hashes.append(chunk_hash)

                                self.log.debug(
                                    f"Chunk #{len(chunk_hashes)} <c>{chunk_hash[:8]}</c> "
                                    f"received for {_colored_path}"
                                    f" (<g>{self._block_size}</g> bytes)"
                                )
                                # channel 满时阻塞 → 反压输入流
                                await send.send(
                                    (chunk_hash, bytes(buffer), remote_path),
                                )

                                buffer.clear()
                                hasher = hashlib.sha256()

                    # 最后一块
                    if buffer:
                        chunk_hash = hasher.hexdigest()
                        chunk_hashes.append(chunk_hash)

                        self.log.debug(
                            f"Chunk #{len(chunk_hashes)} <c>{chunk_hash[:8]}</c> "
                            f"received for {_colored_path}"
                            f" (<g>{len(buffer)}</g> bytes)"
                        )
                        await send.send(
                            (chunk_hash, bytes(buffer), remote_path),
                        )

                # send 关闭 → worker 退出 → tg 退出 → 所有上传完成
                self.log.debug(f"All chunk upload workers completed for {_colored_path}")
            except BaseException as primary:
                self.log.error(  # noqa: TRY400
                    f"Upload failed: {_colored_path} "
                    f"(<g>{total_size}</g> bytes streamed, "
                    f"<g>{len(chunk_hashes)}</g> chunks processed)"
                )
                cleanup_errors = await self._rollback_upload_transaction(
                    remote_path,
                    newly_added_refs,
                    staged_bins,
                    guards,
                )
                self._raise_upload_failure(primary, cleanup_errors)

            now = datetime.now(UTC)
            meta = FileMeta(
                info=FileInfo(
                    path=remote_path.as_posix(),
                    name=remote_path.name,
                    kind=EntryKind.FILE,
                    size=total_size,
                    modified=now,
                    created=now,
                ),
                chunks=chunk_hashes,
            )
            meta_bytes = meta.model_dump_json().encode()
            old_only = old_hashes - set(chunk_hashes)

            # Commit + post-commit cleanup are shielded so cancellation cannot
            # re-enter pre-commit rollback after durable metadata exists.
            with anyio.CancelScope(shield=True):
                try:
                    await self._index.mkdir(remote_path.parent, parents=True, exist_ok=True)
                    await self._index.upload_bytes(meta_bytes, remote_path, overwrite=True)
                except BaseException as primary:
                    try:
                        metadata_committed = await self._metadata_matches_upload(remote_path, meta_bytes)
                    except BaseException as inspection_error:
                        cleanup_errors: list[BaseException] = [inspection_error]
                        try:
                            await self._release_upload_guards(guards)
                        except BaseException as cleanup_error:
                            cleanup_errors.append(cleanup_error)
                        self._raise_upload_failure(primary, cleanup_errors)

                    if metadata_committed:
                        self.log.error(  # noqa: TRY400
                            f"Upload metadata committed with error: {_colored_path} "
                            f"(<g>{total_size}</g> bytes, <g>{len(chunk_hashes)}</g> chunks)"
                        )
                        try:
                            await self._post_commit_upload_cleanup(
                                remote_path,
                                old_only=old_only,
                                guards=guards,
                            )
                        except BaseException as cleanup_error:
                            self._raise_upload_failure(primary, [cleanup_error])
                        raise

                    self.log.error(  # noqa: TRY400
                        f"Upload failed: {_colored_path} "
                        f"(<g>{total_size}</g> bytes streamed, "
                        f"<g>{len(chunk_hashes)}</g> chunks processed)"
                    )
                    cleanup_errors = await self._rollback_upload_transaction(
                        remote_path,
                        newly_added_refs,
                        staged_bins,
                        guards,
                    )
                    self._raise_upload_failure(primary, cleanup_errors)

                try:
                    await self._post_commit_upload_cleanup(
                        remote_path,
                        old_only=old_only,
                        guards=guards,
                    )
                except BaseException:
                    self.log.error(  # noqa: TRY400
                        f"Upload post-commit cleanup failed: {_colored_path} "
                        f"(<g>{total_size}</g> bytes, <g>{len(chunk_hashes)}</g> chunks)"
                    )
                    raise
            # A cancellation requested during the shielded commit is delivered
            # only after metadata and its required cleanup are durable.
            await anyio.lowlevel.checkpoint()

        self.log.info(
            f"Upload complete: {_colored_path} (<g>{total_size}</g> bytes in <g>{len(chunk_hashes)}</g> chunks)"
        )
