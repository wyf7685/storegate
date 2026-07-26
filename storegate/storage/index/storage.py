import contextlib
import dataclasses
import functools
import hashlib
import json
import math
from collections.abc import AsyncGenerator, AsyncIterable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Literal, NoReturn, Self, final, override

import anyio
import anyio.lowlevel
from anyio.streams.memory import MemoryObjectReceiveStream
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from storegate.log import escape_tag

from ..abstract import (
    AbstractStorage,
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    WalkEntry,
    make_namespace_identity,
    validate_download_offset,
    validate_same_path_file_operation,
    validate_same_path_tree_operation,
)
from ._guard import download_private_file, lstat_private_entry, lstat_private_entry_or_none
from .lock import LockLease, StorageFileLocker, _is_tombstone
from .ref import ChunkRefManager, hash_to_path

BLOCK_SIZE = 64 * 1024 * 1024  # 64 MB
MAX_CONCURRENT_UPLOADS = 2
CHUNKS_INDEX_FILE = "/__chunks_index_id__"
MIN_LOCK_LEASE = 0.03
DEFAULT_LOCK_TIMEOUT = 30.0
DEFAULT_LOCK_LEASE = 300.0
# Lock objects live beside their subject in the index storage: ``<path>.lock``
# for a file and ``<path>.tree`` for a tree operation. A user file carrying one
# of these suffixes would occupy its own lock slot, so it is rejected outright
# rather than allowed to collide with lease payloads and tombstones.
RESERVED_SUFFIXES = (".lock", ".tree")


class FileMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    info: FileInfo
    chunks: list[str]

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_file_info(cls, value: object) -> object:
        if isinstance(value, Mapping):
            info = value.get("info")
            if isinstance(info, Mapping) and "is_dir" in info:
                raise ValueError("Legacy FileInfo is_dir metadata is not supported")
        return value

    @model_validator(mode="after")
    def require_file_kind(self) -> Self:
        if self.info.kind is not EntryKind.FILE:
            raise ValueError("Index FileMeta info must describe a regular file")
        return self


@final
class IndexStorage(AbstractStorage):
    _index: AbstractStorage
    _chunks: AbstractStorage

    def __init__(
        self,
        index: AbstractStorage,
        chunks: AbstractStorage,
        block_size: int = BLOCK_SIZE,
        max_concurrent_uploads: int = MAX_CONCURRENT_UPLOADS,
        lock_mode: Literal["strong", "best_effort", "disabled"] = "strong",
        lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
        lock_lease: float = DEFAULT_LOCK_LEASE,
    ):
        if index is chunks:
            raise ValueError("Index storage and chunks storage cannot be the same.")
        if not isinstance(block_size, int) or isinstance(block_size, bool) or block_size <= 0:
            raise ValueError("block_size must be a positive integer")
        if (
            not isinstance(max_concurrent_uploads, int)
            or isinstance(max_concurrent_uploads, bool)
            or max_concurrent_uploads <= 0
        ):
            raise ValueError("max_concurrent_uploads must be a positive integer")
        if lock_mode not in ("strong", "best_effort", "disabled"):
            raise ValueError("lock_mode must be 'strong', 'best_effort', or 'disabled'")
        if not isinstance(lock_timeout, (int, float)) or not math.isfinite(lock_timeout) or lock_timeout <= 0:
            raise ValueError("lock_timeout must be finite and greater than zero")
        if not isinstance(lock_lease, (int, float)) or not math.isfinite(lock_lease) or lock_lease < MIN_LOCK_LEASE:
            raise ValueError(f"lock_lease must be finite and at least {MIN_LOCK_LEASE}")

        super().__init__()
        self._index = index
        self._chunks = chunks
        self._block_size = block_size
        self._max_concurrent_uploads = max_concurrent_uploads
        self._lock_mode = lock_mode
        self._locker = StorageFileLocker(
            self,
            lock_timeout=lock_timeout,
            lock_lease=lock_lease,
            lock_mode=lock_mode,
        )
        self._refs = ChunkRefManager(
            storage=self,
            chunks=chunks,
            lock_chunk=self._lock_chunk,
        )
        self._pending_rollback: list[AbstractStorage] = []

    @staticmethod
    def _reject_reserved(*paths: PathLike) -> None:
        """Reject user paths that would collide with IndexStorage lock objects.

        Locks are stored beside their subject as ``<path>.lock`` / ``<path>.tree``
        in the index storage, so a user entry with one of those suffixes would
        share a key with lease payloads and tombstones.
        """
        for path in paths:
            name = AbstractStorage.normalize_path(path).name
            if name.endswith(RESERVED_SUFFIXES):
                raise ValueError(f"IndexStorage reserves the {RESERVED_SUFFIXES} suffixes for locks: {path}")

    @property
    @override
    def display_id(self) -> str:
        return f"index:{self._index.display_id}#{self._chunks.display_id}"

    @property
    @override
    def namespace_identity(self) -> str:
        return make_namespace_identity(
            "index",
            block_size=self._block_size,
            chunks=self._chunks.namespace_identity,
            index=self._index.namespace_identity,
        )

    @override
    async def connect(self) -> None:
        index = self._index
        chunks = self._chunks
        self.log.info(f"Connecting IndexStorage (index=<c>{index.display_id}</c>, chunks=<c>{chunks.display_id}</c>)")
        if self._lock_mode == "strong":
            self._require_compare_exchange(index, "index")
            self._require_compare_exchange(chunks, "chunks")
        await self._retry_pending_rollback()
        index_started = False
        chunks_started = False
        try:
            index_started = True
            await index.connect()
            chunks_started = True
            await chunks.connect()
            index_namespace_identity = index.namespace_identity
            try:
                existing_raw = (await download_private_file(chunks, CHUNKS_INDEX_FILE, label="binding file")).decode()
            except FileNotFoundError:
                existing_raw = None
            if existing_raw is None:
                payload = json.dumps(
                    {
                        "version": 2,
                        "index_namespace_identity": index_namespace_identity,
                    },
                    separators=(",", ":"),
                ).encode()
                await chunks.upload_bytes(payload, CHUNKS_INDEX_FILE, overwrite=False)
                self.log.success(
                    f"Registered chunks storage <c>{chunks.display_id}</c> → index <c>{index.display_id}</c>"
                )
            else:
                try:
                    existing = json.loads(existing_raw)
                except json.JSONDecodeError as error:
                    raise RuntimeError(
                        "Chunks storage binding uses an unsupported legacy format; "
                        "clean cutover requires version 2 JSON with index_namespace_identity"
                    ) from error
                if (
                    not isinstance(existing, dict)
                    or existing.get("version") != 2
                    or not isinstance(existing.get("index_namespace_identity"), str)
                ):
                    raise RuntimeError(
                        "Chunks storage binding uses an unsupported legacy format; "
                        "clean cutover requires version 2 JSON with index_namespace_identity"
                    )
                bound_identity = existing["index_namespace_identity"]
                if bound_identity != index_namespace_identity:
                    self.log.error(
                        f"Chunks storage <c>{chunks.display_id}</c> is already associated with "
                        f"index <r>{escape_tag(bound_identity)}</r>, rejecting index <c>{index.display_id}</c>"
                    )
                    raise RuntimeError(
                        f"Chunks storage is already associated with a different index storage: {bound_identity}"
                    )
                self.log.debug(
                    f"Chunks storage <c>{chunks.display_id}</c> already bound to index <c>{bound_identity}</c>"
                )
        except BaseException as primary:
            cleanup_errors: list[BaseException] = []
            failed_cleanup: list[AbstractStorage] = []
            with anyio.CancelScope(shield=True):
                if chunks_started:
                    try:
                        await chunks.close()
                    except BaseException as secondary:
                        cleanup_errors.append(secondary)
                        failed_cleanup.append(chunks)
                if index_started:
                    try:
                        await index.close()
                    except BaseException as secondary:
                        cleanup_errors.append(secondary)
                        failed_cleanup.append(index)
            self._pending_rollback.extend(failed_cleanup)
            if cleanup_errors:
                raise BaseExceptionGroup(
                    "Index storage connection rollback failed", [primary, *cleanup_errors]
                ) from None
            raise

    @staticmethod
    def _require_compare_exchange(storage: AbstractStorage, label: str) -> None:
        if not storage.capabilities.compare_exchange:
            raise RuntimeError(
                f"IndexStorage lock_mode='strong' requires compare_exchange capability "
                f"on the {label} storage ({storage.display_id}), "
                f"which does not support it. Use lock_mode='best_effort' or 'disabled' instead."
            )

    async def _retry_pending_rollback(self) -> None:
        if not self._pending_rollback:
            return
        pending = self._pending_rollback
        self._pending_rollback = []
        cleanup_errors: list[BaseException] = []
        with anyio.CancelScope(shield=True):
            for storage in pending:
                try:
                    await storage.close()
                except BaseException as error:
                    cleanup_errors.append(error)
                    self._pending_rollback.append(storage)
        if cleanup_errors:
            raise BaseExceptionGroup("Index storage pending rollback failed", cleanup_errors) from None

    @override
    async def close(self) -> None:
        self.log.debug("Closing IndexStorage")
        try:
            await self._chunks.close()
        finally:
            await self._index.close()

    @override
    async def ping(self) -> bool:
        return await self._index.ping() and await self._chunks.ping()

    @contextlib.asynccontextmanager
    async def _lock_index(self, index_path: PathLike) -> AsyncGenerator[None]:
        lock_path = f"{index_path}.lock"
        lease = await self._locker.acquire_lock(self._index, lock_path)
        try:
            async with self._locker.renewing_locks([lease]):
                yield
        except BaseException:
            await self._locker.release_locks(self._index, [(lock_path, lease)], suppress_errors=True)
            raise
        else:
            await self._locker.release_locks(self._index, [(lock_path, lease)], suppress_errors=False)

    @contextlib.asynccontextmanager
    async def _lock_chunk(self, chunk_hash: str) -> AsyncGenerator[None]:
        lock_path = hash_to_path(chunk_hash, "lock")
        lease = await self._locker.acquire_lock(self._chunks, lock_path)
        try:
            async with self._locker.renewing_locks([lease]):
                yield
        except BaseException:
            await self._locker.release_locks(self._chunks, [(lock_path, lease)], suppress_errors=True)
            raise
        else:
            await self._locker.release_locks(self._chunks, [(lock_path, lease)], suppress_errors=False)

    @contextlib.asynccontextmanager
    async def _lock_indexes(self, *index_paths: PathLike) -> AsyncGenerator[None]:
        lock_paths = [f"{index_path}.lock" for index_path in sorted(set(index_paths))]
        leases: list[tuple[PathLike, LockLease | None]] = []
        try:
            for lock_path in lock_paths:
                leases.append((lock_path, await self._locker.acquire_lock(self._index, lock_path)))  # noqa: PERF401
            async with self._locker.renewing_locks(lease for _, lease in leases):
                yield
        except BaseException:
            await self._locker.release_locks(self._index, reversed(leases), suppress_errors=True)
            raise
        else:
            await self._locker.release_locks(self._index, reversed(leases), suppress_errors=False)

    @contextlib.asynccontextmanager
    async def _lock_chunks(self, chunk_hashes: Iterable[str]) -> AsyncGenerator[None]:
        lock_paths = [hash_to_path(chunk_hash, "lock") for chunk_hash in sorted(set(chunk_hashes))]
        leases: list[tuple[PathLike, LockLease | None]] = []
        try:
            for lock_path in lock_paths:
                leases.append((lock_path, await self._locker.acquire_lock(self._chunks, lock_path)))  # noqa: PERF401
            async with self._locker.renewing_locks(lease for _, lease in leases):
                yield
        except BaseException:
            await self._locker.release_locks(self._chunks, reversed(leases), suppress_errors=True)
            raise
        else:
            await self._locker.release_locks(self._chunks, reversed(leases), suppress_errors=False)

    async def _get_file_meta(self, path: PathLike) -> FileMeta | None:
        path = self.normalize_path(path)
        try:
            meta_bytes = await download_private_file(self._index, path, label="metadata entry")
        except FileNotFoundError:
            return None
        if not meta_bytes:
            return None
        # Internal lock tombstones are not user file metadata.
        if _is_tombstone(meta_bytes):
            return None
        try:
            return FileMeta.model_validate_json(meta_bytes.decode())
        except (UnicodeDecodeError, ValidationError) as error:
            raise OSError(f"Corrupted file metadata for {path}") from error

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
                            info=dataclasses.replace(src_meta.info, path=dst.as_posix(), name=dst.name),
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
                            info=dataclasses.replace(src_meta.info, path=dst.as_posix(), name=dst.name),
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

    async def _read_is_tombstone(self, entry_path: PurePosixPath) -> bool:
        """Return True when *entry_path* is a recognised clean-release tombstone."""
        try:
            data = await download_private_file(self._index, entry_path, label="tree entry")
            return _is_tombstone(data)
        except FileNotFoundError, OSError:
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
            return dataclasses.replace(info, path=path.as_posix(), name=path.name)
        meta = await self._get_file_meta(path)
        if meta is None:
            raise FileNotFoundError(f"File not found: {path}")
        return dataclasses.replace(meta.info, path=path.as_posix(), name=path.name)

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
                    yield dataclasses.replace(entry, path=entry_path.as_posix(), name=entry_path.name)
                case EntryKind.FILE:
                    meta = await self._get_file_meta(entry_path)
                    if meta is not None:
                        yield dataclasses.replace(meta.info, path=entry_path.as_posix(), name=entry_path.name)
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
                entries.append(dataclasses.replace(meta.info, path=entry_path.as_posix(), name=entry_path.name))

        async for underlying_entry in self._index.walk(path):
            entries: list[FileInfo] = []
            async with anyio.create_task_group() as tg:
                for entry in underlying_entry.entries:
                    entry_path = self.normalize_path(entry.path)
                    match entry.kind:
                        case EntryKind.DIRECTORY:
                            entries.append(dataclasses.replace(entry, path=entry_path.as_posix(), name=entry_path.name))
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
