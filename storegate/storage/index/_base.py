import contextlib
import json
import math
from collections.abc import AsyncGenerator, Iterable
from typing import Literal, override

import anyio
from pydantic import ValidationError

from storegate.log import escape_tag

from ..abstract import AbstractStorage, PathLike, make_namespace_identity
from ._guard import download_private_file
from .lock import LockLease, StorageFileLocker, _is_tombstone
from .models import FileMeta
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


class IndexStorageBase(AbstractStorage):
    """State, lifecycle and lock primitives shared by the IndexStorage mixins.

    Splitting the operation mixins out of ``IndexStorage`` keeps each
    transaction in its own module; they cooperate only through the members
    defined here, so this class is the whole contract between them.
    """

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
