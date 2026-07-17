import contextlib
import dataclasses
import functools
import hashlib
import json
import math
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import ClassVar, final, override

import anyio
import anyio.lowlevel
from anyio.streams.memory import MemoryObjectReceiveStream
from pydantic import BaseModel, ValidationError

from app.log import escape_tag

from ..abstract import AbstractStorage, BytesLike, FileInfo, PathLike, make_cache_identity

BLOCK_SIZE = 64 * 1024 * 1024  # 64 MB
MAX_CONCURRENT_UPLOADS = 2
CHUNKS_INDEX_FILE = "/__chunks_index_id__"
MIN_LOCK_LEASE = 0.03
DEFAULT_LOCK_TIMEOUT = 30.0
DEFAULT_LOCK_LEASE = 300.0


class LockLeaseLostError(RuntimeError):
    """Raised when an active IndexStorage lock can no longer prove ownership."""


@dataclasses.dataclass(slots=True)
class _LocalLockGuard:
    lock: anyio.Lock
    references: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class _LockLease:
    owner: str
    expires: datetime
    storage: AbstractStorage
    path: PathLike


def hash_to_path(hash_str: str, suffix: str | None = None) -> str:
    """Convert a hash string to a path with subdirectories."""
    return f"{hash_str[:2]}/{hash_str[2:6]}/{hash_str[6:]}{f".{suffix}" if suffix else ""}"


class FileMeta(BaseModel):
    info: FileInfo
    chunks: list[str]


@final
class IndexStorage(AbstractStorage):
    _local_lock_guards: ClassVar[dict[str, _LocalLockGuard]] = {}
    _index: AbstractStorage
    _chunks: AbstractStorage
    _block_size: int
    _max_concurrent_uploads: int
    _skip_locking: bool
    _lock_timeout: float
    _lock_lease: float

    def __init__(
        self,
        index: AbstractStorage,
        chunks: AbstractStorage,
        block_size: int = BLOCK_SIZE,
        max_concurrent_uploads: int = MAX_CONCURRENT_UPLOADS,
        skip_locking: bool = False,
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
        if not isinstance(lock_timeout, (int, float)) or not math.isfinite(lock_timeout) or lock_timeout <= 0:
            raise ValueError("lock_timeout must be finite and greater than zero")
        if not isinstance(lock_lease, (int, float)) or not math.isfinite(lock_lease) or lock_lease < MIN_LOCK_LEASE:
            raise ValueError(f"lock_lease must be finite and at least {MIN_LOCK_LEASE}")
        super().__init__()
        self._index = index
        self._chunks = chunks
        self._block_size = block_size
        self._max_concurrent_uploads = max_concurrent_uploads
        self._skip_locking = skip_locking
        self._lock_timeout = lock_timeout
        self._lock_lease = lock_lease
        self._pending_rollback: list[AbstractStorage] = []

    @property
    @override
    def id(self) -> str:
        return f"index:{self._index.id}#{self._chunks.id}"

    @property
    @override
    def cache_identity(self) -> str | None:
        index_identity = self._index.cache_identity
        chunks_identity = self._chunks.cache_identity
        if index_identity is None or chunks_identity is None:
            return None
        return make_cache_identity(
            "index",
            block_size=self._block_size,
            chunks=chunks_identity,
            index=index_identity,
        )

    @override
    async def connect(self) -> None:
        index = self._index
        chunks = self._chunks
        self.log.info(f"Connecting IndexStorage (index=<c>{index.id}</c>, chunks=<c>{self._chunks.id}</c>)")
        await self._retry_pending_rollback()
        index_started = False
        chunks_started = False
        try:
            index_started = True
            await index.connect()
            chunks_started = True
            await chunks.connect()
            try:
                existing = (await chunks.download_bytes(CHUNKS_INDEX_FILE)).decode()
            except FileNotFoundError:
                existing = None
            if existing is None:
                await chunks.upload_bytes(index.id.encode(), CHUNKS_INDEX_FILE, overwrite=False)
                self.log.debug(f"Registered chunks storage <c>{chunks.id}</c> → index <c>{index.id}</c>")
            elif existing != index.id:
                self.log.error(
                    f"Chunks storage <c>{chunks.id}</c> is already associated with "
                    f"index <r>{escape_tag(existing)}</r>, rejecting index <c>{index.id}</c>"
                )
                raise RuntimeError(f"Chunks storage is already associated with a different index storage: {existing}")
            else:
                self.log.debug(f"Chunks storage <c>{chunks.id}</c> already bound to index <c>{existing}</c>")
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
    async def _local_lock_guard(self, key: str) -> AsyncGenerator[None]:
        registry = IndexStorage._local_lock_guards
        entry = registry.get(key)
        if entry is None:
            entry = _LocalLockGuard(anyio.Lock())
            registry[key] = entry
        entry.references += 1
        acquired = False
        try:
            await entry.lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                entry.lock.release()
            entry.references -= 1
            if entry.references == 0 and registry.get(key) is entry:
                del registry[key]

    async def _release_storage_file_lock_locked(
        self, storage: AbstractStorage, lock_path: PathLike, lease: _LockLease
    ) -> None:
        try:
            current = await storage.download_bytes(lock_path)
            data = json.loads(current.decode())
            if data.get("owner") != lease.owner:
                self.log.warning(f"Lock <y>{escape_tag(lock_path)}</y> owner changed; leaving it intact")
                return
            # The storage contract has no conditional delete. Re-reading immediately before
            # unlink minimizes the takeover race, but another process can still replace the
            # lock after this check and before unlink.
            if await storage.download_bytes(lock_path) != current:
                self.log.warning(f"Lock <y>{escape_tag(lock_path)}</y> changed; leaving it intact")
                return
            await storage.unlink(lock_path, missing_ok=True)
            self.log.trace(f"Lock <y>{escape_tag(lock_path)}</y> released")
        except FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError:
            return

    def _renewal_validity(self) -> float:
        """Leave a scheduler-safe validity window for one bounded renewal probe."""
        return max(self._lock_lease + (2 * self._lock_timeout), 0.5)

    async def _renew_storage_file_lock(self, lease: _LockLease) -> None:
        interval = self._lock_lease / 3
        key = f"{lease.storage.id}:{lease.storage.normalize_path(lease.path)}"
        while True:
            try:
                with anyio.fail_after(self._lock_timeout):
                    async with self._local_lock_guard(key):
                        current = await lease.storage.download_bytes(lease.path)
                        data = json.loads(current.decode())
                        if data.get("owner") != lease.owner:
                            raise LockLeaseLostError(f"Storage lock ownership lost during renewal: {lease.path}")
                        now = datetime.now(UTC)
                        if now >= lease.expires:
                            raise LockLeaseLostError(f"Storage lock lease expired before renewal: {lease.path}")
                        expires = now + timedelta(seconds=self._renewal_validity())
                        data["expires"] = expires.isoformat()
                        payload = json.dumps(data, separators=(",", ":")).encode()
                        # A second read is the best ownership proof available without CAS.
                        if await lease.storage.download_bytes(lease.path) != current:
                            raise LockLeaseLostError(f"Storage lock ownership changed during renewal: {lease.path}")
                        await lease.storage.upload_bytes(payload, lease.path, overwrite=True)
                        lease = dataclasses.replace(lease, expires=expires)
            except TimeoutError:
                if datetime.now(UTC) >= lease.expires:
                    raise LockLeaseLostError(f"Storage lock renewal probe timed out: {lease.path}") from None
                await anyio.lowlevel.checkpoint()
                continue
            except (FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
                raise LockLeaseLostError(f"Storage lock ownership lost during renewal: {lease.path}") from error
            await anyio.sleep(interval)

    @contextlib.asynccontextmanager
    async def _renewing_locks(self, leases: Iterable[_LockLease | None]) -> AsyncGenerator[None]:
        try:
            async with anyio.create_task_group() as tg:
                for lease in leases:
                    if lease is not None:
                        tg.start_soon(self._renew_storage_file_lock, lease)
                try:
                    yield
                finally:
                    tg.cancel_scope.cancel()
        except BaseExceptionGroup as group:
            error: BaseException = group
            while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
                error = error.exceptions[0]
            if error is group:
                raise
            raise error from group

    async def _acquire_storage_file_lock(self, storage: AbstractStorage, lock_path: PathLike) -> _LockLease | None:
        _colored_path = f"<y>{escape_tag(lock_path)}</y>"
        if self._skip_locking:
            self.log.trace(f"Lock {_colored_path} disabled, skipping ...")
            return None

        key = f"{storage.id}:{storage.normalize_path(lock_path)}"
        deadline = anyio.current_time() + self._lock_timeout
        while True:
            remaining = deadline - anyio.current_time()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}")
            lease: _LockLease | None = None
            try:
                with anyio.fail_after(remaining):
                    async with self._local_lock_guard(key):
                        if not await storage.exists(lock_path):
                            owner = uuid.uuid4().hex
                            handoff_timeout = deadline - anyio.current_time()
                            if handoff_timeout <= 0:
                                raise TimeoutError
                            now = datetime.now(UTC)
                            # A backend may commit before its upload call returns. Cover its
                            # bounded handoff plus one lease and renewal probe budget.
                            expires = now + timedelta(seconds=handoff_timeout + self._renewal_validity())
                            lease = _LockLease(owner, expires, storage, lock_path)
                            payload = json.dumps(
                                {"owner": owner, "created": now.isoformat(), "expires": expires.isoformat()},
                                separators=(",", ":"),
                            ).encode()
                            try:
                                # This shield protects the post-commit handoff from external
                                # cancellation without extending the acquisition deadline.
                                with anyio.fail_after(handoff_timeout, shield=True):
                                    await storage.upload_bytes(payload, lock_path, overwrite=False)
                            except FileExistsError:
                                lease = None
                            if lease is not None:
                                await anyio.lowlevel.checkpoint()
                                self.log.trace(f"Lock {_colored_path} acquired")
                                return lease
                        else:
                            try:
                                lock_bytes = await storage.download_bytes(lock_path)
                            except FileNotFoundError:
                                lock_bytes = None
                            if lock_bytes is not None:
                                try:
                                    lock_data = json.loads(lock_bytes.decode())
                                    stale = datetime.fromisoformat(lock_data["expires"]) <= datetime.now(UTC)
                                except KeyError, TypeError, ValueError, UnicodeDecodeError:
                                    try:
                                        info = await storage.stat(lock_path)
                                        stale = (
                                            info.modified is not None
                                            and (datetime.now(UTC) - info.modified).total_seconds() >= self._lock_lease
                                        )
                                    except FileNotFoundError:
                                        stale = False
                                if stale:
                                    # The equality check narrows, but cannot eliminate, the
                                    # final cross-process replace-before-unlink race without CAS.
                                    try:
                                        if await storage.download_bytes(lock_path) == lock_bytes:
                                            await storage.unlink(lock_path, missing_ok=True)
                                    except FileNotFoundError:
                                        pass
                                    continue
            except BaseException as error:
                if lease is not None:
                    try:
                        await self._release_storage_file_lock(storage, lock_path, lease)
                    except BaseException as cleanup_error:
                        self.log.warning(
                            f"Failed to clean up uncertain lock <y>{escape_tag(lock_path)}</y>: {cleanup_error!r}"
                        )
                if isinstance(error, TimeoutError):
                    raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}") from error
                raise

            remaining = deadline - anyio.current_time()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}")
            await anyio.sleep(min(0.1, remaining))

    async def _release_storage_file_lock(
        self, storage: AbstractStorage, lock_path: PathLike, lease: _LockLease | None
    ) -> None:
        if self._skip_locking or lease is None:
            return
        key = f"{storage.id}:{storage.normalize_path(lock_path)}"
        try:
            with anyio.fail_after(self._lock_timeout, shield=True):
                async with self._local_lock_guard(key):
                    await self._release_storage_file_lock_locked(storage, lock_path, lease)
        except TimeoutError as error:
            raise TimeoutError(f"Timed out releasing storage lock: {lock_path}") from error

    async def _release_storage_file_locks(
        self,
        storage: AbstractStorage,
        leases: Iterable[tuple[PathLike, _LockLease | None]],
        *,
        suppress_errors: bool,
    ) -> None:
        failures: list[tuple[PathLike, BaseException]] = []
        with anyio.CancelScope(shield=True):
            for lock_path, lease in leases:
                try:
                    await self._release_storage_file_lock(storage, lock_path, lease)
                except BaseException as error:
                    failures.append((lock_path, error))
        if not failures:
            return
        if suppress_errors:
            for lock_path, error in failures:
                self.log.warning(f"Failed to clean up storage lock <y>{escape_tag(lock_path)}</y>: {error!r}")
            return
        if len(failures) == 1:
            raise failures[0][1]
        raise BaseExceptionGroup("Failed to release storage locks", [error for _, error in failures])

    @contextlib.asynccontextmanager
    async def _lock_index(self, index_path: PathLike) -> AsyncGenerator[None]:
        lock_path = f"{index_path}.lock"
        lease = await self._acquire_storage_file_lock(self._index, lock_path)
        try:
            async with self._renewing_locks([lease]):
                yield
        except BaseException:
            await self._release_storage_file_locks(self._index, [(lock_path, lease)], suppress_errors=True)
            raise
        else:
            await self._release_storage_file_locks(self._index, [(lock_path, lease)], suppress_errors=False)

    @contextlib.asynccontextmanager
    async def _lock_chunk(self, chunk_hash: str) -> AsyncGenerator[None]:
        lock_path = hash_to_path(chunk_hash, "lock")
        lease = await self._acquire_storage_file_lock(self._chunks, lock_path)
        try:
            async with self._renewing_locks([lease]):
                yield
        except BaseException:
            await self._release_storage_file_locks(self._chunks, [(lock_path, lease)], suppress_errors=True)
            raise
        else:
            await self._release_storage_file_locks(self._chunks, [(lock_path, lease)], suppress_errors=False)

    @contextlib.asynccontextmanager
    async def _lock_indexes(self, *index_paths: PathLike) -> AsyncGenerator[None]:
        lock_paths = [f"{index_path}.lock" for index_path in sorted(set(index_paths))]
        leases: list[tuple[PathLike, _LockLease | None]] = []
        try:
            for lock_path in lock_paths:
                leases.append((lock_path, await self._acquire_storage_file_lock(self._index, lock_path)))  # noqa: PERF401
            async with self._renewing_locks(lease for _, lease in leases):
                yield
        except BaseException:
            await self._release_storage_file_locks(self._index, reversed(leases), suppress_errors=True)
            raise
        else:
            await self._release_storage_file_locks(self._index, reversed(leases), suppress_errors=False)

    @contextlib.asynccontextmanager
    async def _lock_chunks(self, chunk_hashes: Iterable[str]) -> AsyncGenerator[None]:
        lock_paths = [hash_to_path(chunk_hash, "lock") for chunk_hash in sorted(set(chunk_hashes))]
        leases: list[tuple[PathLike, _LockLease | None]] = []
        try:
            for lock_path in lock_paths:
                leases.append((lock_path, await self._acquire_storage_file_lock(self._chunks, lock_path)))  # noqa: PERF401
            async with self._renewing_locks(lease for _, lease in leases):
                yield
        except BaseException:
            await self._release_storage_file_locks(self._chunks, reversed(leases), suppress_errors=True)
            raise
        else:
            await self._release_storage_file_locks(self._chunks, reversed(leases), suppress_errors=False)

    async def _get_file_meta(self, path: PathLike) -> FileMeta | None:
        try:
            meta_bytes = await self._index.download_bytes(self.normalize_path(path))
        except FileNotFoundError:
            return None
        if not meta_bytes:
            return None
        try:
            return FileMeta.model_validate_json(meta_bytes.decode())
        except ValidationError as e:
            raise OSError(f"Corrupted file metadata for {path}") from e

    async def _chunk_load_refs(self, chunk_hash: str) -> set[str] | None:
        ref_path = hash_to_path(chunk_hash, "ref")
        try:
            ref_bytes = await self._chunks.download_bytes(ref_path)
        except FileNotFoundError:
            return None
        return set(ref_bytes.decode().splitlines())

    async def _chunk_incref(self, chunk_hash: str, *remote_path: PathLike) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        _colored_remote_paths = ", ".join(f"<i>{escape_tag(p)}</i>" for p in remote_path)

        refs: set[str] = await self._chunk_load_refs(chunk_hash) or set()

        refs.update(self.normalize_path(p).as_posix() for p in remote_path)
        await self._chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(f"Chunk {_colored_hash} +ref → <g>{len(refs)}</g> ({_colored_remote_paths})")

    async def _chunk_decref(self, chunk_hash: str, *remote_path: PathLike) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        _colored_remote_paths = ", ".join(f"<i>{escape_tag(p)}</i>" for p in remote_path)

        refs = await self._chunk_load_refs(chunk_hash)
        if refs is None:
            self.log.warning(f"Chunk {_colored_hash} ref file missing, skip decref ({_colored_remote_paths})")
            return

        removed = False
        for p in remote_path:
            p = self.normalize_path(p).as_posix()
            if p in refs:
                refs.remove(p)
                removed = True
            else:
                self.log.warning(f"Chunk {_colored_hash} ref entry not found for <i>{escape_tag(p)}</i>, skip decref")

        if refs:
            if removed:
                await self._chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
                self.log.debug(f"Chunk {_colored_hash} -ref → <g>{len(refs)}</g> (<i>{_colored_remote_paths}</i>)")
            else:
                self.log.debug(f"Chunk {_colored_hash} -ref no change (<i>{_colored_remote_paths}</i>)")
        else:
            await self._chunks.unlink(ref_path, missing_ok=True)
            await self._chunks.unlink(hash_to_path(chunk_hash, "bin"), missing_ok=True)
            self.log.debug(f"Chunk {_colored_hash} ref=0, deleted data (<i>{_colored_remote_paths}</i>)")

    async def _chunk_transref(
        self,
        chunk_hash: str,
        *pairs: tuple[PathLike, PathLike],
        missing_ok: bool = False,
    ) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"

        refs: set[str] | None = await self._chunk_load_refs(chunk_hash)
        if refs is None:
            if not missing_ok:
                raise FileNotFoundError(f"Chunk {chunk_hash} ref file missing for transref")
            refs = set()

        for src_path, dst_path in pairs:
            src_path = self.normalize_path(src_path).as_posix()
            dst_path = self.normalize_path(dst_path).as_posix()
            if src_path not in refs:
                if missing_ok:
                    self.log.warning(
                        f"Chunk {_colored_hash} ref entry not found for transref: <i>{escape_tag(src_path)}</i>"
                    )
                else:
                    raise FileNotFoundError(f"Chunk {chunk_hash} ref entry not found for transref: {src_path}")

            refs.remove(src_path)
            refs.add(dst_path)

        await self._chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(
            f"Chunk {_colored_hash} transref: "
            f"{", ".join(f"<i>{escape_tag(src)}</i> → <i>{escape_tag(dst)}</i>" for src, dst in pairs)} "
            f"(<g>{len(refs)}</g> refs)"
        )

    @contextlib.asynccontextmanager
    async def _chunk_temp_ref(self, chunk_hash: str) -> AsyncGenerator[None]:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        temp_ref = f"$tempref-{uuid.uuid4().hex[:8]}"

        async with self._lock_chunk(chunk_hash):
            refs = await self._chunk_load_refs(chunk_hash) or set()
            refs.add(temp_ref)
            await self._chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(f"Chunk {_colored_hash} +tempref → <g>{len(refs)}</g> (<i>{escape_tag(temp_ref)}</i>)")

        try:
            yield
        finally:
            async with self._lock_chunk(chunk_hash):
                refs = await self._chunk_load_refs(chunk_hash) or set()
                if temp_ref in refs:
                    refs.remove(temp_ref)
                    if refs:
                        await self._chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
                        self.log.debug(
                            f"Chunk {_colored_hash} -tempref → <g>{len(refs)}</g> (<i>{escape_tag(temp_ref)}</i>)"
                        )
                    else:
                        await self._chunks.unlink(ref_path, missing_ok=True)
                        await self._chunks.unlink(hash_to_path(chunk_hash, "bin"), missing_ok=True)
                        self.log.debug(f"Chunk {_colored_hash} tempref=0, deleted data (<i>{escape_tag(temp_ref)}</i>)")

    async def _chunk_add_temp_ref_unlocked(self, chunk_hash: str, temp_ref: str) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        refs = await self._chunk_load_refs(chunk_hash) or set()
        refs.add(temp_ref)
        await self._chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)

    async def _chunk_remove_temp_ref_unlocked(self, chunk_hash: str, temp_ref: str) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        refs = await self._chunk_load_refs(chunk_hash)
        if refs is None or temp_ref not in refs:
            return
        refs.remove(temp_ref)
        if refs:
            await self._chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        else:
            await self._chunks.unlink(ref_path, missing_ok=True)
            await self._chunks.unlink(hash_to_path(chunk_hash, "bin"), missing_ok=True)

    async def _chunk_add_rollback_guards(self, chunk_hashes: Iterable[str]) -> dict[str, str]:
        guards: dict[str, str] = {}
        try:
            for chunk_hash in sorted(set(chunk_hashes)):
                temp_ref = f"$rollback-{uuid.uuid4().hex}"
                await self._chunk_add_temp_ref_unlocked(chunk_hash, temp_ref)
                guards[chunk_hash] = temp_ref
        except BaseException:
            with anyio.CancelScope(shield=True):
                for chunk_hash, temp_ref in guards.items():
                    with contextlib.suppress(Exception):
                        await self._chunk_remove_temp_ref_unlocked(chunk_hash, temp_ref)
            raise
        return guards

    async def _chunk_release_rollback_guards(self, guards: dict[str, str]) -> None:
        for chunk_hash, temp_ref in guards.items():
            await self._chunk_remove_temp_ref_unlocked(chunk_hash, temp_ref)

    async def _save_chunk_worker(
        self,
        recv: MemoryObjectReceiveStream[tuple[str, bytes, PathLike]],
        incref_done: set[str],
    ) -> None:
        """Worker：从 channel 拉取 block，保存或复用已有分块。"""
        async for chunk_hash, data, remote_path in recv:
            bin_path = hash_to_path(chunk_hash, "bin")

            async with self._lock_chunk(chunk_hash):
                if not await self._chunks.exists(bin_path):
                    start = anyio.current_time()
                    await self._chunks.upload_bytes(data, bin_path)
                    elapsed = anyio.current_time() - start
                    self.log.debug(
                        f"Chunk <c>{chunk_hash[:8]}</c> uploaded (<g>{len(data)}</g> bytes, <g>{elapsed:.2f}</g> s)"
                    )
                else:
                    self.log.debug(f"Chunk <c>{chunk_hash[:8]}</c> already exists, skipping upload")
                await self._chunk_incref(chunk_hash, remote_path)

            incref_done.add(chunk_hash)

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        remote_path = self.normalize_path(remote_path)

        try:
            info = await self.stat(remote_path)
        except FileNotFoundError:
            pass
        else:
            if info.is_dir:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            if not overwrite:
                raise FileExistsError(f"File already exists: {remote_path}")

        _colored_path = f"<y>{escape_tag(remote_path)}</y>"
        self.log.info(f"Upload starting: {_colored_path}")

        chunk_hashes: list[str] = []
        total_size = 0
        incref_done: set[str] = set()
        max_workers = self._max_concurrent_uploads

        try:
            async with self._lock_index(remote_path):
                # 取出旧元数据，用于后续清理不再引用的旧分块
                old_meta = await self._get_file_meta(remote_path)

                send, recv = anyio.create_memory_object_stream[tuple[str, bytes, PathLike]](max_workers * 2)

                async with anyio.create_task_group() as tg, send:
                    for worker_idx in range(max_workers):
                        self.log.debug(f"Starting chunk upload worker #{worker_idx + 1}")
                        tg.start_soon(self._save_chunk_worker, recv.clone(), incref_done)
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

                now = datetime.now(UTC)
                meta = FileMeta(
                    info=FileInfo(
                        path=remote_path.as_posix(),
                        name=remote_path.name,
                        is_dir=False,
                        size=total_size,
                        modified=now,
                        created=now,
                    ),
                    chunks=chunk_hashes,
                )
                await self._index.mkdir(remote_path.parent, parents=True, exist_ok=True)
                await self._index.upload_bytes(meta.model_dump_json().encode(), remote_path, overwrite=True)

            # 清理旧文件不再引用的分块
            if old_meta is not None:
                async with self._lock_chunks(old_meta.chunks), anyio.create_task_group() as tg:
                    for h in set(old_meta.chunks) - set(chunk_hashes):
                        tg.start_soon(self._chunk_decref, h, remote_path)

            self.log.info(
                f"Upload complete: {_colored_path} (<g>{total_size}</g> bytes in <g>{len(chunk_hashes)}</g> chunks)"
            )
        except Exception:
            self.log.error(  # noqa: TRY400
                f"Upload failed: {_colored_path} "
                f"(<g>{total_size}</g> bytes streamed, "
                f"<g>{len(chunk_hashes)}</g> chunks processed)"
            )

            # 回滚已 incref 的分块
            with anyio.CancelScope(shield=True):
                async with self._lock_chunks(incref_done), anyio.create_task_group() as tg:
                    for h in incref_done:
                        tg.start_soon(self._chunk_decref, h, remote_path)
            raise

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
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
                    if not await self._chunks.exists(bin_path):
                        raise FileNotFoundError(f"Chunk #{idx + 1} {chunk_hash} not found for file {remote_path}")
                    chunk_size = (await self._chunks.stat(bin_path)).size
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
                async with self._chunk_temp_ref(chunk_hash):
                    if not await self._chunks.exists(bin_path):
                        raise FileNotFoundError(f"Chunk #{idx + 1} {chunk_hash} not found for file {remote_path}")
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

        if await self._index.is_dir(path):
            raise IsADirectoryError(f"Is a directory: {path}")

        async with self._lock_index(path):
            meta = await self._get_file_meta(path)
            if meta is None:
                if missing_ok:
                    return
                raise FileNotFoundError(f"File not found: {path}")
            async with self._lock_chunks(meta.chunks):
                async with anyio.create_task_group() as tg:
                    for chunk_hash in meta.chunks:
                        tg.start_soon(self._chunk_decref, chunk_hash, path)
                await self._index.unlink(path)
        self.log.info(f"Deleted: {_colored_path} (<g>{meta.info.size}</g> bytes, <g>{len(meta.chunks)}</g> chunks)")

    @override
    async def rmdir(self, path: PathLike) -> None:
        try:
            info = await self._index.stat(path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Directory not found: {path}") from e
        if not info.is_dir:
            raise NotADirectoryError(f"Not a directory: {path}")
        if not await self._is_dir_empty(path):
            raise OSError(f"Directory not empty: {path}")

        await self._index.rmdir(path)

    @override
    async def move(
        self,
        src: PathLike,
        dst: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        if src == dst:
            if await self._index.is_dir(src):
                raise IsADirectoryError(f"Is a directory: {src}")
            if await self._get_file_meta(src) is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            if not overwrite:
                raise FileExistsError(f"Destination file already exists: {dst}")
            return

        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        async with self._lock_indexes(src, dst):
            if await self._index.is_dir(src):
                raise IsADirectoryError(f"Is a directory: {src}")
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
                guards = await self._chunk_add_rollback_guards(old_only)
                try:
                    try:
                        async with anyio.create_task_group() as tg:
                            for chunk_hash in src_meta.chunks:
                                tg.start_soon(self._chunk_transref, chunk_hash, (src, dst))
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
                                        tg.start_soon(self._chunk_incref, chunk_hash, src)
                                    else:
                                        tg.start_soon(
                                            functools.partial(
                                                self._chunk_transref,
                                                chunk_hash,
                                                (dst, src),
                                                missing_ok=True,
                                            )
                                        )
                            await self._chunk_release_rollback_guards(guards)
                        raise

                    try:
                        for chunk_hash in old_only:
                            await self._chunk_decref(chunk_hash, dst)
                    except BaseException:
                        with anyio.CancelScope(shield=True):
                            if dst_meta is not None:
                                await self._index.upload_bytes(dst_meta.model_dump_json().encode(), dst, overwrite=True)
                            async with anyio.create_task_group() as tg:
                                for chunk_hash in old_only:
                                    tg.start_soon(self._chunk_incref, chunk_hash, dst)
                                for chunk_hash in src_meta.chunks:
                                    if chunk_hash in old_hashes:
                                        tg.start_soon(self._chunk_incref, chunk_hash, src)
                                    else:
                                        tg.start_soon(
                                            functools.partial(
                                                self._chunk_transref,
                                                chunk_hash,
                                                (dst, src),
                                                missing_ok=True,
                                            )
                                        )
                            await self._chunk_release_rollback_guards(guards)
                        raise
                    with anyio.CancelScope(shield=True):
                        await self._chunk_release_rollback_guards(guards)
                except BaseException:  # noqa: TRY203
                    raise
            await self._index.unlink(src)

    @override
    async def copy(
        self,
        src: PathLike,
        dst: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        if src == dst:
            if await self._index.is_dir(src):
                raise IsADirectoryError(f"Is a directory: {src}")
            if await self._get_file_meta(src) is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            if not overwrite:
                raise FileExistsError(f"Destination file already exists: {dst}")
            return

        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        async with self._lock_indexes(src, dst):
            if await self._index.is_dir(src):
                raise IsADirectoryError(f"Is a directory: {src}")
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
                guards = await self._chunk_add_rollback_guards(old_only)
                try:
                    try:
                        async with anyio.create_task_group() as tg:
                            for chunk_hash in src_meta.chunks:
                                if chunk_hash not in old_hashes:
                                    tg.start_soon(self._chunk_incref, chunk_hash, dst)
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
                                        tg.start_soon(self._chunk_decref, chunk_hash, dst)
                            await self._chunk_release_rollback_guards(guards)
                        raise

                    try:
                        for chunk_hash in old_only:
                            await self._chunk_decref(chunk_hash, dst)
                    except BaseException:
                        with anyio.CancelScope(shield=True):
                            if dst_meta is not None:
                                await self._index.upload_bytes(dst_meta.model_dump_json().encode(), dst, overwrite=True)
                            async with anyio.create_task_group() as tg:
                                for chunk_hash in old_only:
                                    tg.start_soon(self._chunk_incref, chunk_hash, dst)
                                for chunk_hash in src_meta.chunks:
                                    if chunk_hash not in old_hashes:
                                        tg.start_soon(self._chunk_decref, chunk_hash, dst)
                            await self._chunk_release_rollback_guards(guards)
                        raise
                    with anyio.CancelScope(shield=True):
                        await self._chunk_release_rollback_guards(guards)
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
        return await self._index.mkdir(self.normalize_path(path), parents=parents, exist_ok=exist_ok)

    @override
    async def rmtree(self, path: PathLike) -> None:
        path = self.normalize_path(path)
        _colored_path = f"<y>{escape_tag(path)}</y>"
        self.log.info(f"RmTree: {_colored_path}")

        count = 0
        async with anyio.create_task_group() as tg:
            async for info in self._index.iterdir(path):
                count += 1
                tg.start_soon(self.rmtree if info.is_dir else self.unlink, self.normalize_path(info.path))
        await self._index.rmdir(path)
        self.log.info(f"RmTree complete: {_colored_path} (<g>{count}</g> entries removed)")

    async def _collect_tree(self, root: PurePosixPath) -> tuple[dict[PurePosixPath, FileMeta], list[PurePosixPath]]:
        metas: dict[PurePosixPath, FileMeta] = {}
        relatives: list[PurePosixPath] = []
        async for _, directories, files in self._index.walk(root):
            relatives.extend(self.normalize_path(directory.path).relative_to(root) for directory in directories)
            for file in files:
                path = self.normalize_path(file.path)
                meta = await self._get_file_meta(path)
                if meta is None:
                    raise FileNotFoundError(f"File not found: {path}")
                metas[path] = meta
        return metas, relatives

    @staticmethod
    def _raise_tree_failure(primary: BaseException, rollback_error: BaseException | None) -> None:
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
        if await self._index.exists(directory):
            if not await self._index.is_dir(directory):
                raise FileExistsError(f"Path is a file: {directory}")
            return
        await self._ensure_tree_directory(directory.parent, created)
        await self._index.mkdir(directory, parents=False, exist_ok=False)
        created.add(directory)

    async def _add_tree_rollback_guards(self, chunk_hashes: Iterable[str]) -> dict[str, str]:
        guards: dict[str, str] = {}
        try:
            for chunk_hash in sorted(set(chunk_hashes)):
                temp_ref = f"$tree-rollback-{uuid.uuid4().hex}"
                async with self._lock_chunk(chunk_hash):
                    await self._chunk_add_temp_ref_unlocked(chunk_hash, temp_ref)
                guards[chunk_hash] = temp_ref
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self._release_tree_rollback_guards(guards)
            raise
        return guards

    async def _release_tree_rollback_guards(self, guards: dict[str, str]) -> None:
        for chunk_hash, temp_ref in guards.items():
            async with self._lock_chunk(chunk_hash):
                await self._chunk_remove_temp_ref_unlocked(chunk_hash, temp_ref)

    async def _restore_tree_file(self, path: PurePosixPath, meta: FileMeta) -> None:
        current = await self._get_file_meta(path)
        if current is not None:
            current_hashes = set(current.chunks)
            expected_hashes = set(meta.chunks)
            for chunk_hash in current_hashes - expected_hashes:
                async with self._lock_chunk(chunk_hash):
                    await self._chunk_decref(chunk_hash, path)
        await self._index.mkdir(path.parent, parents=True, exist_ok=True)
        await self._index.upload_bytes(meta.model_dump_json().encode(), path, overwrite=True)
        for chunk_hash in meta.chunks:
            async with self._lock_chunk(chunk_hash):
                refs = await self._chunk_load_refs(chunk_hash) or set()
                if self.normalize_path(path).as_posix() not in refs:
                    await self._chunk_incref(chunk_hash, path)

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
        source_metas, source_dirs = await self._collect_tree(src)
        destination_paths = {dst.joinpath(path.relative_to(src)) for path in source_metas}
        destination_metas: dict[PurePosixPath, FileMeta] = {}
        for path in destination_paths:
            if await self._index.is_dir(path):
                raise IsADirectoryError(f"Destination is a directory: {path}")
            if meta := await self._get_file_meta(path):
                destination_metas[path] = meta

        old_destination_chunks = {chunk_hash for meta in destination_metas.values() for chunk_hash in meta.chunks}
        guards = await self._add_tree_rollback_guards(old_destination_chunks)
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
                    await self._release_tree_rollback_guards(guards)
                except BaseException as error:
                    if rollback_error is None:
                        rollback_error = error
            self._raise_tree_failure(primary, rollback_error)
        else:
            await self._release_tree_rollback_guards(guards)
        return len(source_metas), len(source_dirs)

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        if not await self._index.is_dir(src):
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self._index.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")
        if dst == src or dst.is_relative_to(src):
            raise ValueError("Destination must not be inside the source tree")
        async with self._lock_indexes(f"{src}.tree", f"{dst}.tree"):
            files, directories = await self._apply_tree_transaction(src, dst, move=False)
        self.log.info(
            f"CopyTree complete: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
            f"(<g>{files}</g> files, <g>{directories}</g> dirs)"
        )

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        if not await self._index.is_dir(src):
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self._index.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")
        if dst == src or dst.is_relative_to(src):
            raise ValueError("Destination must not be inside the source tree")
        async with self._lock_indexes(f"{src}.tree", f"{dst}.tree"):
            files, directories = await self._apply_tree_transaction(src, dst, move=True)
        self.log.info(
            f"MoveTree complete: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
            f"(<g>{files}</g> files, <g>{directories}</g> dirs)"
        )

    @override
    async def exists(self, path: PathLike) -> bool:
        return await self._index.exists(self.normalize_path(path))

    @override
    async def is_file(self, path: PathLike) -> bool:
        return await self._index.is_file(self.normalize_path(path))

    @override
    async def is_dir(self, path: PathLike) -> bool:
        return await self._index.is_dir(self.normalize_path(path))

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        path = self.normalize_path(path)
        try:
            stat = await self._index.stat(path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {path}") from e
        if stat.is_dir:
            return stat
        meta_bytes = await self._index.download_bytes(path)
        try:
            meta = FileMeta.model_validate_json(meta_bytes.decode())
        except ValidationError as e:
            raise OSError(f"Corrupted file metadata for {path}") from e
        return meta.info

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        path = self.normalize_path(path)
        if not await self._index.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")
        async for entry in self._index.iterdir(path):
            if entry.is_dir:
                yield FileInfo(
                    path=self.normalize_path(entry.path).as_posix(),
                    name=entry.name,
                    is_dir=True,
                    size=entry.size,
                    modified=entry.modified,
                    created=entry.created,
                )
            else:
                meta = await self._get_file_meta(entry.path)
                if meta is not None:
                    yield meta.info

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[tuple[str, list[FileInfo], list[FileInfo]]]:
        path = self.normalize_path(path)
        if not await self._index.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        async def _fetch_meta(path: PathLike, files: list[FileInfo]) -> None:
            meta = await self._get_file_meta(path)
            if meta is not None:
                files.append(meta.info)

        async for sp, sd, sf in self._index.walk(path):
            files: list[FileInfo] = []
            async with anyio.create_task_group() as tg:
                for entry in sf:
                    tg.start_soon(_fetch_meta, entry.path, files)
            files.sort(key=lambda x: x.name)
            dirs = [
                FileInfo(
                    path=self.normalize_path(d.path).as_posix(),
                    name=d.name,
                    is_dir=True,
                    size=d.size,
                    modified=d.modified,
                    created=d.created,
                )
                for d in sd
            ]
            yield (self.normalize_path(sp).as_posix(), dirs, files)

    @override
    async def list_(self, path: PathLike) -> list[FileInfo]:
        path = self.normalize_path(path)
        if not await self._index.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        async def _fetch_meta(path: PathLike) -> None:
            meta = await self._get_file_meta(path)
            if meta is not None:
                files.append(meta.info)

        files: list[FileInfo] = []
        async with anyio.create_task_group() as tg:
            async for entry in self._index.iterdir(path):
                if entry.is_dir:
                    files.append(
                        FileInfo(
                            path=self.normalize_path(entry.path).as_posix(),
                            name=entry.name,
                            is_dir=True,
                            size=entry.size,
                            modified=entry.modified,
                            created=entry.created,
                        )
                    )
                else:
                    tg.start_soon(_fetch_meta, entry.path)

        files.sort(key=lambda x: x.name)
        return files
