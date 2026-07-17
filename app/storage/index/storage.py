import contextlib
import dataclasses
import functools
import hashlib
import json
import math
import uuid
from collections import defaultdict
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
        await index.connect()
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
                f"index <r>{escape_tag(existing)}</r>, "
                f"rejecting index <c>{index.id}</c>"
            )
            raise RuntimeError(f"Chunks storage is already associated with a different index storage: {existing}")
        else:
            self.log.debug(f"Chunks storage <c>{chunks.id}</c> already bound to index <c>{existing}</c>")

    @override
    async def close(self) -> None:
        self.log.debug("Closing IndexStorage")
        await self._index.close()
        await self._chunks.close()

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

    async def _renew_storage_file_lock(self, lease: _LockLease) -> None:
        interval = self._lock_lease / 3
        margin = min(interval / 2, self._lock_lease / 10)
        key = f"{lease.storage.id}:{lease.storage.normalize_path(lease.path)}"
        while True:
            await anyio.sleep(interval)
            while True:
                remaining = (lease.expires - datetime.now(UTC)).total_seconds()
                if remaining <= margin:
                    return
                try:
                    with anyio.fail_after(remaining - margin):
                        async with self._local_lock_guard(key):
                            try:
                                current = await lease.storage.download_bytes(lease.path)
                                data = json.loads(current.decode())
                                if data.get("owner") != lease.owner:
                                    return
                                expires = datetime.now(UTC) + timedelta(seconds=self._lock_lease)
                                data["expires"] = expires.isoformat()
                                payload = json.dumps(data, separators=(",", ":")).encode()
                                # Re-read before overwrite: without CAS, a cross-process
                                # replacement can still happen after this comparison.
                                if await lease.storage.download_bytes(lease.path) != current:
                                    return
                                await lease.storage.upload_bytes(payload, lease.path, overwrite=True)
                                lease = dataclasses.replace(lease, expires=expires)
                            except FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError:
                                return
                    break
                except TimeoutError:
                    await anyio.lowlevel.checkpoint()

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
                            # A backend may commit before its upload call returns. Cover the
                            # entire bounded handoff plus one lease so a slow successful
                            # handoff cannot return an already-stale record.
                            expires = now + timedelta(seconds=handoff_timeout + self._lock_lease)
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
                                    # The local guard serializes same-process recovery. The
                                    # equality check narrows, but cannot eliminate, the final
                                    # cross-process replace-before-unlink race without CAS.
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
    ) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        if src == dst:
            return

        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"Move: {_colored_src} → {_colored_dst}")

        async with self._lock_indexes(src, dst):
            src_meta = await self._get_file_meta(src)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst)
            if dst_meta is not None:
                raise FileExistsError(f"Destination file already exists: {dst}")

            new_dst_meta = FileMeta(
                info=FileInfo(
                    path=dst.as_posix(),
                    name=dst.name,
                    is_dir=False,
                    size=src_meta.info.size,
                    modified=src_meta.info.modified,
                    created=src_meta.info.created,
                ),
                chunks=src_meta.chunks,
            )
            new_dst_meta_bytes = new_dst_meta.model_dump_json().encode()

            async def rollback_chunks() -> None:
                try:
                    with anyio.CancelScope(shield=True):
                        async with anyio.create_task_group() as tg:
                            for chunk_hash in src_meta.chunks:
                                pfunc = functools.partial(
                                    self._chunk_transref,
                                    chunk_hash,
                                    (dst, src),
                                    missing_ok=True,
                                )
                                tg.start_soon(pfunc)
                except Exception:
                    self.log.exception(f"Failed to rollback chunks for move: {_colored_src} → {_colored_dst}")

            async with self._lock_chunks(src_meta.chunks):
                try:
                    async with anyio.create_task_group() as tg:
                        for chunk_hash in src_meta.chunks:
                            tg.start_soon(self._chunk_transref, chunk_hash, (src, dst))
                except Exception as exc:
                    self.log.error(  # noqa: TRY400
                        f"Failed to transref chunks for move: {_colored_src} → {_colored_dst} "
                        f"— <r>{escape_tag(repr(exc))}</r>"
                    )
                    await rollback_chunks()
                    raise

                try:
                    await self._index.mkdir(dst.parent, parents=True, exist_ok=True)
                    await self._index.upload_bytes(new_dst_meta_bytes, dst, overwrite=True)
                except Exception as exc:
                    self.log.error(  # noqa: TRY400
                        f"Failed to upload metadata for move: {_colored_src} → {_colored_dst} "
                        f"— <r>{escape_tag(repr(exc))}</r>"
                    )
                    await rollback_chunks()
                    raise

            await self._index.unlink(src)

        self.log.info(
            f"Moved: {_colored_src} → {_colored_dst} "
            f"(<g>{src_meta.info.size}</g> bytes, <g>{len(src_meta.chunks)}</g> chunks)"
        )

    @override
    async def copy(
        self,
        src: PathLike,
        dst: PathLike,
    ) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        if src == dst:
            return

        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"Copy: {_colored_src} → {_colored_dst}")

        async with self._lock_indexes(src, dst):
            src_meta = await self._get_file_meta(src)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst)
            if dst_meta is not None:
                raise FileExistsError(f"Destination file already exists: {dst}")

            new_dst_meta = FileMeta(
                info=dataclasses.replace(src_meta.info, path=dst.as_posix(), name=dst.name),
                chunks=src_meta.chunks.copy(),
            )
            new_dst_meta_bytes = new_dst_meta.model_dump_json().encode()

            async def rollback_chunks() -> None:
                try:
                    with anyio.CancelScope(shield=True):
                        async with anyio.create_task_group() as tg:
                            for chunk_hash in src_meta.chunks:
                                tg.start_soon(self._chunk_decref, chunk_hash, dst)
                except Exception:
                    self.log.exception(f"Failed to rollback chunks for {_colored_dst}")

            async with self._lock_chunks(src_meta.chunks):
                try:
                    async with anyio.create_task_group() as tg:
                        for chunk_hash in src_meta.chunks:
                            tg.start_soon(self._chunk_incref, chunk_hash, dst)
                except Exception as exc:
                    self.log.error(f"Failed to incref chunks for {_colored_dst}: — <r>{escape_tag(repr(exc))}</r>")  # noqa: TRY400
                    await rollback_chunks()
                    raise

                try:
                    await self._index.mkdir(dst.parent, parents=True, exist_ok=True)
                    await self._index.upload_bytes(new_dst_meta_bytes, dst, overwrite=True)
                except Exception as exc:
                    self.log.error(f"Failed to upload metadata for {_colored_dst}: — <r>{escape_tag(repr(exc))}</r>")  # noqa: TRY400
                    await rollback_chunks()
                    raise

        self.log.info(
            f"Copied: {_colored_src} → {_colored_dst} "
            f"(<g>{src_meta.info.size}</g> bytes, <g>{len(src_meta.chunks)}</g> chunks)"
        )

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

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"CopyTree: {_colored_src} → {_colored_dst}")

        # 类型和策略校验
        if not await self._index.is_dir(src):
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self._index.is_dir(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        # walk 收集源树所有文件
        async def collect_chunk_updates(info: FileInfo) -> None:
            meta = await self._get_file_meta(info.path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {info.path}")
            files.append(meta)
            file_rel = self.normalize_path(meta.info.path).relative_to(src)
            for chunk_hash in meta.chunks:
                chunk_updates[chunk_hash].add(dst.joinpath(file_rel))

        files: list[FileMeta] = []
        dir_rels: list[PurePosixPath] = []
        chunk_updates: dict[str, set[PathLike]] = defaultdict(set)
        async with anyio.create_task_group() as tg:
            async for _, sd, sf in self._index.walk(src):
                for info in sf:
                    tg.start_soon(collect_chunk_updates, info)
                dir_rels.extend(self.normalize_path(d.path).relative_to(src) for d in sd)

        # 创建目标目录结构
        await self._index.mkdir(dst, parents=True, exist_ok=True)
        for rel in sorted(dir_rels, key=lambda r: len(r.parts)):
            await self._index.mkdir(dst.joinpath(rel), parents=True, exist_ok=True)

        # 并发更新 chunk refs
        async def batch_incref(chunk_hash: str, dst_paths: set[PathLike]) -> None:
            async with self._lock_chunk(chunk_hash):
                await self._chunk_incref(chunk_hash, *dst_paths)

        async with anyio.create_task_group() as tg:
            for chunk_hash, dst_paths in chunk_updates.items():
                tg.start_soon(batch_incref, chunk_hash, dst_paths)

        # 并发创建目标文件元数据
        async def create_dst_meta(src_meta: FileMeta) -> None:
            dst_file = dst.joinpath(self.normalize_path(src_meta.info.path).relative_to(src))
            async with self._lock_index(dst_file):
                if old_meta := await self._get_file_meta(dst_file):
                    async with self._lock_chunks(old_meta.chunks), anyio.create_task_group() as inner_tg:
                        for chunk_hash in old_meta.chunks:
                            inner_tg.start_soon(self._chunk_decref, chunk_hash, dst_file)
                dst_meta = FileMeta(
                    info=dataclasses.replace(
                        src_meta.info,
                        path=dst_file.as_posix(),
                        name=dst_file.name,
                    ),
                    chunks=src_meta.chunks.copy(),
                )
                dst_meta_bytes = dst_meta.model_dump_json().encode()
                await self._index.upload_bytes(dst_meta_bytes, dst_meta.info.path, overwrite=True)

        async with anyio.create_task_group() as tg:
            for src_meta in files:
                tg.start_soon(create_dst_meta, src_meta)

        self.log.info(
            f"CopyTree complete: {_colored_src} → {_colored_dst} "
            f"(<g>{len(files)}</g> files, <g>{len(dir_rels)}</g> dirs)"
        )

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"MoveTree: {_colored_src} → {_colored_dst}")

        # 类型和策略校验
        if not await self._index.is_dir(src):
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self._index.is_dir(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        # walk 收集源树所有文件
        async def collect_chunks(info: FileInfo) -> None:
            meta = await self._get_file_meta(info.path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {info.path}")
            files.append(meta)
            src_file = self.normalize_path(meta.info.path)
            dst_file = dst.joinpath(src_file.relative_to(src))
            for chunk_hash in meta.chunks:
                chunk_transrefs[chunk_hash].add((src_file, dst_file))

        files: list[FileMeta] = []
        dir_rels: list[PurePosixPath] = []
        chunk_transrefs: dict[str, set[tuple[PathLike, PathLike]]] = defaultdict(set)
        async with anyio.create_task_group() as tg:
            async for _, sd, sf in self._index.walk(src):
                for info in sf:
                    tg.start_soon(collect_chunks, info)
                dir_rels.extend(self.normalize_path(d.path).relative_to(src) for d in sd)

        # 创建目标目录结构
        await self.mkdir(dst, parents=True, exist_ok=True)
        for rel in sorted(dir_rels, key=lambda r: len(r.parts)):
            await self.mkdir(dst.joinpath(rel).as_posix(), parents=True, exist_ok=True)

        # 批量 transref: 每个 chunk 一把锁，一次处理全部 (src,dst) 对
        async def batch_transref(chunk_hash: str, pairs: set[tuple[PathLike, PathLike]]) -> None:
            async with self._lock_chunk(chunk_hash):
                await self._chunk_transref(chunk_hash, *pairs)

        async with anyio.create_task_group() as tg:
            for chunk_hash, pairs in chunk_transrefs.items():
                tg.start_soon(batch_transref, chunk_hash, pairs)

        # 移动文件 meta (锁 src+dst, 写 dst, 删 src)
        async def move_meta(src_meta: FileMeta) -> None:
            src_file = self.normalize_path(src_meta.info.path)
            dst_file = dst.joinpath(src_file.relative_to(src))
            async with self._lock_indexes(src_file, dst_file):
                # overwrite: 清理目标端旧 chunks
                if old_meta := await self._get_file_meta(dst_file):
                    async with self._lock_chunks(old_meta.chunks), anyio.create_task_group() as inner_tg:
                        for chunk_hash in old_meta.chunks:
                            inner_tg.start_soon(self._chunk_decref, chunk_hash, dst_file)
                dst_meta = FileMeta(
                    info=dataclasses.replace(
                        src_meta.info,
                        path=dst_file.as_posix(),
                        name=dst_file.name,
                    ),
                    chunks=src_meta.chunks.copy(),
                )
                await self._index.upload_bytes(dst_meta.model_dump_json().encode(), dst_file, overwrite=True)
                await self._index.unlink(src_file)

        async with anyio.create_task_group() as tg:
            for src_meta in files:
                tg.start_soon(move_meta, src_meta)

        # 清理源目录结构 (自底向上)
        for rel in sorted(dir_rels, key=lambda r: len(r.parts), reverse=True):
            await self._index.rmdir(src.joinpath(rel))
        await self._index.rmdir(src)

        self.log.info(
            f"MoveTree complete: {_colored_src} → {_colored_dst} "
            f"(<g>{len(files)}</g> files, <g>{len(dir_rels)}</g> dirs)"
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
