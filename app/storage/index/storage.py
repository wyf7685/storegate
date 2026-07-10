import contextlib
import dataclasses
import hashlib
import uuid
from collections import defaultdict
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
from pydantic import BaseModel

from app.log import escape_tag

from ..abstract import AbstractStorage, BytesLike, FileInfo

BLOCK_SIZE = 64 * 1024 * 1024  # 64 MB
MAX_CONCURRENT_UPLOADS = 2
CHUNKS_INDEX_FILE = "__chunks_index_id__"


def hash_to_path(hash_str: str, suffix: str | None = None) -> str:
    """Convert a hash string to a path with subdirectories."""
    return f"{hash_str[:2]}/{hash_str[2:6]}/{hash_str[6:]}{f".{suffix}" if suffix else ""}"


class FileMeta(BaseModel):
    info: FileInfo
    chunks: list[str]


@final
class IndexStorage(AbstractStorage):
    _index: AbstractStorage | None
    _chunks: AbstractStorage | None
    _block_size: int
    _max_concurrent_uploads: int
    _skip_locking: bool

    def __init__(
        self,
        index: AbstractStorage,
        chunks: AbstractStorage,
        block_size: int = BLOCK_SIZE,
        max_concurrent_uploads: int = MAX_CONCURRENT_UPLOADS,
        skip_locking: bool = False,
    ):
        if index is chunks:
            raise ValueError("Index storage and chunks storage cannot be the same.")

        super().__init__()
        self._index = index
        self._chunks = chunks
        self._block_size = block_size
        self._max_concurrent_uploads = max_concurrent_uploads
        self._skip_locking = skip_locking

    def _ensure_index(self) -> AbstractStorage:
        if self._index is None:
            raise RuntimeError("Index storage is not set.")
        return self._index

    def _ensure_chunks(self) -> AbstractStorage:
        if self._chunks is None:
            raise RuntimeError("Chunks storage is not set.")
        return self._chunks

    @staticmethod
    def _to_abs_path(path: str) -> str:
        if not path.startswith("/"):
            return "/" + path
        return path

    @override
    @property
    def id(self) -> str:
        if self._index is None:
            raise RuntimeError("Index storage is not set.")
        if self._chunks is None:
            raise RuntimeError("Chunks storage is not set.")
        return f"index:{self._index.id}#{self._chunks.id}"

    @override
    async def connect(self) -> None:
        index = self._ensure_index()
        chunks = self._ensure_chunks()
        self.log.info(f"Connecting IndexStorage (index=<c>{index.id}</c>, chunks=<c>{chunks.id}</c>)")
        await index.connect()
        await chunks.connect()

        if not await chunks.exists(CHUNKS_INDEX_FILE):
            await chunks.upload_bytes(index.id.encode(), CHUNKS_INDEX_FILE, overwrite=True)
            self.log.debug(f"Registered chunks storage <c>{chunks.id}</c> → index <c>{index.id}</c>")
        else:
            existing_index_id = (await chunks.download_bytes(CHUNKS_INDEX_FILE)).decode()
            if existing_index_id != index.id:
                self.log.error(
                    f"Chunks storage <c>{chunks.id}</c> is already associated with "
                    f"index <r>{escape_tag(existing_index_id)}</r>, "
                    f"rejecting index <c>{index.id}</c>"
                )
                raise RuntimeError(
                    f"Chunks storage is already associated with a different index storage: {existing_index_id}"
                )
            self.log.debug(f"Chunks storage <c>{chunks.id}</c> already bound to index <c>{existing_index_id}</c>")

    @override
    async def close(self) -> None:
        self.log.debug("Closing IndexStorage")
        await self._ensure_index().close()
        await self._ensure_chunks().close()

    @override
    async def ping(self) -> bool:
        return await self._ensure_index().ping() and await self._ensure_chunks().ping()

    async def _acquire_storage_file_lock(self, storage: AbstractStorage, lock_path: str) -> None:
        _colored_path = f"<y>{escape_tag(lock_path)}</y>"
        if self._skip_locking:
            self.log.trace(f"Lock {_colored_path} disabled, skipping ...")
            return

        while True:
            if await storage.exists(lock_path):
                self.log.trace(f"Lock {_colored_path} is held, waiting ...")
                await anyio.sleep(0.1)
                continue
            try:
                await storage.upload_bytes(b"", lock_path, overwrite=False)
                break
            except FileExistsError:
                self.log.trace(f"Lock {_colored_path} race detected, retrying ...")
                await anyio.sleep(0.1)

        self.log.trace(f"Lock {_colored_path} acquired")

    async def _release_storage_file_lock(self, storage: AbstractStorage, *lock_paths: str) -> None:
        if self._skip_locking or not lock_paths:
            return

        if len(lock_paths) == 1:
            await storage.unlink(lock_paths[0], missing_ok=True)
            self.log.trace(f"Lock <y>{escape_tag(lock_paths[0])}</y> released")
        else:
            await storage.delete_many(*lock_paths)
            self.log.trace(f"Locks <y>{", ".join(escape_tag(p) for p in lock_paths)}</y> released")

    @contextlib.asynccontextmanager
    async def _lock_index(self, index_path: str) -> AsyncGenerator[None]:
        index = self._ensure_index()
        lock_path = f"{index_path}.lock"

        try:
            await self._acquire_storage_file_lock(index, lock_path)
            yield
        finally:
            await self._release_storage_file_lock(index, lock_path)

    @contextlib.asynccontextmanager
    async def _lock_chunk(self, chunk_hash: str) -> AsyncGenerator[None]:
        chunks = self._ensure_chunks()
        lock_path = hash_to_path(chunk_hash, "lock")

        try:
            await self._acquire_storage_file_lock(chunks, lock_path)
            yield
        finally:
            await self._release_storage_file_lock(chunks, lock_path)

    @contextlib.asynccontextmanager
    async def _lock_indexes(self, *index_paths: str) -> AsyncGenerator[None]:
        index = self._ensure_index()
        try:
            for index_path in sorted(set(index_paths)):
                await self._acquire_storage_file_lock(index, f"{index_path}.lock")
            yield
        finally:
            await self._release_storage_file_lock(index, *(f"{index_path}.lock" for index_path in index_paths))

    @contextlib.asynccontextmanager
    async def _lock_chunks(self, chunk_hashes: Iterable[str]) -> AsyncGenerator[None]:
        chunks = self._ensure_chunks()
        try:
            for chunk_hash in sorted(set(chunk_hashes)):
                await self._acquire_storage_file_lock(chunks, hash_to_path(chunk_hash, "lock"))
            yield
        finally:
            await self._release_storage_file_lock(
                chunks, *(hash_to_path(chunk_hash, "lock") for chunk_hash in chunk_hashes)
            )

    async def _get_file_meta(self, path: str) -> FileMeta | None:
        index = self._ensure_index()
        try:
            meta_bytes = await index.download_bytes(path)
        except FileNotFoundError:
            return None
        if not meta_bytes:
            return None
        return FileMeta.model_validate_json(meta_bytes.decode())

    async def _chunk_load_refs(self, chunk_hash: str) -> set[str] | None:
        chunks = self._ensure_chunks()
        ref_path = hash_to_path(chunk_hash, "ref")
        if not await chunks.exists(ref_path):
            return None

        refs_bytes = await chunks.download_bytes(ref_path)
        return set(refs_bytes.decode().splitlines())

    async def _chunk_incref(self, chunk_hash: str, *remote_path: str) -> None:
        chunks = self._ensure_chunks()
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        _colored_remote_paths = ", ".join(f"<i>{escape_tag(p)}</i>" for p in remote_path)

        refs: set[str] = await self._chunk_load_refs(chunk_hash) or set()

        refs.update(remote_path)
        await chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(f"Chunk {_colored_hash} +ref → <g>{len(refs)}</g> ({_colored_remote_paths})")

    async def _chunk_decref(self, chunk_hash: str, *remote_path: str) -> None:
        chunks = self._ensure_chunks()
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        _colored_remote_paths = ", ".join(f"<i>{escape_tag(p)}</i>" for p in remote_path)

        refs = await self._chunk_load_refs(chunk_hash)
        if refs is None:
            self.log.warning(f"Chunk {_colored_hash} ref file missing, skip decref ({_colored_remote_paths})")
            return

        removed = False
        for p in remote_path:
            if p in refs:
                refs.remove(p)
                removed = True
            else:
                self.log.warning(f"Chunk {_colored_hash} ref entry not found for <i>{escape_tag(p)}</i>, skip decref")

        if refs:
            if removed:
                await chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
                self.log.debug(f"Chunk {_colored_hash} -ref → <g>{len(refs)}</g> (<i>{_colored_remote_paths}</i>)")
            else:
                self.log.debug(f"Chunk {_colored_hash} -ref no change (<i>{_colored_remote_paths}</i>)")
        else:
            await chunks.unlink(ref_path, missing_ok=True)
            await chunks.unlink(hash_to_path(chunk_hash, "bin"), missing_ok=True)
            self.log.debug(f"Chunk {_colored_hash} ref=0, deleted data (<i>{_colored_remote_paths}</i>)")

    async def _chunk_transref(self, chunk_hash: str, *pairs: tuple[str, str], missing_ok: bool = False) -> None:
        chunks = self._ensure_chunks()
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"

        refs: set[str] | None = await self._chunk_load_refs(chunk_hash)
        if refs is None:
            if not missing_ok:
                raise FileNotFoundError(f"Chunk {chunk_hash} ref file missing for transref")
            refs = set()

        for src_path, dst_path in pairs:
            if src_path not in refs:
                if missing_ok:
                    self.log.warning(
                        f"Chunk {_colored_hash} ref entry not found for transref: <i>{escape_tag(src_path)}</i>"
                    )
                else:
                    raise FileNotFoundError(f"Chunk {chunk_hash} ref entry not found for transref: {src_path}")

            refs.remove(src_path)
            refs.add(dst_path)

        await chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(
            f"Chunk {_colored_hash} transref: "
            f"{", ".join(f"<i>{escape_tag(src)}</i> → <i>{escape_tag(dst)}</i>" for src, dst in pairs)} "
            f"(<g>{len(refs)}</g> refs)"
        )

    @contextlib.asynccontextmanager
    async def _chunk_temp_ref(self, chunk_hash: str) -> AsyncGenerator[None]:
        chunks = self._ensure_chunks()
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        temp_ref = f"$tempref-{uuid.uuid4().hex[:8]}"

        async with self._lock_chunk(chunk_hash):
            refs = await self._chunk_load_refs(chunk_hash) or set()
            refs.add(temp_ref)
            await chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(f"Chunk {_colored_hash} +tempref → <g>{len(refs)}</g> (<i>{escape_tag(temp_ref)}</i>)")

        try:
            yield
        finally:
            async with self._lock_chunk(chunk_hash):
                refs = await self._chunk_load_refs(chunk_hash) or set()
                if temp_ref in refs:
                    refs.remove(temp_ref)
                    if refs:
                        await chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
                        self.log.debug(
                            f"Chunk {_colored_hash} -tempref → <g>{len(refs)}</g> (<i>{escape_tag(temp_ref)}</i>)"
                        )
                    else:
                        await chunks.unlink(ref_path, missing_ok=True)
                        await chunks.unlink(hash_to_path(chunk_hash, "bin"), missing_ok=True)
                        self.log.debug(f"Chunk {_colored_hash} tempref=0, deleted data (<i>{escape_tag(temp_ref)}</i>)")

    async def _save_chunk_worker(
        self,
        recv: MemoryObjectReceiveStream[tuple[str, bytes, str]],
        incref_done: set[str],
    ) -> None:
        """Worker：从 channel 拉取 block，保存或复用已有分块。"""
        chunks = self._ensure_chunks()
        async for chunk_hash, data, remote_path in recv:
            bin_path = hash_to_path(chunk_hash, "bin")

            async with self._lock_chunk(chunk_hash):
                if not await chunks.exists(bin_path):
                    start = anyio.current_time()
                    await chunks.upload_bytes(data, bin_path)
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
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        if not overwrite and await self.exists(remote_path):
            raise FileExistsError(f"File already exists: {remote_path}")

        _colored_path = f"<y>{escape_tag(remote_path)}</y>"
        self.log.info(f"Upload starting: {_colored_path}")

        index = self._ensure_index()
        self._ensure_chunks()
        chunk_hashes: list[str] = []
        total_size = 0
        incref_done: set[str] = set()
        max_workers = self._max_concurrent_uploads

        try:
            async with self._lock_index(remote_path):
                send, recv = anyio.create_memory_object_stream[tuple[str, bytes, str]](max_workers * 2)

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
                        path=remote_path,
                        name=PurePosixPath(remote_path).name,
                        is_dir=False,
                        size=total_size,
                        modified=now,
                        created=now,
                    ),
                    chunks=chunk_hashes,
                )
                await index.mkdir(
                    PurePosixPath(remote_path).parent.as_posix(),
                    parents=True,
                    exist_ok=True,
                )
                await index.upload_bytes(
                    meta.model_dump_json().encode(),
                    remote_path,
                    overwrite=True,
                )

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
        remote_path: str,
        *,
        offset: int = 0,
    ) -> AsyncIterator[bytes]:
        _colored_path = f"<y>{escape_tag(remote_path)}</y>"
        self.log.debug(f"Download starting: {_colored_path}{f" (offset=<g>{offset}</g>)" if offset else ""}")

        chunks_backend = self._ensure_chunks()
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
                    if not await chunks_backend.exists(bin_path):
                        raise FileNotFoundError(f"Chunk #{idx + 1} {chunk_hash} not found for file {remote_path}")
                    chunk_size = (await chunks_backend.stat(bin_path)).size
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
                    if not await chunks_backend.exists(bin_path):
                        raise FileNotFoundError(f"Chunk #{idx + 1} {chunk_hash} not found for file {remote_path}")
                    hasher = hashlib.sha256()
                    chunk_size = 0
                    local_skip = within_offset if is_target_chunk else 0
                    chunk_start = anyio.current_time()

                    async for chunk in chunks_backend.download_stream(bin_path):
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
    async def unlink(self, path: str, *, missing_ok: bool = False) -> None:
        _colored_path = f"<y>{escape_tag(path)}</y>"
        index = self._ensure_index()

        if await index.is_dir(path):
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
                await index.unlink(path)
        self.log.info(f"Deleted: {_colored_path} (<g>{meta.info.size}</g> bytes, <g>{len(meta.chunks)}</g> chunks)")

    @override
    async def rmdir(self, path: str) -> None:
        index = self._ensure_index()

        if await index.is_file(path):
            raise NotADirectoryError(f"Not a directory: {path}")
        if not await index.is_dir(path):
            raise FileNotFoundError(f"Directory not found: {path}")
        if not await self._is_dir_empty(path):
            raise OSError(f"Directory not empty: {path}")

        await index.rmdir(path)

    @override
    async def move(
        self,
        src: str,
        dst: str,
    ) -> None:
        if src == dst:
            return

        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"Move: {_colored_src} → {_colored_dst}")

        index = self._ensure_index()
        async with self._lock_indexes(src, dst):
            src_meta = await self._get_file_meta(src)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst)
            if dst_meta is not None:
                raise FileExistsError(f"Destination file already exists: {dst}")

            new_dst_meta = FileMeta(
                info=FileInfo(
                    path=dst,
                    name=Path(dst).name,
                    is_dir=False,
                    size=src_meta.info.size,
                    modified=src_meta.info.modified,
                    created=src_meta.info.created,
                ),
                chunks=src_meta.chunks,
            )
            new_dst_meta_bytes = new_dst_meta.model_dump_json().encode()
            async with self._lock_chunks(src_meta.chunks):
                await index.mkdir(Path(dst).parent.as_posix(), parents=True, exist_ok=True)
                await index.upload_bytes(new_dst_meta_bytes, dst, overwrite=True)
                async with anyio.create_task_group() as tg:
                    for chunk_hash in src_meta.chunks:
                        tg.start_soon(self._chunk_transref, chunk_hash, (src, dst))
            await index.unlink(src)

        self.log.info(
            f"Moved: {_colored_src} → {_colored_dst} "
            f"(<g>{src_meta.info.size}</g> bytes, <g>{len(src_meta.chunks)}</g> chunks)"
        )

    @override
    async def copy(
        self,
        src: str,
        dst: str,
    ) -> None:
        if src == dst:
            return

        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"Copy: {_colored_src} → {_colored_dst}")

        index = self._ensure_index()
        async with self._lock_indexes(src, dst):
            src_meta = await self._get_file_meta(src)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst)
            if dst_meta is not None:
                raise FileExistsError(f"Destination file already exists: {dst}")

            new_dst_meta = FileMeta(
                info=dataclasses.replace(src_meta.info, path=dst, name=PurePosixPath(dst).name),
                chunks=src_meta.chunks.copy(),
            )
            new_dst_meta_bytes = new_dst_meta.model_dump_json().encode()
            async with self._lock_chunks(src_meta.chunks):
                await index.mkdir(PurePosixPath(dst).parent.as_posix(), parents=True, exist_ok=True)
                await index.upload_bytes(new_dst_meta_bytes, dst, overwrite=True)
                async with anyio.create_task_group() as tg:
                    for chunk_hash in src_meta.chunks:
                        tg.start_soon(self._chunk_incref, chunk_hash, dst)

        self.log.info(
            f"Copied: {_colored_src} → {_colored_dst} "
            f"(<g>{src_meta.info.size}</g> bytes, <g>{len(src_meta.chunks)}</g> chunks)"
        )

    @override
    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        return await self._ensure_index().mkdir(path, parents=parents, exist_ok=exist_ok)

    @override
    async def rmtree(self, path: str) -> None:
        _colored_path = f"<y>{escape_tag(path)}</y>"
        self.log.info(f"RmTree: {_colored_path}")

        index = self._ensure_index()
        count = 0
        async with anyio.create_task_group() as tg:
            async for info in index.iterdir(path):
                count += 1
                tg.start_soon(self.rmtree if info.is_dir else self.unlink, self._to_abs_path(info.path))
        await index.rmdir(path)
        self.log.info(f"RmTree complete: {_colored_path} (<g>{count}</g> entries removed)")

    @override
    async def copytree(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"CopyTree: {_colored_src} → {_colored_dst}")

        index = self._ensure_index()

        # 类型和策略校验
        if not await index.is_dir(src):
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await index.is_dir(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        src_prefix = src.rstrip("/")
        dst_prefix = dst.rstrip("/")

        # walk 收集源树所有文件
        async def _collect_chunk_updates(info: FileInfo) -> None:
            meta = await self._get_file_meta(info.path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {info.path}")
            files.append(meta)
            file_rel = meta.info.path[len(src_prefix) :]
            for chunk_hash in meta.chunks:
                chunk_updates[chunk_hash].add(dst_prefix + file_rel)

        files: list[FileMeta] = []
        dir_rels: list[str] = []
        chunk_updates: dict[str, set[str]] = defaultdict(set)
        async with anyio.create_task_group() as tg:
            async for _, sd, sf in index.walk(src):
                for info in sf:
                    tg.start_soon(_collect_chunk_updates, info)
                dir_rels.extend(self._to_abs_path(d.path)[len(src_prefix) :] for d in sd)

        # 创建目标目录结构
        await self.mkdir(dst, parents=True, exist_ok=True)
        for rel in sorted(dir_rels, key=lambda r: r.count("/")):
            await self.mkdir(dst_prefix + rel, parents=True, exist_ok=True)

        # 并发更新 chunk refs
        async def _batch_incref(chunk_hash: str, dst_paths: set[str]) -> None:
            async with self._lock_chunk(chunk_hash):
                await self._chunk_incref(chunk_hash, *dst_paths)

        async with anyio.create_task_group() as tg:
            for chunk_hash, dst_paths in chunk_updates.items():
                tg.start_soon(_batch_incref, chunk_hash, dst_paths)

        # 并发创建目标文件元数据
        async def _create_dst_meta(src_meta: FileMeta) -> None:
            dst_file = dst_prefix + src_meta.info.path[len(src_prefix) :]
            async with self._lock_index(dst_file):
                if old_meta := await self._get_file_meta(dst_file):
                    async with self._lock_chunks(old_meta.chunks), anyio.create_task_group() as inner_tg:
                        for chunk_hash in old_meta.chunks:
                            inner_tg.start_soon(self._chunk_decref, chunk_hash, dst_file)
                dst_meta = FileMeta(
                    info=dataclasses.replace(src_meta.info, path=dst_file, name=PurePosixPath(dst_file).name),
                    chunks=src_meta.chunks.copy(),
                )
                dst_meta_bytes = dst_meta.model_dump_json().encode()
                await index.upload_bytes(dst_meta_bytes, dst_meta.info.path, overwrite=True)

        async with anyio.create_task_group() as tg:
            for src_meta in files:
                tg.start_soon(_create_dst_meta, src_meta)

        self.log.info(
            f"CopyTree complete: {_colored_src} → {_colored_dst} "
            f"(<g>{len(files)}</g> files, <g>{len(dir_rels)}</g> dirs)"
        )

    @override
    async def movetree(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        _colored_src = f"<y>{escape_tag(src)}</y>"
        _colored_dst = f"<y>{escape_tag(dst)}</y>"
        self.log.info(f"MoveTree: {_colored_src} → {_colored_dst}")

        index = self._ensure_index()

        # 类型和策略校验
        if not await index.is_dir(src):
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await index.is_dir(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        src_prefix = src.rstrip("/")
        dst_prefix = dst.rstrip("/")

        # walk 收集源树所有文件
        async def _collect(info: FileInfo) -> None:
            meta = await self._get_file_meta(info.path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {info.path}")
            files.append(meta)
            src_file = meta.info.path
            dst_file = dst_prefix + meta.info.path[len(src_prefix) :]
            for chunk_hash in meta.chunks:
                chunk_transrefs[chunk_hash].append((src_file, dst_file))

        files: list[FileMeta] = []
        dir_rels: list[str] = []
        chunk_transrefs: dict[str, list[tuple[str, str]]] = defaultdict(list)
        async with anyio.create_task_group() as tg:
            async for _, sd, sf in index.walk(src):
                for info in sf:
                    tg.start_soon(_collect, info)
                dir_rels.extend(self._to_abs_path(d.path)[len(src_prefix) :] for d in sd)

        # 创建目标目录结构
        await self.mkdir(dst, parents=True, exist_ok=True)
        for rel in sorted(dir_rels, key=lambda r: r.count("/")):
            await self.mkdir(dst_prefix + rel, parents=True, exist_ok=True)

        # 批量 transref: 每个 chunk 一把锁，一次处理全部 (src,dst) 对
        async def _batch_transref(chunk_hash: str, pairs: list[tuple[str, str]]) -> None:
            async with self._lock_chunk(chunk_hash):
                await self._chunk_transref(chunk_hash, *pairs)

        async with anyio.create_task_group() as tg:
            for chunk_hash, pairs in chunk_transrefs.items():
                tg.start_soon(_batch_transref, chunk_hash, pairs)

        # 移动文件 meta (锁 src+dst, 写 dst, 删 src)
        async def _move_meta(src_meta: FileMeta) -> None:
            src_file = src_meta.info.path
            dst_file = dst_prefix + src_file[len(src_prefix) :]
            async with self._lock_indexes(src_file, dst_file):
                # overwrite: 清理目标端旧 chunks
                if old_meta := await self._get_file_meta(dst_file):
                    async with self._lock_chunks(old_meta.chunks), anyio.create_task_group() as inner_tg:
                        for chunk_hash in old_meta.chunks:
                            inner_tg.start_soon(self._chunk_decref, chunk_hash, dst_file)
                dst_meta = FileMeta(
                    info=dataclasses.replace(src_meta.info, path=dst_file, name=PurePosixPath(dst_file).name),
                    chunks=src_meta.chunks.copy(),
                )
                await index.upload_bytes(dst_meta.model_dump_json().encode(), dst_file, overwrite=True)
                await index.unlink(src_file)

        async with anyio.create_task_group() as tg:
            for src_meta in files:
                tg.start_soon(_move_meta, src_meta)

        # 清理源目录结构 (自底向上)
        for rel in sorted(dir_rels, key=lambda r: r.count("/"), reverse=True):
            await index.rmdir(src_prefix + rel)
        await index.rmdir(src)

        self.log.info(
            f"MoveTree complete: {_colored_src} → {_colored_dst} "
            f"(<g>{len(files)}</g> files, <g>{len(dir_rels)}</g> dirs)"
        )

    @override
    async def exists(self, path: str) -> bool:
        return await self._ensure_index().exists(path)

    @override
    async def is_file(self, path: str) -> bool:
        return await self._ensure_index().is_file(path)

    @override
    async def is_dir(self, path: str) -> bool:
        return await self._ensure_index().is_dir(path)

    @override
    async def stat(self, path: str) -> FileInfo:
        index = self._ensure_index()
        try:
            stat = await index.stat(path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {path}") from e
        if stat.is_dir:
            return stat
        meta_bytes = await index.download_bytes(path)
        meta = FileMeta.model_validate_json(meta_bytes.decode())
        return meta.info

    @override
    async def iterdir(self, path: str) -> AsyncIterator[FileInfo]:
        index = self._ensure_index()
        if not await index.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")
        async for entry in index.iterdir(path):
            if entry.is_dir:
                yield FileInfo(
                    path=self._to_abs_path(entry.path),
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
    async def walk(self, path: str) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        index = self._ensure_index()
        if not await index.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        async def _fetch_meta(path: str) -> None:
            meta = await self._get_file_meta(path)
            if meta is not None:
                files.append(meta.info)

        async for sp, sd, sf in index.walk(path):
            files: list[FileInfo] = []
            async with anyio.create_task_group() as tg:
                for entry in sf:
                    tg.start_soon(_fetch_meta, entry.path)
            files.sort(key=lambda x: x.name)
            yield (
                self._to_abs_path(sp),
                [
                    FileInfo(
                        path=self._to_abs_path(d.path),
                        name=d.name,
                        is_dir=True,
                        size=d.size,
                        modified=d.modified,
                        created=d.created,
                    )
                    for d in sd
                ],
                files,
            )

    @override
    async def list_(self, path: str) -> list[FileInfo]:
        index = self._ensure_index()
        if not await index.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        async def _fetch_meta(path: str) -> None:
            meta = await self._get_file_meta(path)
            if meta is not None:
                files.append(meta.info)

        files: list[FileInfo] = []
        async with anyio.create_task_group() as tg:
            async for entry in index.iterdir(path):
                if entry.is_dir:
                    files.append(
                        FileInfo(
                            path=self._to_abs_path(entry.path),
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
