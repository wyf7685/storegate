import contextlib
import hashlib
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
from pydantic import BaseModel

from app.log import escape_tag
from app.utils import LoggerWrapper

from ..abstract import AbstractStorage, BytesLike, FileInfo

BLOCK_SIZE = 64 * 1024 * 1024  # 64 MB
MAX_CONCURRENT_UPLOADS = 2
CHUNKS_INDEX_FILE = "__chunks_index_id__"


def hash_to_path(hash_str: str, suffix: str | None = None) -> str:
    """Convert a hash string to a path with subdirectories."""
    return f"{hash_str[:2]}/{hash_str[2:6]}/{hash_str[6:]}{f".{suffix}" if suffix else ""}"


@contextlib.asynccontextmanager
async def _storage_file_lock(log: LoggerWrapper, storage: AbstractStorage, lock_path: str) -> AsyncGenerator[None]:
    _colored_path = f"<y>{escape_tag(lock_path)}</y>"

    while True:
        if await storage.exists(lock_path):
            log.trace(f"Lock {_colored_path} is held, waiting …")
            await anyio.sleep(0.1)
            continue
        try:
            await storage.upload_bytes(b"", lock_path, overwrite=False)
            break
        except FileExistsError:
            log.trace(f"Lock {_colored_path} race detected, retrying …")
            await anyio.sleep(0.1)

    log.trace(f"Lock {_colored_path} acquired")
    try:
        yield
    finally:
        await storage.delete(lock_path)
        log.trace(f"Lock {_colored_path} released")


class FileMeta(BaseModel):
    info: FileInfo
    chunks: list[str]


@final
class IndexStorage(AbstractStorage):
    _index: AbstractStorage | None = None
    _chunks: AbstractStorage | None = None
    _block_size: int = BLOCK_SIZE
    _max_concurrent_uploads: int = MAX_CONCURRENT_UPLOADS

    @classmethod
    def from_storage(
        cls,
        index: AbstractStorage,
        chunks: AbstractStorage,
        block_size: int = BLOCK_SIZE,
        max_concurrent_uploads: int = MAX_CONCURRENT_UPLOADS,
    ) -> IndexStorage:
        if index is chunks:
            raise ValueError("Index storage and chunks storage cannot be the same.")
        self = cls()
        self._index = index
        self._chunks = chunks
        self._block_size = block_size
        self._max_concurrent_uploads = max_concurrent_uploads
        return self

    def _ensure_index(self) -> AbstractStorage:
        if self._index is None:
            raise RuntimeError("Index storage is not set.")
        return self._index

    def _ensure_chunks(self) -> AbstractStorage:
        if self._chunks is None:
            raise RuntimeError("Chunks storage is not set.")
        return self._chunks

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

    @contextlib.asynccontextmanager
    async def _lock_index(self, index_path: str) -> AsyncGenerator[None]:
        async with _storage_file_lock(
            self.log,
            storage=self._ensure_index(),
            lock_path=f"{index_path}.lock",
        ):
            yield

    @contextlib.asynccontextmanager
    async def _lock_chunk(self, chunk_hash: str) -> AsyncGenerator[None]:
        async with _storage_file_lock(
            self.log,
            storage=self._ensure_chunks(),
            lock_path=hash_to_path(chunk_hash, "lock"),
        ):
            yield

    @contextlib.asynccontextmanager
    async def _lock_indexes(self, *index_paths: str) -> AsyncGenerator[None]:
        async with contextlib.AsyncExitStack() as stack:
            for index_path in sorted(set(index_paths)):
                await stack.enter_async_context(self._lock_index(index_path))
            yield

    @contextlib.asynccontextmanager
    async def _lock_chunks(self, chunk_hashes: Iterable[str]) -> AsyncGenerator[None]:
        async with contextlib.AsyncExitStack() as stack:
            for chunk_hash in sorted(set(chunk_hashes)):
                await stack.enter_async_context(self._lock_chunk(chunk_hash))
            yield

    async def _get_file_meta(self, path: str, lock: bool = True) -> FileMeta | None:
        index = self._ensure_index()
        async with self._lock_index(path) if lock else contextlib.nullcontext():
            if not await index.exists(path):
                return None
            meta_bytes = await index.download_bytes(path)
            if not meta_bytes:
                return None
            return FileMeta.model_validate_json(meta_bytes.decode())

    async def _chunk_incref(self, chunk_hash: str, remote_path: str) -> None:
        chunks = self._ensure_chunks()
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"

        if await chunks.exists(ref_path):
            refs: set[str] = set((await chunks.download_bytes(ref_path)).decode().splitlines())
        else:
            refs = set()
        refs.add(remote_path)
        await chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(f"Chunk {_colored_hash} +ref → <g>{len(refs)}</g> (<i>{escape_tag(remote_path)}</i>)")

    async def _chunk_decref(self, chunk_hash: str, remote_path: str) -> None:
        chunks = self._ensure_chunks()
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"

        if not await chunks.exists(ref_path):
            self.log.warning(f"Chunk {_colored_hash} ref file missing, skip decref (<i>{escape_tag(remote_path)}</i>)")
            return
        refs = set((await chunks.download_bytes(ref_path)).decode().splitlines())
        if remote_path not in refs:
            self.log.warning(
                f"Chunk {_colored_hash} ref entry not found for <i>{escape_tag(remote_path)}</i>, skip decref"
            )
            return
        refs.remove(remote_path)
        if refs:
            await chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
            self.log.debug(f"Chunk {_colored_hash} -ref → <g>{len(refs)}</g> (<i>{escape_tag(remote_path)}</i>)")
        else:
            await chunks.delete(ref_path)
            await chunks.delete(hash_to_path(chunk_hash, "bin"))
            self.log.debug(f"Chunk {_colored_hash} ref=0, deleted data (<i>{escape_tag(remote_path)}</i>)")

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
                    await chunks.upload_bytes(data, bin_path)
                    self.log.debug(f"Chunk <c>{chunk_hash[:8]}</c> uploaded (<g>{len(data)}</g> bytes)")
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
                send, recv = anyio.create_memory_object_stream[
                    tuple[str, bytes, str]
                ](max_workers * 2)

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
                                    f"complete for {_colored_path}"
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
                            f"complete for {_colored_path}"
                            f" (<g>{len(buffer)}</g> bytes)"
                        )
                        await send.send(
                            (chunk_hash, bytes(buffer), remote_path),
                        )

                # send 关闭 → worker 退出 → tg 退出 → 所有上传完成

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
                f"Upload complete: {_colored_path} "
                f"(<g>{total_size}</g> bytes in <g>{len(chunk_hashes)}</g> chunks)"
            )
        except Exception:
            self.log.error(  # noqa: TRY400
                f"Upload failed: {_colored_path} "
                f"(<g>{total_size}</g> bytes streamed, "
                f"<g>{len(chunk_hashes)}</g> chunks processed)"
            )
            # 回滚已 incref 的分块
            for h in incref_done:
                await self._chunk_decref(h, remote_path)
            raise

    @override
    async def download_stream(
        self,
        remote_path: str,
    ) -> AsyncIterator[bytes]:
        _colored_path = f"<y>{escape_tag(remote_path)}</y>"
        self.log.debug(f"Download starting: {_colored_path}")

        chunks = self._ensure_chunks()
        async with self._lock_index(remote_path):
            meta = await self._get_file_meta(remote_path, lock=False)
            if meta is None:
                raise FileNotFoundError(f"File not found: {remote_path}")
            for idx, chunk_hash in enumerate(meta.chunks):
                bin_path = hash_to_path(chunk_hash, "bin")
                async with self._lock_chunk(chunk_hash):
                    if not await chunks.exists(bin_path):
                        raise FileNotFoundError(f"Chunk not found: {chunk_hash}")
                    hasher = hashlib.sha256()
                    async for chunk in chunks.download_stream(bin_path):
                        hasher.update(chunk)
                        yield chunk
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

    @override
    async def delete(self, path: str) -> None:
        _colored_path = f"<y>{escape_tag(path)}</y>"
        index = self._ensure_index()
        if await index.is_dir(path):
            try:
                await anext(index.iterdir(path))
                raise OSError(f"Directory not empty: {path}")
            except StopAsyncIteration:
                pass
            await index.delete(path)
            return

        async with self._lock_index(path):
            meta = await self._get_file_meta(path, lock=False)
            if meta is None:
                return
            async with self._lock_chunks(meta.chunks):
                async with anyio.create_task_group() as tg:
                    for chunk_hash in meta.chunks:
                        tg.start_soon(self._chunk_decref, chunk_hash, path)
                await index.delete(path)
        self.log.info(f"Deleted: {_colored_path} (<g>{meta.info.size}</g> bytes, <g>{len(meta.chunks)}</g> chunks)")

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
            src_meta = await self._get_file_meta(src, lock=False)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst, lock=False)
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
                        tg.start_soon(self._chunk_incref, chunk_hash, dst)
                async with anyio.create_task_group() as tg:
                    for chunk_hash in src_meta.chunks:
                        tg.start_soon(self._chunk_decref, chunk_hash, src)
            await index.delete(src)

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
            src_meta = await self._get_file_meta(src, lock=False)
            if src_meta is None:
                raise FileNotFoundError(f"Source file not found: {src}")
            dst_meta = await self._get_file_meta(dst, lock=False)
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
                if info.is_dir:
                    tg.start_soon(self.rmtree, info.path)
                else:
                    tg.start_soon(self.delete, info.path)
        await index.delete(path)
        self.log.info(f"RmTree complete: {_colored_path} (<g>{count}</g> entries removed)")

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
        if await self.is_dir(path):
            return await index.stat(path)
        if not await index.is_file(path):
            raise FileNotFoundError(f"File not found: {path}")
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
                yield entry
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
            yield sp, sd, files
