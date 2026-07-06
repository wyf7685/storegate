import contextlib
import hashlib
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import final, override

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
from pydantic import BaseModel

from app.log import escape_tag
from app.utils import LoggerWrapper

from ..abstract import AbstractStorage, BytesLike, FileInfo

BLOCK_SIZE = 256 * 1024 * 1024  # 256 MB
CHUNKS_INDEX_FILE = "__chunks_index_id__"


def hash_to_path(hash_str: str, suffix: str | None = None) -> str:
    """Convert a hash string to a path with subdirectories."""
    return f"{hash_str[:2]}/{hash_str[2:4]}/{hash_str[4:]}{f".{suffix}" if suffix else ""}"


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

    @classmethod
    def from_storage(
        cls,
        index: AbstractStorage,
        chunks: AbstractStorage,
        block_size: int = BLOCK_SIZE,
    ) -> IndexStorage:
        if index is chunks:
            raise ValueError("Index storage and chunks storage cannot be the same.")
        self = cls()
        self._index = index
        self._chunks = chunks
        self._block_size = block_size
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

        async def stream_chunk(
            chunk_id: int,
            stream: MemoryObjectReceiveStream[str | BytesLike],
        ) -> None:
            send, recv = anyio.create_memory_object_stream[BytesLike](4)
            temp_path = f"{uuid.uuid4().hex}.tmp"

            try:
                async with anyio.create_task_group() as tg, stream, send:
                    tg.start_soon(chunks.upload_stream, recv, temp_path)
                    async for item in stream:
                        if isinstance(item, str):
                            chunk_hash = item
                            break
                        await send.send(item)
                    else:
                        tg.cancel_scope.cancel()
                        self.log.debug(f"Chunk #{chunk_id} stream closed without hash for {_colored_path}")
                        return
            except Exception:
                if await chunks.exists(temp_path):
                    await chunks.delete(temp_path)
                raise

            bin_path = hash_to_path(chunk_hash, "bin")
            await chunks.move(temp_path, bin_path)
            await self._chunk_incref(chunk_hash, remote_path)
            chunk_decref.push_async_callback(self._chunk_decref, chunk_hash, remote_path)
            self.log.debug(f"Chunk <c>{chunk_hash[:8]}</c> saved for {_colored_path}")

        index = self._ensure_index()
        chunks = self._ensure_chunks()
        chunk_hashes: list[str] = []
        total_size = 0
        current_chunk_size = 0
        current_chunk_hash = hashlib.sha256()
        chunk_index = 0

        try:
            async with self._lock_index(remote_path):
                c_send, c_recv = anyio.create_memory_object_stream[str | BytesLike](4)
                async with (
                    contextlib.AsyncExitStack() as chunk_lock,
                    contextlib.AsyncExitStack() as chunk_decref,
                ):
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(stream_chunk, chunk_index + 1, c_recv)
                        async for chunk in stream:
                            total_size += len(chunk)
                            current_chunk_size += len(chunk)
                            if current_chunk_size < self._block_size:
                                current_chunk_hash.update(chunk)
                                await c_send.send(chunk)
                                continue
                            chunk = memoryview(chunk)
                            remaining = self._block_size - (current_chunk_size - len(chunk))
                            current_chunk_hash.update(chunk[:remaining])
                            await c_send.send(chunk[:remaining])
                            chunk_hash = current_chunk_hash.hexdigest()
                            chunk_hashes.append(chunk_hash)
                            chunk_index += 1
                            await chunk_lock.enter_async_context(self._lock_chunk(chunk_hash))
                            await c_send.send(chunk_hash)
                            c_send.close()
                            self.log.debug(
                                f"Chunk #{chunk_index} <c>{chunk_hash[:8]}</c> "
                                f"complete for {_colored_path}"
                                f" (<g>{self._block_size}</g> bytes)"
                            )
                            current_chunk_size = len(chunk) - remaining
                            current_chunk_hash = hashlib.sha256()
                            current_chunk_hash.update(chunk[remaining:])
                            c_send, c_recv = anyio.create_memory_object_stream[str | BytesLike](4)
                            tg.start_soon(stream_chunk, chunk_index + 1, c_recv)
                            await c_send.send(chunk[remaining:])

                        if current_chunk_size > 0:
                            chunk_hash = current_chunk_hash.hexdigest()
                            chunk_hashes.append(chunk_hash)
                            chunk_index += 1
                            await chunk_lock.enter_async_context(self._lock_chunk(chunk_hash))
                            await c_send.send(chunk_hash)
                            c_send.close()
                            self.log.debug(
                                f"Chunk #{chunk_index} <c>{chunk_hash[:8]}</c> "
                                f"complete for {_colored_path}"
                                f" (<g>{current_chunk_size}</g> bytes)"
                            )
                        else:
                            c_send.close()

                    chunk_decref.pop_all()

                now = datetime.now(UTC)
                meta = FileMeta(
                    info=FileInfo(
                        path=remote_path,
                        name=Path(remote_path).name,
                        is_dir=False,
                        size=total_size,
                        modified=now,
                        created=now,
                    ),
                    chunks=chunk_hashes,
                )
                await index.mkdir(Path(remote_path).parent.as_posix(), parents=True, exist_ok=True)
                await index.upload_bytes(meta.model_dump_json().encode(), remote_path, overwrite=True)

            self.log.info(
                f"Upload complete: {_colored_path} (<g>{total_size}</g> bytes in <g>{chunk_index}</g> chunks)"
            )
        except Exception:
            self.log.error(  # noqa: TRY400
                f"Upload failed: {_colored_path} "
                f"(<g>{total_size}</g> bytes streamed, "
                f"<g>{chunk_index}</g> chunks written)"
            )
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
            for chunk_hash in meta.chunks:
                bin_path = hash_to_path(chunk_hash, "bin")
                async with self._lock_chunk(chunk_hash):
                    if not await chunks.exists(bin_path):
                        raise FileNotFoundError(f"Chunk not found: {chunk_hash}")
                    async for chunk in chunks.download_stream(bin_path):
                        yield chunk

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
        async with self._lock_index(path):
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
