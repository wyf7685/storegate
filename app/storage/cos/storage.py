import itertools
from collections.abc import AsyncIterable, AsyncIterator
from pathlib import PurePosixPath
from typing import final, override

import anyio
import anyio.lowlevel

from app.storage.abstract import AbstractStorage, BytesLike, FileInfo
from app.storage.cos.cos_client.models import ListObjectsDir

from .cos_client import AsyncCosClient
from .utils import UPLOAD_CHUNK_SIZE, MultipartUploadTask, coalesce_chunks, create_client


@final
class CosStorage(AbstractStorage):
    _client: AsyncCosClient | None = None

    @override
    async def connect(self) -> None:
        self._client = create_client()
        await self._client.__aenter__()
        if not await self._client.head_bucket():
            raise RuntimeError("Failed to connect to COS bucket. Please check your configuration.")

    @override
    async def close(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None

    @override
    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            return await self._client.head_bucket()
        except Exception:
            return False

    def _ensure_client(self) -> AsyncCosClient:
        if self._client is None:
            raise RuntimeError("Client is not connected.")
        return self._client

    def _remote_path_to_key(self, remote_path: str) -> str:
        path = PurePosixPath(remote_path)
        if path.is_absolute():
            path = path.relative_to("/")
        return str(path) if path != PurePosixPath(".") else ""

    @override
    async def upload_bytes(
        self,
        data: BytesLike,
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        buf = memoryview(data).toreadonly()

        async def aiterable() -> AsyncIterable[memoryview[int]]:
            ptr = 0
            while ptr < len(buf):
                yield buf[ptr : ptr + UPLOAD_CHUNK_SIZE]
                ptr += UPLOAD_CHUNK_SIZE
                await anyio.lowlevel.checkpoint()

        await self.upload_stream(aiterable(), remote_path, overwrite=overwrite)

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        if not overwrite and await self.exists(remote_path):
            raise FileExistsError(f"Object already exists: {remote_path}")

        client = self._ensure_client()
        key = self._remote_path_to_key(remote_path)
        chunk_iter = aiter(coalesce_chunks(stream))
        first_chunk = await anext(chunk_iter, None)
        if first_chunk is None:
            await client.put_object(key=key, data=b"")
            return

        second_chunk = await anext(chunk_iter, None)
        if second_chunk is None:
            await client.put_object(key=key, data=first_chunk)
            return

        async with (
            MultipartUploadTask.create(client, key) as task,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(task.put_chunk, task.next_part_number(), first_chunk)
            tg.start_soon(task.put_chunk, task.next_part_number(), second_chunk)
            await anyio.lowlevel.checkpoint()
            await task.upload_from(chunk_iter)

    @override
    async def download_bytes(
        self,
        remote_path: str,
    ) -> bytes:
        client = self._ensure_client()
        key = self._remote_path_to_key(remote_path)
        return await client.get_object(key=key)

    @override
    async def download_stream(
        self,
        remote_path: str,
    ) -> AsyncIterator[bytes]:
        client = self._ensure_client()
        key = self._remote_path_to_key(remote_path)
        head = await client.head_object(key=key)
        if head is None:
            raise FileNotFoundError(f"Object not found: {remote_path}")
        total_size = head.content_length
        num_chunks = (total_size + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE

        for i in range(num_chunks):
            start = i * UPLOAD_CHUNK_SIZE
            end = min(start + UPLOAD_CHUNK_SIZE - 1, total_size - 1)
            chunk = await client.get_object(key=key, range=(start, end))
            yield chunk

    @override
    async def delete(self, path: str) -> None:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        await client.delete_object(key=key)

    @override
    async def move(
        self,
        src: str,
        dst: str,
    ) -> None:
        raise NotImplementedError

    @override
    async def copy(
        self,
        src: str,
        dst: str,
    ) -> None:
        raise NotImplementedError

    @override
    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        return

    @override
    async def rmtree(self, path: str) -> None:
        client = self._ensure_client()

        async for _, files in self.walk(path):
            for batch in itertools.batched(files, 100):
                await client.delete_objects(self._remote_path_to_key(file.path) for file in batch)

    @override
    async def exists(self, path: str) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        else:
            return True

    @override
    async def is_file(self, path: str) -> bool:
        return await self.exists(path)

    @override
    async def is_dir(self, path: str) -> bool:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        async for _ in client.list_objects(prefix=key or None, delimiter="/"):
            return True
        return False

    @override
    async def stat(self, path: str) -> FileInfo:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        head = await client.head_object(key=key)
        if head is None:
            raise FileNotFoundError(f"Object not found: {path}")
        return FileInfo(
            path=path,
            name=PurePosixPath(path).name,
            size=head.content_length,
            is_dir=False,
        )

    @override
    async def list_(self, path: str) -> list[FileInfo]:
        return [item async for item in self.iterdir(path)]

    @override
    def iterdir(self, path: str) -> AsyncIterator[FileInfo]:
        key = self._remote_path_to_key(path)
        return self._iterdir(key)

    @override
    def walk(self, path: str) -> AsyncIterator[tuple[str, list[FileInfo]]]:
        key = self._remote_path_to_key(path)
        return self._walk(key)

    async def _iterdir(self, key: str) -> AsyncIterator[FileInfo]:
        client = self._ensure_client()
        async for obj in client.list_objects(prefix=key or None, delimiter="/"):
            if isinstance(obj, ListObjectsDir):
                yield FileInfo(path=obj.prefix, name=PurePosixPath(obj.prefix).name, size=0, is_dir=True)
                continue

            rel_path = PurePosixPath(obj.key).relative_to(key)
            if len(rel_path.parts) > 1 or not rel_path.name:
                continue
            yield (FileInfo(path=obj.key, name=rel_path.name, is_dir=False, size=obj.size, modified=obj.last_modified))

    async def _walk(self, key: str) -> AsyncIterator[tuple[str, list[FileInfo]]]:
        dirs: list[FileInfo] = []
        files: list[FileInfo] = []
        async for file in self._iterdir(key):
            (dirs if file.is_dir else files).append(file)

        yield key, files
        for dir in dirs:
            async for sub_path, sub_files in self._walk(dir.path):
                yield sub_path, sub_files
