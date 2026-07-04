import contextlib
import itertools
from collections.abc import AsyncIterable, AsyncIterator
from pathlib import PurePosixPath
from typing import final, override

import anyio
import anyio.lowlevel

from app.config import CosConfig
from app.log import logger
from app.storage.abstract import AbstractStorage, BytesLike, FileInfo
from app.storage.cos.cos_client.models import ListObjectsDir, MultipartUploadPart
from app.utils import coalesce_chunks

from .cos_client import AsyncCosClient
from .utils import UPLOAD_CHUNK_SIZE, MultipartUploadTask, create_client

# Files larger than this are copied via multipart upload to stay within
# the PUT Object - Copy 5 GiB limit and to allow parallel part copies.
COPY_MULTIPART_THRESHOLD = 4 * 1024 * 1024  # 4 MiB


@final
class CosStorage(AbstractStorage):
    _client: AsyncCosClient | None = None
    _config: CosConfig

    def __init__(self, config: CosConfig) -> None:
        self._config = config

    @classmethod
    def from_config(cls, config: CosConfig) -> CosStorage:
        """Create a ``CosStorage`` from a ``CosConfig``."""
        return cls(config)

    @override
    @property
    def id(self) -> str:
        return f"cos:{self._config.bucket}:{self._config.region}"

    @override
    async def connect(self) -> None:
        self._client = create_client(self._config)
        await self._client.__aenter__()
        await self.ping()

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
            # Use list_objects instead of head_bucket to work with minimal
            # IAM policies (head_bucket requires GetBucket permission).
            with contextlib.suppress(StopAsyncIteration):  # bucket exists but is empty — still healthy
                await anext(self._client.list_objects(max_keys=1))
        except Exception:
            return False
        else:
            return True

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
        await self.copy(src, dst)
        await self.delete(src)

    @override
    async def copy(
        self,
        src: str,
        dst: str,
    ) -> None:
        src_key = self._remote_path_to_key(src)
        dst_key = self._remote_path_to_key(dst)
        client = self._ensure_client()

        head = await client.head_object(key=src_key)
        if head is None:
            raise FileNotFoundError(f"Source not found: {src}")

        if head.content_length <= COPY_MULTIPART_THRESHOLD:
            await client.put_object_copy(src_key, dst_key)
        else:
            await self._copy_multipart(src_key, dst_key, head.content_length)

    @override
    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        logger.debug(f"COS is flat — skipping mkdir for '{path}'")

    @override
    async def rmtree(self, path: str) -> None:
        client = self._ensure_client()

        async for _, _, files in self.walk(path):
            for batch in itertools.batched(files, 100):
                await client.delete_objects(self._remote_path_to_key(file.path) for file in batch)

        # Also delete the object at the path itself (COS "directory marker" object).
        if await self.is_file(path):
            await client.delete_object(self._remote_path_to_key(path))

    @override
    async def exists(self, path: str) -> bool:
        return await self.is_file(path) or await self.is_dir(path)

    @override
    async def is_file(self, path: str) -> bool:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        return await client.head_object(key=key) is not None

    @override
    async def is_dir(self, path: str) -> bool:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        prefix = (key + "/") if key else None
        async for _ in client.list_objects(prefix=prefix, delimiter="/", max_keys=1):
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
    def iterdir(self, path: str) -> AsyncIterator[FileInfo]:
        key = self._remote_path_to_key(path)
        return self._iterdir(key)

    @override
    def walk(self, path: str) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        key = self._remote_path_to_key(path)
        return self._walk(key)

    async def _iterdir(self, key: str) -> AsyncIterator[FileInfo]:
        client = self._ensure_client()
        prefix = (key + "/") if key else None
        async for obj in client.list_objects(prefix=prefix, delimiter="/"):
            if isinstance(obj, ListObjectsDir):
                yield FileInfo(
                    path=obj.prefix.rstrip("/"),
                    name=PurePosixPath(obj.prefix.rstrip("/")).name,
                    size=0,
                    is_dir=True,
                )
                continue

            # Extract the immediate child name from the full COS key.
            if key:  # noqa: SIM108
                rest = obj.key[len(key) + 1 :]  # "test/foo/bar.txt" → "foo/bar.txt"
            else:
                rest = obj.key  # "foo/bar.txt" → "foo/bar.txt"

            if "/" in rest:
                continue  # not an immediate child

            yield FileInfo(
                path=obj.key,
                name=rest,
                is_dir=False,
                size=obj.size,
                modified=obj.last_modified,
            )

    async def _walk(self, key: str) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        dirs: list[FileInfo] = []
        files: list[FileInfo] = []
        async for file in self._iterdir(key):
            (dirs if file.is_dir else files).append(file)

        yield key, dirs, files
        for dir in dirs:
            async for sub_path, sub_dirs, sub_files in self._walk(dir.path):
                yield sub_path, sub_dirs, sub_files

    async def _copy_multipart(self, src_key: str, dst_key: str, src_size: int) -> None:
        """Server-side copy via multipart upload for objects above the threshold."""
        client = self._ensure_client()
        upload_id = await client.create_multipart_upload(dst_key)

        try:
            parts: list[MultipartUploadPart] = []
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
        except Exception:
            with contextlib.suppress(Exception):
                await client.abort_multipart_upload(dst_key, upload_id)
            raise
