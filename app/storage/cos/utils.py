import contextlib
from collections.abc import AsyncGenerator, AsyncIterable
from typing import Self

import anyio
import anyio.lowlevel
import httpx

from app.log import escape_tag, logger

from .cos_client import AsyncCosClient, MultipartUploadPart

UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB
DEFAULT_TTL_SECS = 3600  # 1 hour


def get_object_key(key: str) -> str:
    return key


class MultipartUploadTask:
    client: AsyncCosClient
    key: str
    upload_id: str
    parts: list[MultipartUploadPart]

    def __init__(self, client: AsyncCosClient, key: str) -> None:
        self.client = client
        self.key = key
        self.upload_id = ""
        self.parts = []
        self._next_part_number = 1
        self._parts_lock = anyio.Lock()

    @property
    def colored_key(self) -> str:
        return f"<i><c>{escape_tag(self.key)}</></>"

    @classmethod
    @contextlib.asynccontextmanager
    async def create(cls, client: AsyncCosClient, key: str) -> AsyncGenerator[Self]:
        self = cls(client, key)
        self.upload_id = await client.create_multipart_upload(self.key)
        logger.opt(colors=True).info(
            f"Created multipart upload for key={self.colored_key} with upload_id=<y>{self.upload_id}</>"
        )

        try:
            yield self
            await self.complete()
        except Exception:
            try:
                await self.abort()
            except Exception as abort_exc:
                logger.opt(exception=abort_exc).warning(f"Failed to abort multipart upload for key={self.key}")
            raise

    def next_part_number(self) -> int:
        value = self._next_part_number
        self._next_part_number += 1
        return value

    async def put_chunk(self, part_number: int, chunk: bytes) -> None:
        logger.opt(colors=True).debug(f"Uploading part <y>{part_number}</> for key={self.colored_key}")

        last_exc = None
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                etag = await self.client.upload_part(
                    key=self.key,
                    data=chunk,
                    part_number=part_number,
                    upload_id=self.upload_id,
                )
            except httpx.RequestError as exc:
                last_exc = exc
                logger.opt(colors=True).warning(
                    f"Attempt <g>{attempt + 1}</> to upload part <y>{part_number}</> "
                    f"for key={self.colored_key} failed: "
                    f"<r>{escape_tag(repr(exc))}</>"
                )
            else:
                break
        else:
            raise RuntimeError(
                f"Failed to upload part {part_number} for key={self.key} after {max_attempts} attempts: {last_exc!r}"
            ) from last_exc

        part: MultipartUploadPart = {
            "PartNumber": part_number,
            "ETag": etag,
        }
        async with self._parts_lock:
            self.parts.append(part)

    async def complete(self) -> None:
        assert all(part["ETag"] for part in self.parts)
        self.parts.sort(key=lambda part: part["PartNumber"])
        await self.client.complete_multipart_upload(key=self.key, upload_id=self.upload_id, parts=self.parts)
        logger.opt(colors=True).info(
            f"Completed multipart upload for key={self.colored_key} "
            f"with upload_id=<y>{self.upload_id}</> and <g>{len(self.parts)}</> parts"
        )

    async def abort(self) -> None:
        await self.client.abort_multipart_upload(key=self.key, upload_id=self.upload_id)
        logger.opt(colors=True).warning(
            f"Aborted multipart upload for key={self.colored_key} with upload_id=<y>{self.upload_id}</>"
        )

    async def upload_from(
        self,
        aiterable: AsyncIterable[bytes],
        max_workers: int = 8,
    ) -> None:
        async def consumer(ait: AsyncIterable[tuple[int, bytes]]) -> None:
            async for part_number, chunk in ait:
                await self.put_chunk(part_number, chunk)

        send, recv = anyio.create_memory_object_stream[tuple[int, bytes]](max(max_workers * 2, 1))
        async with anyio.create_task_group() as tg, send:
            for _ in range(max_workers):
                tg.start_soon(consumer, recv.clone())
            recv.close()
            async for chunk in aiterable:
                await send.send((self.next_part_number(), chunk))
