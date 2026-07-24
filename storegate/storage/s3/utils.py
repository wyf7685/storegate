import contextlib
import json
from collections.abc import AsyncGenerator, AsyncIterable
from datetime import datetime
from typing import Self

import anyio
import anyio.lowlevel

from storegate.log import escape_tag
from storegate.storage.abstract import EntryKind, FileInfo
from storegate.utils import httpx, logger_wrapper

from .client import AsyncS3Client, CompletedPart


def serialize_file_info(info: FileInfo) -> bytes:
    """将 ``FileInfo`` 序列化为 JSON bytes，用作目录标记对象的值。"""
    if info.kind is not EntryKind.DIRECTORY:
        raise ValueError("S3 directory markers require directory FileInfo")
    data: dict[str, object] = {
        "path": info.path,
        "name": info.name,
        "kind": EntryKind.DIRECTORY.value,
        "size": info.size,
    }
    if info.modified is not None:
        data["modified"] = info.modified.isoformat()
    if info.created is not None:
        data["created"] = info.created.isoformat()
    return json.dumps(data, separators=(",", ":")).encode("utf-8")


def deserialize_file_info(path: str, data: bytes) -> FileInfo:
    """从 JSON bytes 反序列化 ``FileInfo``（仅用于目录标记对象）。"""
    obj = json.loads(data.decode("utf-8"))
    kind = EntryKind(obj["kind"])
    if kind is not EntryKind.DIRECTORY:
        raise ValueError(f"Invalid S3 directory marker kind: {kind.value}")
    modified = datetime.fromisoformat(obj["modified"]) if "modified" in obj else None
    created = datetime.fromisoformat(obj["created"]) if "created" in obj else None
    return FileInfo(
        path=obj.get("path", path),
        name=obj.get("name", ""),
        kind=kind,
        size=obj.get("size", 0),
        modified=modified,
        created=created,
    )


class MultipartUploadTask:
    client: AsyncS3Client
    key: str
    upload_id: str
    parts: list[CompletedPart]

    def __init__(self, client: AsyncS3Client, key: str) -> None:
        self.client = client
        self.key = key
        self.upload_id = ""
        self.parts = []
        self._next_part_number = 1
        self._parts_lock = anyio.Lock()
        self.log = logger_wrapper(f"s3.multipart <i><c>{escape_tag(self.key)}</></>")

    @classmethod
    @contextlib.asynccontextmanager
    async def create(cls, client: AsyncS3Client, key: str) -> AsyncGenerator[Self]:
        self = cls(client, key)
        self.upload_id = await client.create_multipart_upload(self.key)
        self.log.info(f"Created multipart upload with upload_id=<y>{self.upload_id}</>")

        try:
            yield self
            await self.complete()
        except BaseException as primary:
            abort_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await self.abort()
                except BaseException as secondary:
                    abort_error = secondary
            if abort_error is not None:
                self.log.warning(f"Failed to abort multipart upload for {self.key}", exception=abort_error)
                # Always re-raise primary-first so CancelScope cannot drop the cancel cause.
                raise BaseExceptionGroup(
                    "S3 multipart upload failed and abort failed",
                    [primary, abort_error],
                ) from None
            raise

    def next_part_number(self) -> int:
        value = self._next_part_number
        self._next_part_number += 1
        return value

    async def put_chunk(self, part_number: int, chunk: bytes) -> None:
        self.log.debug(f"Uploading #<y>{part_number}</>")

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
                self.log.warning(
                    f"Attempt <g>{attempt + 1}</> to upload #<y>{part_number}</> failed: <r>{escape_tag(repr(exc))}</>"
                )
            else:
                break
        else:
            raise RuntimeError(
                f"Failed to upload part {part_number} for {self.key} after {max_attempts} attempts: {last_exc!r}"
            ) from last_exc

        part: CompletedPart = {
            "PartNumber": part_number,
            "ETag": etag,
        }
        async with self._parts_lock:
            self.parts.append(part)

    async def complete(self) -> None:
        assert all(part["ETag"] for part in self.parts)
        self.parts.sort(key=lambda part: part["PartNumber"])
        await self.client.complete_multipart_upload(key=self.key, upload_id=self.upload_id, parts=self.parts)
        self.log.info(
            f"Completed multipart upload with upload_id=<y>{self.upload_id}</> and <g>{len(self.parts)}</> parts"
        )

    async def abort(self) -> None:
        await self.client.abort_multipart_upload(key=self.key, upload_id=self.upload_id)
        self.log.warning(f"Aborted multipart upload with upload_id=<y>{self.upload_id}</>")

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
