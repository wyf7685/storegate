from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import final, override

import anyio
import anyio.lowlevel

from storegate.log import escape_tag
from storegate.storage.abstract import (
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    VersionedBytes,
    WalkEntry,
    validate_download_offset,
)
from storegate.utils import coalesce_chunks

from ._base import (
    COPY_MULTIPART_THRESHOLD as COPY_MULTIPART_THRESHOLD,
)
from ._base import (
    COPYTREE_MAX_WORKERS as COPYTREE_MAX_WORKERS,
)
from ._base import (
    UPLOAD_CHUNK_SIZE as UPLOAD_CHUNK_SIZE,
)
from ._base import (
    translator as translator,
)
from ._transfer import S3TransferMixin
from ._tree import S3TreeMixin
from .client import ListObjectsCommonPrefix, ListObjectsContents, S3ClientError, S3HttpStatusError
from .utils import MultipartUploadTask, deserialize_file_info, serialize_file_info


@final
class S3Storage(S3TreeMixin, S3TransferMixin):
    """Object storage over the S3 API with marker-object directory emulation.

    Whole-tree transactions live in :mod:`._tree` and single-object move/copy
    in :mod:`._transfer`; shared state, lifecycle and key mapping live in
    :mod:`._base`.
    """

    @override
    @translator.wrap("Failed to read versioned object {path}")
    async def read_versioned(self, path: PathLike) -> VersionedBytes | None:
        key = self._remote_path_to_key(path)
        client = self._ensure_client()
        # Read data and ETag from one GET so the token identifies the returned bytes.
        try:
            async with client.stream_get(key) as response:
                etag = response.headers.get("ETag")
                if etag is None or etag == "":
                    raise S3ClientError("Missing ETag in versioned GET response")
                data = await response.aread()
        except S3HttpStatusError as exc:
            if exc.status_code == 404:
                if await self.is_dir(path):
                    raise IsADirectoryError(f"Not a regular file: {path}") from None
                return None
            raise
        return VersionedBytes(data=data, token=etag)

    @override
    @translator.wrap("Failed to compare-exchange {path}")
    async def compare_exchange(
        self,
        path: PathLike,
        *,
        expected_token: str | None,
        data: BytesLike,
    ) -> VersionedBytes | None:
        path = self.normalize_path(path)
        client = self._ensure_client()
        key = self._remote_path_to_key(path)

        # Refuse directory markers / directory targets before conditional PUT.
        # Conflict path must not create parents or perform other writes.
        if await self.is_dir(path):
            raise IsADirectoryError(f"Is a directory: {path}")

        payload = bytes(data)
        headers = {"If-None-Match": "*"} if expected_token is None else {"If-Match": expected_token}

        try:
            etag = await client.put_object(key=key, data=payload, headers=headers)
        except S3HttpStatusError as exc:
            if exc.status_code == 412:
                return None
            raise

        return VersionedBytes(data=payload, token=etag)

    @override
    @translator.wrap("Failed to upload stream to {remote_path} (overwrite={overwrite})")
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
            if info.kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            if not overwrite:
                raise FileExistsError(f"File already exists: {remote_path}")

        client = self._ensure_client()
        key = self._remote_path_to_key(remote_path)
        self.log.info(f"Upload: <y>{escape_tag(key)}</y>")
        await self.mkdir(remote_path.parent.as_posix(), parents=True, exist_ok=True)

        chunk_iter = aiter(coalesce_chunks(stream, UPLOAD_CHUNK_SIZE))
        first_chunk = await anext(chunk_iter, None)
        if first_chunk is None:
            self.log.debug(f"Upload: <y>{escape_tag(key)}</y> — empty object")
            await client.put_object(key=key, data=b"")
            return

        second_chunk = await anext(chunk_iter, None)
        if second_chunk is None:
            self.log.debug(f"Upload: <y>{escape_tag(key)}</y> — single chunk (<g>{len(first_chunk)}</g> bytes)")
            await client.put_object(key=key, data=first_chunk)
            return

        self.log.debug(f"Upload: <y>{escape_tag(key)}</y> — multipart upload")
        async with (
            MultipartUploadTask.create(client, key) as task,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(task.put_chunk, task.next_part_number(), first_chunk)
            tg.start_soon(task.put_chunk, task.next_part_number(), second_chunk)
            await anyio.lowlevel.checkpoint()
            await task.upload_from(chunk_iter)

    @override
    @translator.wrap_agen("Failed to download stream from {remote_path} (offset={offset})")
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        offset = validate_download_offset(offset)
        key = self._remote_path_to_key(remote_path)
        client = self._ensure_client()
        head = await client.head_object(key=key)
        if head is None:
            # Preserve directory/404 contract: directory markers are not downloadable files.
            if await self.is_dir(remote_path):
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            raise FileNotFoundError(f"Object not found: {remote_path}")
        total_size = head.content_length

        if offset >= total_size:
            return

        range_start = offset if offset > 0 else None
        self.log.debug(
            f"Download: <y>{escape_tag(key)}</y> (<g>{total_size}</g> bytes, offset=<g>{offset}</g>, single stream GET)"
        )
        async with client.stream_get(key, range_start=range_start) as response:
            async for chunk in response.aiter_bytes():
                if chunk:
                    yield chunk

    @override
    @translator.wrap("Failed to unlink {path} (missing_ok={missing_ok})")
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        key = self._remote_path_to_key(path)
        client = self._ensure_client()

        # 1. 文件：直接删除
        if await client.head_object(key=key) is not None:
            self.log.info(f"Delete file: <y>{escape_tag(key)}</y>")
            await client.delete_object(key=key)
            return

        # 2. 目录：报错
        if await self.is_dir(path):
            raise IsADirectoryError(f"Is a directory: {path}")

        # 3. 不存在
        if not missing_ok:
            raise FileNotFoundError(f"File not found: {path}")

    @override
    @translator.wrap("Failed to remove directory {path}")
    async def rmdir(self, path: PathLike) -> None:
        key = self._remote_path_to_key(path)
        client = self._ensure_client()
        if await client.head_object(key=key) is not None:
            raise NotADirectoryError(f"Not a directory: {path}")
        if await self.is_dir(path):
            if not await self._is_dir_empty(path):
                raise OSError(f"Directory not empty: {path}")
            dir_key = self._dir_key(path)
            if dir_key is not None:
                await client.delete_object(key=dir_key)
            return
        raise FileNotFoundError(f"Directory not found: {path}")

    @override
    @translator.wrap("Failed to delete {path}")
    async def delete(self, path: PathLike) -> None:
        key = self._remote_path_to_key(path)
        client = self._ensure_client()
        if await client.head_object(key=key) is not None:
            await client.delete_object(key=key)
            return
        if await self.is_dir(path):
            if not await self._is_dir_empty(path):
                raise OSError(f"Directory not empty: {path}")
            dir_key = self._dir_key(path)
            if dir_key is not None:
                await client.delete_object(key=dir_key)
            return
        raise FileNotFoundError(f"Path not found: {path}")

    @override
    @translator.wrap("Failed to delete objects: {paths}")
    async def delete_many(self, *paths: PathLike) -> None:
        normalized = tuple(self.normalize_path(path) for path in paths)
        for path in normalized:
            try:
                await self.delete(path)
            except FileNotFoundError:
                continue

    @override
    @translator.wrap("Failed to create directory {path} (parents={parents}, exist_ok={exist_ok})")
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        key = self._remote_path_to_key(path)

        # 根目录始终存在
        if key == "":
            if exist_ok:
                return
            raise FileExistsError("Root directory already exists")

        client = self._ensure_client()
        dir_key = self._dir_key(path)
        assert dir_key is not None  # key != "" 保证了这一点

        # 冲突检查：同名文件
        if await client.head_object(key=key) is not None:
            raise FileExistsError(f"Path is a file: {path}")

        # 重复检查：标记对象已存在
        if await client.head_object(key=dir_key) is not None:
            if exist_ok:
                return
            raise FileExistsError(f"Directory already exists: {path}")

        # 父目录处理
        parent = PurePosixPath(path).parent
        parent_path = parent.as_posix()
        parent_is_root = parent_path in (".", "/", "")
        if parents and not parent_is_root:
            await self.mkdir(parent_path, parents=True, exist_ok=True)
        elif not parent_is_root and not await self.is_dir(parent_path):
            raise FileNotFoundError(f"Parent directory not found: {parent_path}")

        # 创建标记对象
        now = datetime.now(UTC)
        np = self.normalize_path(path)
        info = FileInfo(
            path=np.as_posix(),
            name=np.name,
            kind=EntryKind.DIRECTORY,
            size=0,
            modified=now,
            created=now,
        )
        await client.put_object(key=dir_key, data=serialize_file_info(info))
        self.log.info(f"MkDir: <y>{escape_tag(key)}</y>")

    @override
    @translator.wrap("Failed to check existence of {path}")
    async def exists(self, path: PathLike) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    @override
    @translator.wrap("Failed to check if path is a file: {path}")
    async def is_file(self, path: PathLike) -> bool:
        key = self._remote_path_to_key(path)
        client = self._ensure_client()
        return await client.head_object(key=key) is not None

    @override
    @translator.wrap("Failed to check if path is a directory: {path}")
    async def is_dir(self, path: PathLike) -> bool:
        try:
            info = await self.stat(path)
        except FileNotFoundError:
            return False
        return info.kind is EntryKind.DIRECTORY

    @override
    @translator.wrap("Failed to stat {path}")
    async def stat(self, path: PathLike) -> FileInfo:
        key = self._remote_path_to_key(path)
        np = self.normalize_path(path)
        client = self._ensure_client()

        # 根目录：不需要 S3 请求
        if key == "":
            return FileInfo(path=np.as_posix(), name="", kind=EntryKind.DIRECTORY, size=0)

        # 1. 尝试作为常规文件
        head = await client.head_object(key=key)
        if head is not None:
            return FileInfo(
                path=np.as_posix(),
                name=np.name,
                kind=EntryKind.FILE,
                size=head.content_length,
                modified=head.last_modified,
            )

        # 2. 尝试作为目录（读取标记对象）
        dir_key = self._dir_key(path)
        if dir_key is not None:
            head = await client.head_object(key=dir_key)
            if head is not None:
                body = await client.get_object(key=dir_key)
                return deserialize_file_info(np.as_posix(), body)

        raise FileNotFoundError(f"Object not found: {path}")

    @override
    @translator.wrap_agen("Failed to iterate directory {path}")
    def iterdir(self, path: PathLike) -> AsyncIterator[FileInfo]:
        key = self._remote_path_to_key(path)
        return self._iterdir(key)

    @override
    @translator.wrap_agen("Failed to walk directory {path}")
    async def walk(self, path: PathLike) -> AsyncIterator[WalkEntry]:
        key = self._remote_path_to_key(path)
        try:
            info = await self.lstat(path)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {path}") from None
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        async for entry in self._walk(key):
            yield entry

    async def _iterdir(self, key: str) -> AsyncIterator[FileInfo]:
        client = self._ensure_client()
        key = key.removeprefix("/")
        prefix = (key + "/") if key else None
        async for obj in client.list_objects(prefix=prefix, delimiter="/"):
            if isinstance(obj, ListObjectsCommonPrefix):
                dir_path = obj.prefix.rstrip("/")
                # 读取标记对象获取完整 FileInfo
                try:
                    body = await client.get_object(key=dir_path + "/")
                except S3HttpStatusError:
                    # 无标记对象 → 不是合法目录，跳过
                    continue
                yield deserialize_file_info(self.normalize_path(dir_path).as_posix(), body)
                continue

            # 提取直接子级名称
            #   "test/foo/bar.txt" → "foo/bar.txt"
            #   "foo/bar.txt" → "foo/bar.txt"
            rest = obj.key[len(key) + 1 :] if key else obj.key
            # 跳过：非直接子级 (rest 含 "/") 或目录自身的标记对象 (rest 为空)
            if not rest or "/" in rest:
                continue

            yield FileInfo(
                path=self.normalize_path(obj.key).as_posix(),
                name=rest,
                kind=EntryKind.FILE,
                size=obj.size,
                modified=obj.last_modified,
            )

    async def _walk(self, key: str) -> AsyncIterator[WalkEntry]:
        entries = tuple(sorted([entry async for entry in self._iterdir(key)], key=lambda entry: entry.path))
        yield WalkEntry(path=self.normalize_path(key).as_posix(), entries=entries)
        for entry in entries:
            if entry.kind is EntryKind.DIRECTORY:
                async for walked in self._walk(entry.path):
                    yield walked

    @override
    @translator.wrap("Failed to list directory {path}")
    async def list_(self, path: PathLike) -> list[FileInfo]:
        key = self._remote_path_to_key(path)
        client = self._ensure_client()
        prefix = (key + "/") if key else None
        infos: list[FileInfo] = []

        async def _fetch_dir_info(dir_path: str) -> None:
            try:
                body = await client.get_object(key=dir_path + "/")
            except S3HttpStatusError:
                # 无标记对象 → 不是合法目录，跳过
                return
            infos.append(deserialize_file_info(self.normalize_path(dir_path).as_posix(), body))

        async with anyio.create_task_group() as tg:
            async for obj in client.list_objects(prefix=prefix, delimiter="/"):
                if isinstance(obj, ListObjectsCommonPrefix):
                    dir_path = obj.prefix.rstrip("/")
                    tg.start_soon(_fetch_dir_info, dir_path)
                    continue

                # 提取直接子级名称
                #   "test/foo/bar.txt" → "foo/bar.txt"
                #   "foo/bar.txt" → "foo/bar.txt"
                rest = obj.key[len(key) + 1 :] if key else obj.key
                # 跳过：非直接子级 (rest 含 "/") 或目录自身的标记对象 (rest 为空)
                if not rest or "/" in rest:
                    continue

                infos.append(
                    FileInfo(
                        path=self.normalize_path(obj.key).as_posix(),
                        name=rest,
                        kind=EntryKind.FILE,
                        size=obj.size,
                        modified=obj.last_modified,
                    )
                )

        return infos

    @override
    async def _is_dir_empty(self, path: PathLike) -> bool:
        key = self._remote_path_to_key(path)
        prefix = (key + "/") if key else None
        async for item in self._ensure_client().list_objects(prefix=prefix, delimiter="/", max_keys=2):
            # skip directory marker object itself
            if isinstance(item, ListObjectsContents) and item.key.rstrip("/") == key:
                continue
            return False
        return True
