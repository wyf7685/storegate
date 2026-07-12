import contextlib
import itertools
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
import anyio.lowlevel

from app.log import escape_tag
from app.storage.abstract import AbstractStorage, BytesLike, FileInfo, PathLike
from app.utils import ExceptionTranslator, coalesce_chunks, flatten_exception_group

from .cos_client import (
    AsyncCosClient,
    CosClientError,
    CosConfig,
    CosHttpStatusError,
    ListObjectsDir,
    ListObjectsItem,
    MultipartUploadPart,
)
from .utils import MultipartUploadTask, deserialize_file_info, serialize_file_info

UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB
# Files larger than this are copied via multipart upload to stay within
# the PUT Object - Copy 5 GiB limit and to allow parallel part copies.
COPY_MULTIPART_THRESHOLD = 4 * 1024 * 1024  # 4 MiB


translator = ExceptionTranslator(
    bypass=OSError,
    catch=CosClientError,
    default=OSError,
)


@translator.handles(CosHttpStatusError)
def _(exc_group: ExceptionGroup[CosHttpStatusError], msg: str) -> OSError:
    first = next(flatten_exception_group(exc_group))
    return {404: FileNotFoundError, 403: PermissionError}.get(first.status_code, OSError)(f"{msg}: {first}")


@final
class CosStorage(AbstractStorage):
    _client: AsyncCosClient | None = None
    _config: CosConfig

    def __init__(self, config: str | Path | CosConfig) -> None:
        super().__init__()
        self._config = config if isinstance(config, CosConfig) else CosConfig.from_file(config)

    @property
    @override
    def id(self) -> str:
        return f"cos:{self._config.bucket}:{self._config.region}"

    @override
    async def connect(self) -> None:
        self._client = AsyncCosClient(self._config)
        await self._client.__aenter__()
        if not await self.ping():
            raise RuntimeError("Failed to connect to COS bucket. Please check your configuration.")
        self.log.info(f"Connected to bucket <c>{self._config.bucket}</c> in region <c>{self._config.region}</c>")

    @override
    async def close(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            # Use list_objects instead of head_bucket to work with minimal
            # IAM policies (head_bucket requires GetBucket permission).
            async with contextlib.aclosing(self._client.list_objects(max_keys=1)) as agen:
                await anext(agen, None)
        except Exception:
            return False
        else:
            return True

    def _ensure_client(self) -> AsyncCosClient:
        if self._client is None:
            raise RuntimeError("Client is not connected.")
        return self._client

    def _remote_path_to_key(self, remote_path: PathLike) -> str:
        path = PurePosixPath(remote_path)
        if path.is_absolute():
            path = path.relative_to("/")
        return str(path) if path != PurePosixPath(".") else ""

    def _dir_key(self, path: PathLike) -> str | None:
        """返回目录标记对象的 COS 键。

        目录标记对象使用尾随 ``/`` 的键存储。
        根目录（``""``）没有标记对象，返回 ``None``。
        """
        key = self._remote_path_to_key(path)
        if key == "":
            return None
        return key + "/"

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
            if info.is_dir:
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
        client = self._ensure_client()
        key = self._remote_path_to_key(remote_path)
        head = await client.head_object(key=key)
        if head is None:
            raise FileNotFoundError(f"Object not found: {remote_path}")
        total_size = head.content_length

        if offset >= total_size:
            return

        num_chunks = (total_size - offset + DOWNLOAD_CHUNK_SIZE - 1) // DOWNLOAD_CHUNK_SIZE

        self.log.debug(
            f"Download: <y>{escape_tag(key)}</y> (<g>{total_size}</g> bytes, "
            f"offset=<g>{offset}</g>, <g>{num_chunks}</g> chunks)"
        )

        for i in range(num_chunks):
            start = offset + i * DOWNLOAD_CHUNK_SIZE
            end = min(start + DOWNLOAD_CHUNK_SIZE - 1, total_size - 1)
            chunk = await client.get_object(key=key, range=(start, end))
            yield chunk

    @override
    @translator.wrap("Failed to unlink {path} (missing_ok={missing_ok})")
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)

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
        client = self._ensure_client()
        key = self._remote_path_to_key(path)

        # 1. 文件：报错
        if await client.head_object(key=key) is not None:
            raise NotADirectoryError(f"Not a directory: {path}")

        # 2. 目录：检查为空后删除标记对象
        if await self.is_dir(path):
            if not await self._is_dir_empty(path):
                raise OSError(f"Directory not empty: {path}")

            dir_key = self._dir_key(path)
            assert dir_key is not None  # is_dir=True 且不是根目录
            self.log.info(f"Delete dir: <y>{escape_tag(dir_key)}</y>")
            await client.delete_object(key=dir_key)
            return

        # 3. 不存在：静默成功

    @override
    @translator.wrap("Failed to delete {path}")
    async def delete(self, path: PathLike) -> None:
        """Delete a file or an empty directory.

        Uses inline branching to minimize COS API calls, rather than the
        base class convenience method which would require an extra
        ``head_object`` via :meth:`is_dir`.
        """
        client = self._ensure_client()
        key = self._remote_path_to_key(path)

        # 1. 文件：直接删除
        if await client.head_object(key=key) is not None:
            self.log.info(f"Delete: <y>{escape_tag(key)}</y>")
            await client.delete_object(key=key)
            return

        # 2. 目录：检查为空后删除标记对象
        if await self.is_dir(path):
            if not await self._is_dir_empty(path):
                raise OSError(f"Directory not empty: {path}")

            dir_key = self._dir_key(path)
            assert dir_key is not None  # is_dir=True 且不是根目录
            self.log.info(f"Delete dir: <y>{escape_tag(dir_key)}</y>")
            await client.delete_object(key=dir_key)
            return

        # 3. 不存在：静默成功

    @override
    @translator.wrap("Failed to delete objects: {paths}")
    async def delete_many(self, *paths: PathLike) -> None:
        client = self._ensure_client()
        objects_to_delete: list[str] = []

        for path in paths:
            key = self._remote_path_to_key(path)

            # 1. 文件：直接删除
            if await client.head_object(key=key) is not None:
                objects_to_delete.append(key)
                continue

            # 2. 目录：内联检查（复用已计算的 dir_key，避免 is_dir 的冗余 head_object）
            dir_key = self._dir_key(path)
            if dir_key is not None and await client.head_object(key=dir_key) is not None:
                if not await self._is_dir_empty(path):
                    raise OSError(f"Directory not empty: {path}")
                objects_to_delete.append(dir_key)
                continue

            # 3. 不存在：静默成功

        if objects_to_delete:
            self.log.info(f"Delete many: <y>{escape_tag(repr(objects_to_delete))}</y>")
            await client.delete_objects(objects_to_delete)

    @override
    async def move(
        self,
        src: PathLike,
        dst: PathLike,
    ) -> None:
        src_key = self._remote_path_to_key(src)
        dst_key = self._remote_path_to_key(dst)
        self.log.info(f"Move: <y>{escape_tag(src_key)}</y> → <y>{escape_tag(dst_key)}</y>")
        await self.copy(src, dst)
        try:
            await self._ensure_client().delete_object(key=src_key)
        except Exception as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to delete source after move: <y>{escape_tag(src_key)}</y> — <r>{escape_tag(repr(exc))}</r>"
            )
            try:
                await self._ensure_client().delete_object(key=dst_key)
            except Exception:
                self.log.exception(f"Failed to rollback destination after failed move: <y>{escape_tag(dst_key)}</y>")
            raise OSError(f"Failed to delete source after move: {src}") from exc

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(
        self,
        src: PathLike,
        dst: PathLike,
    ) -> None:
        src_key = self._remote_path_to_key(src)
        dst_key = self._remote_path_to_key(dst)
        client = self._ensure_client()

        head = await client.head_object(key=src_key)
        if head is None:
            raise FileNotFoundError(f"Source not found: {src}")

        if head.content_length <= COPY_MULTIPART_THRESHOLD:
            self.log.debug(f"Copy: <y>{escape_tag(src_key)}</y> → <y>{escape_tag(dst_key)}</y> (PUT Object - Copy)")
            await client.put_object_copy(src_key, dst_key)
        else:
            self.log.debug(f"Copy: <y>{escape_tag(src_key)}</y> → <y>{escape_tag(dst_key)}</y> (multipart copy)")
            await self._copy_multipart(src_key, dst_key, head.content_length)

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
            is_dir=True,
            size=0,
            modified=now,
            created=now,
        )
        await client.put_object(key=dir_key, data=serialize_file_info(info))
        self.log.info(f"MkDir: <y>{escape_tag(key)}</y>")

    @override
    @translator.wrap("Failed to remove directory tree {path}")
    async def rmtree(self, path: PathLike) -> None:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        self.log.info(f"RmTree: <y>{escape_tag(key)}</y>")

        if not await self.is_dir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        # 先收集再删除：避免删除操作干扰 walk 的 list_objects 分页迭代
        file_keys: list[str] = []
        dir_paths: list[str] = []
        async for _sp, _sd, sf in self.walk(path):
            file_keys.extend(self._remote_path_to_key(f.path) for f in sf)
            dir_paths.extend(d.path for d in _sd)

        # 批量删除文件
        deleted_files = 0
        for batch in itertools.batched(file_keys, 100):
            if batch:
                await client.delete_objects(batch)
                deleted_files += len(batch)

        # 自底向上删除目录标记对象
        deleted_dirs = 0
        for dir_path in reversed(dir_paths):
            dir_key = self._dir_key(dir_path)
            if dir_key is not None and await client.head_object(key=dir_key) is not None:
                await client.delete_object(key=dir_key)
                deleted_dirs += 1

        # 删除根路径自身的标记对象
        root_dir_key = self._dir_key(path)
        if root_dir_key is not None and await client.head_object(key=root_dir_key) is not None:
            await client.delete_object(key=root_dir_key)
            deleted_dirs += 1

        self.log.info(
            f"RmTree complete: <y>{escape_tag(key)}</y> (<g>{deleted_files}</g> files, <g>{deleted_dirs}</g> dirs)"
        )

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        client = self._ensure_client()

        # 类型和策略校验
        if not await self.is_dir(src):
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await self.is_dir(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        src = self.normalize_path(src)
        dst = self.normalize_path(dst)

        # walk 收集源树所有文件和目录
        file_paths: list[PurePosixPath] = []
        dir_paths: list[PurePosixPath] = []
        async for _sp, _sd, sf in self.walk(src):
            file_paths.extend(self.normalize_path(f.path) for f in sf)
            dir_paths.extend(self.normalize_path(d.path) for d in _sd)

        self.log.info(
            f"CopyTree: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
            f"(<g>{len(file_paths)}</g> files, <g>{len(dir_paths)}</g> subdirs)"
        )

        # 创建目标端所有目录标记 (按深度排序, 自上而下)
        all_dst_dirs: set[PurePosixPath] = {dst}
        for dir_path in dir_paths:
            all_dst_dirs.add(dst.joinpath(dir_path.relative_to(src)))
        for src_file in file_paths:
            all_dst_dirs.add(dst.joinpath(src_file.relative_to(src)).parent)

        dir_created = datetime.now(UTC)
        dir_create_done: set[str] = set()
        try:
            for d in sorted(all_dst_dirs, key=lambda p: len(p.parts)):
                if dir_key := self._dir_key(d):
                    info = FileInfo(
                        path=d.as_posix(),
                        name=d.name,
                        is_dir=True,
                        size=0,
                        modified=dir_created,
                        created=dir_created,
                    )
                    await client.put_object(key=dir_key, data=serialize_file_info(info))
                    dir_create_done.add(dir_key)
        except Exception as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to create directory: <y>{escape_tag(d)}</y> — <r>{escape_tag(repr(exc))}</r>"
            )
            # 回滚已创建的目录标记对象
            try:
                with anyio.CancelScope(shield=True):
                    await client.delete_objects(dir_create_done)
            except Exception:
                self.log.exception(
                    f"Failed to rollback created directories: <y>{escape_tag(repr(dir_create_done))}</y>"
                )
            raise OSError(f"Failed to create directory: {d}") from exc

        # 并发复制所有文件
        started_dst_keys: set[str] = set()
        try:
            async with anyio.create_task_group() as tg:
                for src_file in file_paths:
                    dst_file = dst.joinpath(src_file.relative_to(src))
                    tg.start_soon(self.copy, src_file, dst_file)
                    started_dst_keys.add(self._remote_path_to_key(dst_file))
                    await anyio.lowlevel.checkpoint()
        except Exception as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to copy tree: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y>"
                f" — <r>{escape_tag(repr(exc))}</r>"
            )
            # 回滚已复制的文件
            try:
                with anyio.CancelScope(shield=True):
                    await client.delete_objects(started_dst_keys | dir_create_done)
            except Exception:
                self.log.exception(
                    f"Failed to rollback copied files and created directories: "
                    f"<y>{escape_tag(repr(started_dst_keys | dir_create_done))}</y>"
                )
            raise OSError(f"Failed to copy tree: {src} → {dst}") from exc

    @override
    @translator.wrap("Failed to check existence of {path}")
    async def exists(self, path: PathLike) -> bool:
        key = self._remote_path_to_key(path)
        if key == "":
            return True  # 根目录始终存在

        client = self._ensure_client()

        # 文件
        if await client.head_object(key=key) is not None:
            return True

        # 目录标记
        dir_key = self._dir_key(path)
        if dir_key is not None and await client.head_object(key=dir_key) is not None:  # noqa: SIM103
            return True

        return False

    @override
    @translator.wrap("Failed to check if path is a file: {path}")
    async def is_file(self, path: PathLike) -> bool:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        return await client.head_object(key=key) is not None

    @override
    @translator.wrap("Failed to check if path is a directory: {path}")
    async def is_dir(self, path: PathLike) -> bool:
        key = self._remote_path_to_key(path)
        if key == "":
            return True  # 根目录始终为目录

        dir_key = self._dir_key(path)
        if dir_key is None:  # pragma: no cover
            return False
        return await self._ensure_client().head_object(key=dir_key) is not None

    @override
    @translator.wrap("Failed to stat {path}")
    async def stat(self, path: PathLike) -> FileInfo:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        np = self.normalize_path(path)

        # 根目录：不需要 COS 请求
        if key == "":
            return FileInfo(path=np.as_posix(), name="", is_dir=True, size=0)

        # 1. 尝试作为常规文件
        head = await client.head_object(key=key)
        if head is not None:
            return FileInfo(
                path=np.as_posix(),
                name=np.name,
                size=head.content_length,
                is_dir=False,
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
    def walk(self, path: PathLike) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        key = self._remote_path_to_key(path)
        return self._walk(key)

    async def _iterdir(self, key: str) -> AsyncIterator[FileInfo]:
        client = self._ensure_client()
        key = key.removeprefix("/")
        prefix = (key + "/") if key else None
        async for obj in client.list_objects(prefix=prefix, delimiter="/"):
            if isinstance(obj, ListObjectsDir):
                dir_path = obj.prefix.rstrip("/")
                # 读取标记对象获取完整 FileInfo
                try:
                    body = await client.get_object(key=dir_path + "/")
                except CosHttpStatusError:
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
                is_dir=False,
                size=obj.size,
                modified=obj.last_modified,
            )

    async def _walk(self, key: str) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        dirs: list[FileInfo] = []
        files: list[FileInfo] = []
        async for file in self._iterdir(key):
            (dirs if file.is_dir else files).append(file)

        yield self.normalize_path(key).as_posix(), dirs, files
        for dir in dirs:
            async for sp, sd, sf in self._walk(dir.path):
                yield sp, sd, sf

    @override
    @translator.wrap("Failed to list directory {path}")
    async def list_(self, path: PathLike) -> list[FileInfo]:
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        prefix = (key + "/") if key else None
        infos: list[FileInfo] = []

        async def _fetch_dir_info(dir_path: str) -> None:
            try:
                body = await client.get_object(key=dir_path + "/")
            except CosHttpStatusError:
                # 无标记对象 → 不是合法目录，跳过
                return
            infos.append(deserialize_file_info(self.normalize_path(dir_path).as_posix(), body))

        async with anyio.create_task_group() as tg:
            async for obj in client.list_objects(prefix=prefix, delimiter="/"):
                if isinstance(obj, ListObjectsDir):
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
                        is_dir=False,
                        size=obj.size,
                        modified=obj.last_modified,
                    )
                )

        return infos

    async def _copy_multipart(self, src_key: str, dst_key: str, src_size: int) -> None:
        """Server-side copy via multipart upload for objects above the threshold."""
        client = self._ensure_client()
        num_parts = (src_size + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE
        self.log.debug(
            f"Multipart copy: <y>{escape_tag(src_key)}</y> → <y>{escape_tag(dst_key)}</y> "
            f"(<g>{src_size}</g> bytes in <g>{num_parts}</g> parts)"
        )
        try:
            upload_id = await client.create_multipart_upload(dst_key)
        except Exception as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to create multipart upload: <y>{escape_tag(dst_key)}</y> — <r>{escape_tag(repr(exc))}</r>"
            )
            raise OSError(f"Failed to create multipart upload: {dst_key}") from exc

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
            self.log.debug(f"Multipart copy complete: <y>{escape_tag(dst_key)}</y>")
        except Exception as exc:
            try:
                with anyio.CancelScope(shield=True):
                    await client.abort_multipart_upload(dst_key, upload_id)
            except Exception:
                self.log.exception(f"Failed to abort multipart upload after failed copy: <y>{escape_tag(dst_key)}</y>")
            raise OSError(f"Failed to copy object: {src_key} → {dst_key}") from exc

    @override
    async def _is_dir_empty(self, path: PathLike) -> bool:
        key = self._remote_path_to_key(path)
        prefix = (key + "/") if key else None
        async for item in self._ensure_client().list_objects(prefix=prefix, delimiter="/", max_keys=2):
            # skip directory marker object itself
            if isinstance(item, ListObjectsItem) and item.key.rstrip("/") == key:
                continue
            return False
        return True
