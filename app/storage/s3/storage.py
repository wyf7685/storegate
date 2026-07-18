import contextlib
import itertools
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
import anyio.lowlevel

from app.log import escape_tag
from app.storage.abstract import (
    AbstractStorage,
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    WalkEntry,
    make_cache_identity,
)
from app.utils import ExceptionTranslator, coalesce_chunks, flatten_exception_group

from .client import (
    AsyncS3Client,
    CompletedPart,
    ListObjectsCommonPrefix,
    ListObjectsContents,
    S3ClientError,
    S3Config,
    S3HttpStatusError,
)
from .utils import MultipartUploadTask, deserialize_file_info, serialize_file_info

UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB
# Files larger than this are copied via multipart upload to stay within
# the CopyObject 5 GiB limit and to allow parallel part copies.
COPY_MULTIPART_THRESHOLD = 4 * 1024 * 1024  # 4MB


translator = ExceptionTranslator(
    bypass=OSError,
    catch=S3ClientError,
    default=OSError,
)


@translator.handles(S3HttpStatusError)
def _(exc_group: ExceptionGroup[S3HttpStatusError], msg: str) -> OSError:
    first = next(flatten_exception_group(exc_group))
    return {404: FileNotFoundError, 403: PermissionError}.get(first.status_code, OSError)(f"{msg}: {first}")


@final
class S3Storage(AbstractStorage):
    _client: AsyncS3Client | None = None
    _config: S3Config

    def __init__(self, config: str | Path | S3Config) -> None:
        super().__init__()
        self._config = config if isinstance(config, S3Config) else S3Config.from_file(config)

    @property
    @override
    def id(self) -> str:
        return f"s3:{self._config.bucket}:{self._config.region}"

    @property
    @override
    def cache_identity(self) -> str:
        config = self._config
        return make_cache_identity(
            "s3",
            bucket=config.bucket,
            endpoint_url=config.endpoint_url,
            path_style=config.path_style,
            region=config.region,
            scheme=config.scheme,
        )

    @override
    async def connect(self) -> None:
        if self._client is not None:
            retained = self._client
            with anyio.CancelScope(shield=True):
                await retained.__aexit__(None, None, None)
                if self._client is retained:
                    self._client = None
        client = AsyncS3Client(self._config)
        self._client = client
        try:
            await client.__aenter__()
            if not await self.ping():
                raise RuntimeError("Failed to connect to S3 bucket. Please check your configuration.")
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await client.__aexit__(None, None, None)
                except BaseException as secondary:
                    cleanup_error = secondary
                else:
                    self._client = None
            if cleanup_error is not None:
                self._client = client
                raise BaseExceptionGroup("S3 connection rollback failed", [primary, cleanup_error]) from None
            raise
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

    def _ensure_client(self) -> AsyncS3Client:
        if self._client is None:
            raise RuntimeError("Client is not connected.")
        return self._client

    def _remote_path_to_key(self, remote_path: PathLike) -> str:
        path = PurePosixPath(remote_path)
        if path.is_absolute():
            path = path.relative_to("/")
        return str(path) if path != PurePosixPath(".") else ""

    def _dir_key(self, path: PathLike) -> str | None:
        """返回目录标记对象的 S3 键。

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
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
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
        for path in paths:
            try:
                await self.delete(path)
            except FileNotFoundError:
                continue

    async def _rollback_move_destination(
        self,
        client: AsyncS3Client,
        src_key: str,
        dst_key: str,
        backup_key: str | None,
        *,
        copy_completed: bool,
    ) -> None:
        """Restore a move's source and destination, preserving rollback failures."""
        failures: list[BaseException] = []
        with anyio.CancelScope(shield=True):
            if copy_completed:
                try:
                    if await client.head_object(key=src_key) is None:
                        await client.put_object_copy(dst_key, src_key)
                except BaseException as error:
                    failures.append(error)
            try:
                if backup_key is not None:
                    await client.put_object_copy(backup_key, dst_key)
                    await client.delete_object(key=backup_key)
                elif copy_completed:
                    await client.delete_object(key=dst_key)
            except BaseException as error:
                failures.append(error)
        if not failures:
            return
        if len(failures) == 1:
            raise failures[0]
        raise BaseExceptionGroup("Failed to roll back move destination", failures)

    @staticmethod
    def _raise_move_failure(primary: BaseException, rollback_error: BaseException | None) -> None:
        if rollback_error is None:
            raise primary
        if isinstance(primary, Exception) and isinstance(rollback_error, Exception):
            raise BaseExceptionGroup("Move and rollback failed", [primary, rollback_error]) from None
        if isinstance(rollback_error, anyio.get_cancelled_exc_class()):
            raise rollback_error
        raise primary

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        if self._remote_path_to_key(src) == self._remote_path_to_key(dst):
            if not overwrite:
                raise FileExistsError(f"Source and destination are the same: {src}")
            if await self.is_dir(src):
                raise IsADirectoryError(f"Is a directory: {src}")
            if await self._ensure_client().head_object(key=self._remote_path_to_key(src)) is None:
                raise FileNotFoundError(f"Source not found: {src}")
            return

        client = self._ensure_client()
        src_key = self._remote_path_to_key(src)
        dst_key = self._remote_path_to_key(dst)
        try:
            if await self.is_dir(dst):
                raise IsADirectoryError(f"Destination is a directory: {dst}")
        except TypeError:
            pass
        if not overwrite and await self.exists(dst):
            raise FileExistsError(f"Destination already exists: {dst}")

        backup_key: str | None = None
        if overwrite and await client.head_object(key=dst_key) is not None:
            backup_key = f"{dst_key}.storegate-move-backup-{uuid.uuid4().hex}"
            await client.put_object_copy(dst_key, backup_key)

        try:
            await self.copy(src, dst, overwrite=overwrite)
        except BaseException as primary:
            rollback_error: BaseException | None = None
            if backup_key is not None:
                try:
                    await self._rollback_move_destination(client, src_key, dst_key, backup_key, copy_completed=False)
                except BaseException as error:
                    rollback_error = error
            self._raise_move_failure(primary, rollback_error)

        try:
            await client.delete_object(key=src_key)
        except BaseException as error:
            rollback_error: BaseException | None = None
            try:
                await self._rollback_move_destination(client, src_key, dst_key, backup_key, copy_completed=True)
            except BaseException as rollback_failure:
                rollback_error = rollback_failure
            primary = OSError(f"Failed to delete source after move: {src}")
            primary.__cause__ = error
            self._raise_move_failure(primary, rollback_error)

        if backup_key is not None:
            with anyio.CancelScope(shield=True):
                try:
                    await client.delete_object(key=backup_key)
                except BaseException:
                    self.log.exception(f"Failed to delete move backup: {backup_key}")

    @override
    @translator.wrap("Failed to copy {src} → {dst}")
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_key = self._remote_path_to_key(src)
        dst_key = self._remote_path_to_key(dst)
        client = self._ensure_client()
        if src_key == dst_key:
            if not overwrite:
                raise FileExistsError(f"Source and destination are the same: {src}")
            if await self.is_dir(src):
                raise IsADirectoryError(f"Is a directory: {src}")
            if await client.head_object(key=src_key) is None:
                raise FileNotFoundError(f"Source not found: {src}")
            return
        head = await client.head_object(key=src_key)
        if await self.is_dir(src):
            raise IsADirectoryError(f"Is a directory: {src}")
        if head is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if await self.is_dir(dst):
            raise IsADirectoryError(f"Destination is a directory: {dst}")
        if not overwrite and await client.head_object(key=dst_key) is not None:
            raise FileExistsError(f"Destination already exists: {dst}")
        if head.content_length <= COPY_MULTIPART_THRESHOLD:
            await client.put_object_copy(src_key, dst_key)
        else:
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
            kind=EntryKind.DIRECTORY,
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
        async for walked in self.walk(path):
            file_keys.extend(
                self._remote_path_to_key(entry.path) for entry in walked.entries if entry.kind is EntryKind.FILE
            )
            dir_paths.extend(entry.path for entry in walked.entries if entry.kind is EntryKind.DIRECTORY)

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

    async def _cleanup_copytree_backups(self, client: AsyncS3Client, backup_keys: Iterable[str]) -> None:
        pending = set(backup_keys)
        if not pending:
            return

        last_error: BaseException | None = None
        with anyio.CancelScope(shield=True):
            for _attempt in range(2):
                try:
                    await client.delete_objects(pending)
                except BaseException as exc:
                    last_error = exc

                remaining: set[str] = set()
                for backup_key in pending:
                    try:
                        if await client.head_object(key=backup_key) is not None:
                            remaining.add(backup_key)
                    except BaseException as exc:
                        last_error = exc
                        remaining.add(backup_key)
                if not remaining:
                    return
                pending = remaining

        message = f"Failed to remove copytree backups: {sorted(pending)}"
        if last_error is None:
            raise OSError(message)
        raise OSError(message) from last_error

    async def _stage_copytree_targets(
        self, client: AsyncS3Client, target_keys: set[str]
    ) -> tuple[dict[str, str], set[str]]:
        backups: dict[str, str] = {}
        created_targets: set[str] = set()
        backup_prefix = f".storegate-copytree-backup-{uuid.uuid4().hex}/"
        try:
            for target_key in sorted(target_keys):
                if await client.head_object(key=target_key) is None:
                    created_targets.add(target_key)
                else:
                    backup_key = f"{backup_prefix}{target_key}"
                    await client.put_object_copy(target_key, backup_key)
                    backups[target_key] = backup_key
        except BaseException:
            try:
                await self._cleanup_copytree_backups(client, backups.values())
            except BaseException as cleanup_exc:
                raise OSError(f"Failed to clean staged copytree backups: {cleanup_exc}") from cleanup_exc
            raise
        return backups, created_targets

    async def _rollback_copytree_targets(
        self,
        client: AsyncS3Client,
        backups: dict[str, str],
        created_targets: set[str],
    ) -> None:
        errors: list[str] = []
        first_error: BaseException | None = None
        restored_backup_keys: set[str] = set()
        with anyio.CancelScope(shield=True):
            if created_targets:
                try:
                    await client.delete_objects(created_targets)
                except BaseException as exc:
                    errors.append(f"Failed to remove newly created copytree targets: {exc}")
                    first_error = exc
            for target_key, backup_key in backups.items():
                try:
                    await client.put_object_copy(backup_key, target_key)
                except BaseException as exc:
                    errors.append(f"Failed to restore copytree target {target_key}: {exc}")
                    if first_error is None:
                        first_error = exc
                else:
                    restored_backup_keys.add(backup_key)
            try:
                await self._cleanup_copytree_backups(client, restored_backup_keys)
            except BaseException as exc:
                errors.append(f"Failed to clean restored copytree backups: {exc}")
                if first_error is None:
                    first_error = exc
        if errors:
            message = "Failed to roll back copytree: " + "; ".join(errors)
            if first_error is None:
                raise OSError(message)
            raise OSError(message) from first_error

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
        async for walked in self.walk(src):
            file_paths.extend(
                self.normalize_path(entry.path) for entry in walked.entries if entry.kind is EntryKind.FILE
            )
            dir_paths.extend(
                self.normalize_path(entry.path) for entry in walked.entries if entry.kind is EntryKind.DIRECTORY
            )

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

        dir_keys = {dir_key for directory in all_dst_dirs if (dir_key := self._dir_key(directory)) is not None}
        file_keys = {self._remote_path_to_key(dst.joinpath(src_file.relative_to(src))) for src_file in file_paths}
        backups, created_targets = await self._stage_copytree_targets(client, dir_keys | file_keys)

        dir_created = datetime.now(UTC)

        try:
            for directory in sorted(all_dst_dirs, key=lambda path: len(path.parts)):
                if dir_key := self._dir_key(directory):
                    info = FileInfo(
                        path=directory.as_posix(),
                        name=directory.name,
                        kind=EntryKind.DIRECTORY,
                        size=0,
                        modified=dir_created,
                        created=dir_created,
                    )
                    await client.put_object(key=dir_key, data=serialize_file_info(info))

            async with anyio.create_task_group() as tg:
                for src_file in file_paths:
                    dst_file = dst.joinpath(src_file.relative_to(src))
                    tg.start_soon(self.copy, src_file, dst_file)
                    await anyio.lowlevel.checkpoint()
        except BaseException as exc:
            self.log.error(  # noqa: TRY400
                f"Failed to copy tree: <y>{escape_tag(src)}</y> → <y>{escape_tag(dst)}</y> "
                f"— <r>{escape_tag(repr(exc))}</r>"
            )
            try:
                await self._rollback_copytree_targets(client, backups, created_targets)
            except BaseException as rollback_exc:
                if isinstance(exc, Exception) and isinstance(rollback_exc, Exception):
                    raise BaseExceptionGroup(
                        f"Failed to copy tree: {src} → {dst}; rollback also failed",
                        [exc, rollback_exc],
                    ) from None
                if isinstance(rollback_exc, anyio.get_cancelled_exc_class()):
                    raise
                raise exc from None
            if isinstance(exc, Exception):
                raise OSError(f"Failed to copy tree: {src} → {dst}") from exc
            raise

        await self._cleanup_copytree_backups(client, backups.values())

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
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
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
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
        np = self.normalize_path(path)

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
        client = self._ensure_client()
        key = self._remote_path_to_key(path)
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
            parts: list[CompletedPart] = []
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
            if isinstance(item, ListObjectsContents) and item.key.rstrip("/") == key:
                continue
            return False
        return True
