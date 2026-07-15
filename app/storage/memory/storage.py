import itertools
from collections.abc import AsyncGenerator, AsyncIterable
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import final, override

from app.storage.abstract import AbstractStorage, BytesLike, FileInfo, PathLike

_sid = itertools.count()


@final
class MemoryStorage(AbstractStorage):
    """In-memory storage backend for testing / ephemeral use.

    Usage::

        async with MemoryStorage() as storage:
            await storage.upload_bytes(b"hello", "foo.txt")
    """

    def __init__(self, root: str | None = None) -> None:
        super().__init__()
        _root = PurePosixPath("/", root) if root is not None else PurePosixPath("/")
        # Normalise away double-leading-slash when root == "/".
        self._root: PurePosixPath = PurePosixPath(str(_root).replace("//", "/"))
        self._files: dict[str, bytes] = {}  # path → content
        self._dirs: set[str] = set()  # directory paths
        self._now: float = datetime.now(tz=UTC).timestamp()
        self._id: int = next(_sid)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    @override
    def id(self) -> str:
        return f"memory:{self._id}:{self._root.as_posix()}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        pass

    @override
    async def close(self) -> None:
        pass

    @override
    async def ping(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _resolve(self, path: PathLike) -> str:
        """Normalise *path* relative to ``self._root``."""
        resolved = self._root.joinpath(self.normalize_path(path).relative_to("/")).as_posix()
        return "" if resolved in (".", "/") else resolved

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_parent_dirs(self, path: str) -> None:
        """Record all ancestor directories of *path*."""
        parent = PurePosixPath(path).parent
        parts = parent.parts
        for i in range(1, len(parts) + 1):
            self._dirs.add(str(PurePosixPath(*parts[:i])))

    def _is_dir(self, path: str) -> bool:
        if path == "" or path in self._dirs:
            return True
        prefix = path + "/" if path else ""
        return any(k.startswith(prefix) for k in self._files)

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        try:
            info = await self.stat(remote_path)
        except FileNotFoundError:
            pass
        else:
            if info.is_dir:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            if not overwrite:
                raise FileExistsError(f"File already exists: {remote_path}")

        target = self._resolve(remote_path)
        self._ensure_parent_dirs(target)

        buffer = bytearray()
        async for chunk in stream:
            buffer.extend(chunk if isinstance(chunk, (bytes, bytearray, memoryview)) else memoryview(chunk))

        self._files[target] = bytes(buffer)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        target = self._resolve(remote_path)

        try:
            data = self._files[target]
        except KeyError:
            raise FileNotFoundError(f"File not found: {remote_path}") from None

        if offset >= len(data):
            return

        remaining = data[offset:]
        step = 1024 * 1024
        for i in range(0, len(remaining), step):
            yield remaining[i : i + step]

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        target = self._resolve(path)

        if target in self._files:
            del self._files[target]
            return

        if target in self._dirs or target == "" or self._is_dir(target):
            raise IsADirectoryError(f"Is a directory: {path}")

        if missing_ok:
            return

        raise FileNotFoundError(f"File not found: {path}")

    @override
    async def rmdir(self, path: PathLike) -> None:
        target = self._resolve(path)

        if target in self._files:
            raise NotADirectoryError(f"Not a directory: {path}")
        if target not in self._dirs and target != "" and not self._is_dir(target):
            raise FileNotFoundError(f"Directory not found: {path}")

        if target in self._dirs:
            self._dirs.discard(target)
        prefix = target + "/" if target else ""
        # Only allow deletion of empty directories.
        for key in self._files:
            if key.startswith(prefix):
                raise OSError(f"Directory not empty: {path}")
        for key in self._dirs:
            if key != target and key.startswith(prefix):
                raise OSError(f"Directory not empty: {path}")

    @override
    async def move(self, src: PathLike, dst: PathLike) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)

        if source not in self._files:
            raise FileNotFoundError(f"Source not found: {src}")
        if dest in self._dirs:
            raise FileExistsError(f"Destination is an existing directory: {dst}")
        if dest in self._files:
            raise FileExistsError(f"Destination file already exists: {dst}")

        self._ensure_parent_dirs(dest)
        self._files[dest] = self._files.pop(source)

    @override
    async def copy(self, src: PathLike, dst: PathLike) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)

        if source not in self._files:
            raise FileNotFoundError(f"Source not found: {src}")
        if dest in self._dirs:
            raise FileExistsError(f"Destination is an existing directory: {dst}")
        if dest in self._files:
            raise FileExistsError(f"Destination file already exists: {dst}")

        self._ensure_parent_dirs(dest)
        self._files[dest] = self._files[source]

    # ------------------------------------------------------------------
    # Directory
    # ------------------------------------------------------------------

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        target = self._resolve(path)

        if target in self._files:
            raise FileExistsError(f"Path is a file: {path}")
        if target in self._dirs:
            if exist_ok:
                return
            raise FileExistsError(f"Directory already exists: {path}")

        if parents:
            self._ensure_parent_dirs(target)

        parent = str(PurePosixPath(target).parent) if target else ""
        if parent in (".", "/"):
            parent = ""
        if target and parent != "" and parent not in self._dirs:
            raise FileNotFoundError(f"Parent directory not found: {path}")

        self._dirs.add(target or "")

    @override
    async def rmtree(self, path: PathLike) -> None:
        target = self._resolve(path)

        if target in self._files:
            raise NotADirectoryError(f"Not a directory: {path}")

        prefix = target + "/" if target else ""

        for key in list(self._files):
            if key == target or key.startswith(prefix):
                del self._files[key]

        for key in list(self._dirs):
            if key == target or key.startswith(prefix):
                self._dirs.discard(key)

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        target_src = self._resolve(src)
        target_dst = self._resolve(dst)

        # 类型校验
        if not (target_src in self._dirs or target_src == "" or self._is_dir(target_src)):
            raise NotADirectoryError(f"Not a directory: {src}")

        # 覆盖策略
        if not overwrite and (
            target_dst in self._dirs or target_dst == "" or self._is_dir(target_dst) or target_dst in self._files
        ):
            raise FileExistsError(f"Destination already exists: {dst}")

        src_prefix = target_src + "/" if target_src else ""

        # 收集源下所有文件和目录
        files_to_copy: list[tuple[str, str]] = []
        for key in self._files:
            if key == target_src or key.startswith(src_prefix):
                rel = key[len(src_prefix) :] if target_src else key
                dst_path = target_dst + "/" + rel if target_dst else rel
                files_to_copy.append((key, dst_path))

        dirs_to_create: list[str] = []
        for key in self._dirs:
            if key == target_src:
                dirs_to_create.append(target_dst)
            elif key.startswith(src_prefix):
                rel = key[len(src_prefix) :] if target_src else key
                dirs_to_create.append(target_dst + "/" + rel)

        # overwrite: 清理目标端已有数据
        if overwrite:
            dst_prefix = target_dst + "/" if target_dst else ""
            for key in list(self._files):
                if key == target_dst or key.startswith(dst_prefix):
                    del self._files[key]
            for key in list(self._dirs):
                if key == target_dst or key.startswith(dst_prefix):
                    self._dirs.discard(key)

        # 创建目标目录结构
        for d in dirs_to_create:
            self._dirs.add(d)
        if target_dst and target_dst not in self._dirs:
            self._dirs.add(target_dst)
        self._ensure_parent_dirs(target_dst)

        # 复制所有文件 (bytes 不可变, 共享引用安全)
        for src_path, dst_path in files_to_copy:
            self._files[dst_path] = self._files[src_path]

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        target_src = self._resolve(src)
        target_dst = self._resolve(dst)

        # 类型校验
        if not (target_src in self._dirs or target_src == "" or self._is_dir(target_src)):
            raise NotADirectoryError(f"Not a directory: {src}")

        # 覆盖策略
        if not overwrite and (
            target_dst in self._dirs or target_dst == "" or self._is_dir(target_dst) or target_dst in self._files
        ):
            raise FileExistsError(f"Destination already exists: {dst}")

        src_prefix = target_src + "/" if target_src else ""

        # overwrite: 清理目标端已有数据
        if overwrite:
            dst_prefix = target_dst + "/" if target_dst else ""
            for key in list(self._files):
                if key == target_dst or key.startswith(dst_prefix):
                    del self._files[key]
            for key in list(self._dirs):
                if key == target_dst or key.startswith(dst_prefix):
                    self._dirs.discard(key)

        # 移动文件 (键重命名)
        moved_files: dict[str, bytes] = {}
        for key in list(self._files):
            if key == target_src or key.startswith(src_prefix):
                rel = key[len(src_prefix) :] if target_src else key
                dst_path = target_dst + "/" + rel if target_dst else rel
                moved_files[dst_path] = self._files.pop(key)
        self._files.update(moved_files)

        # 移动目录
        for key in list(self._dirs):
            if key == target_src:
                self._dirs.discard(key)
                self._dirs.add(target_dst)
            elif key.startswith(src_prefix):
                rel = key[len(src_prefix) :] if target_src else key
                dst_path = target_dst + "/" + rel if target_dst else rel
                self._dirs.discard(key)
                self._dirs.add(dst_path)

        self._ensure_parent_dirs(target_dst)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @override
    async def exists(self, path: PathLike) -> bool:
        target = self._resolve(path)
        return target in self._files or target in self._dirs or self._is_dir(target)

    @override
    async def is_file(self, path: PathLike) -> bool:
        return self._resolve(path) in self._files

    @override
    async def is_dir(self, path: PathLike) -> bool:
        target = self._resolve(path)
        if target in self._dirs:
            return True
        return target == "" or self._is_dir(target)

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        target = self._resolve(path)
        name = PurePosixPath(target).name if target else ""

        if target in self._files:
            return FileInfo(
                path=self.normalize_path(path).as_posix(),
                name=name,
                is_dir=False,
                size=len(self._files[target]),
                modified=datetime.fromtimestamp(self._now, tz=UTC),
                created=datetime.fromtimestamp(self._now, tz=UTC),
            )

        if target in self._dirs or target == "" or self._is_dir(target):
            return FileInfo(
                path=self.normalize_path(path).as_posix(),
                name=name,
                is_dir=True,
                size=0,
                modified=datetime.fromtimestamp(self._now, tz=UTC),
                created=datetime.fromtimestamp(self._now, tz=UTC),
            )

        raise FileNotFoundError(f"Path not found: {path}")

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        target = self._resolve(path)

        # Allow listing root that has files but hasn't been explicitly mkdir'd.
        if target not in self._dirs and target != "" and not self._is_dir(target):
            raise NotADirectoryError(f"Not a directory: {path}")

        seen: set[str] = set()
        prefix = target + "/" if target else ""

        # Yield subdirectories.
        for d in self._dirs:
            if d == target or not d.startswith(prefix):
                continue
            rest = d[len(prefix) :].lstrip("/")
            if not rest or "/" in rest:
                continue  # root marker or not an immediate child
            if rest in seen:
                continue
            seen.add(rest)
            yield FileInfo(
                path=self.normalize_path(path).joinpath(rest).as_posix(),
                name=rest,
                is_dir=True,
                size=0,
                modified=datetime.fromtimestamp(self._now, tz=UTC),
                created=datetime.fromtimestamp(self._now, tz=UTC),
            )

        # Yield files.
        for fpath, content in self._files.items():
            if not fpath.startswith(prefix):
                continue
            rest = fpath[len(prefix) :].lstrip("/")
            if not rest or "/" in rest:
                continue  # not an immediate child
            if rest in seen:
                continue
            seen.add(rest)
            yield FileInfo(
                path=self.normalize_path(path).joinpath(rest).as_posix(),
                name=rest,
                is_dir=False,
                size=len(content),
                modified=datetime.fromtimestamp(self._now, tz=UTC),
                created=datetime.fromtimestamp(self._now, tz=UTC),
            )

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[tuple[str, list[FileInfo], list[FileInfo]]]:
        target = self._resolve(path)

        if target not in self._dirs and target != "" and not self._is_dir(target):
            raise NotADirectoryError(f"Not a directory: {path}")

        prefix = target.removesuffix("/") + "/" if target else ""

        # Collect immediate children, partitioned into dirs and files.
        dir_names: set[str] = set()
        dirs: list[FileInfo] = []
        files: list[FileInfo] = []
        seen: set[str] = set()

        for d in self._dirs:
            if d == target or not d.startswith(prefix):
                continue
            rest = d[len(prefix) :].lstrip("/")
            if not rest or "/" in rest:
                continue
            if rest in seen:
                continue
            seen.add(rest)
            dir_names.add(rest)
            dirs.append(
                FileInfo(
                    path=self.normalize_path(path).joinpath(rest).as_posix(),
                    name=rest,
                    is_dir=True,
                    size=0,
                    modified=datetime.fromtimestamp(self._now, tz=UTC),
                    created=datetime.fromtimestamp(self._now, tz=UTC),
                )
            )

        for fpath, content in self._files.items():
            if not fpath.startswith(prefix):
                continue
            rest = fpath[len(prefix) :].lstrip("/")
            if not rest or "/" in rest:
                continue
            if rest in seen:
                continue
            seen.add(rest)
            files.append(
                FileInfo(
                    path=self.normalize_path(path).joinpath(rest).as_posix(),
                    name=rest,
                    is_dir=False,
                    size=len(content),
                    modified=datetime.fromtimestamp(self._now, tz=UTC),
                    created=datetime.fromtimestamp(self._now, tz=UTC),
                )
            )

        yield self.normalize_path(path).as_posix(), dirs, files

        for d in sorted(dir_names):
            sub_path = str(PurePosixPath(path) / d) if path else d
            async for sp, sd, sf in self.walk(sub_path):
                yield sp, sd, sf
