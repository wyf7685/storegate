from __future__ import annotations

import errno
from collections.abc import AsyncGenerator, AsyncIterable
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Literal, override

from storegate.storage import AbstractStorage, BytesLike, EntryKind, FileInfo, WalkEntry
from storegate.storage.abstract import PathLike


class SymlinkTrapStorage(AbstractStorage):
    """Small storage that deliberately follows links in mutation methods."""

    def __init__(self, *, intermediate_error: Literal["eloop", "value_error"] = "eloop") -> None:
        super().__init__()
        self.files: dict[str, bytes] = {
            "/target.txt": b"target-secret",
            "/target-dir/child.txt": b"nested-secret",
            "/visible.txt": b"visible-content",
        }
        self.directories: set[str] = {"/", "/target-dir"}
        self.links: dict[str, str] = {
            "/dir-link": "/target-dir",
            "/link.txt": "/target.txt",
        }
        self.dangerous_calls: list[tuple[str, str, str | None]] = []
        self.intermediate_error = intermediate_error
        self._timestamp = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)

    @property
    @override
    def display_id(self) -> str:
        return "dav-symlink-trap"

    @property
    @override
    def namespace_identity(self) -> str:
        return "dav-symlink-trap:sha256:test"

    @override
    async def connect(self) -> None:
        return None

    @override
    async def close(self) -> None:
        return None

    @override
    async def ping(self) -> bool:
        return True

    def _normalize(self, path: PathLike) -> str:
        return self.normalize_path(path).as_posix()

    def _intermediate_target(self, path: str) -> str | None:
        for link, target in self.links.items():
            if path.startswith(f"{link}/"):
                return f"{target}{path[len(link) :]}"
        return None

    def _target(self, path: str) -> str:
        return self.links.get(path, self._intermediate_target(path) or path)

    def _is_link_path(self, path: str) -> bool:
        return path in self.links or self._intermediate_target(path) is not None

    def _info(self, path: str, kind: EntryKind, *, size: int = 0) -> FileInfo:
        return FileInfo(
            path=path,
            name=PurePosixPath(path).name,
            kind=kind,
            size=size,
            modified=self._timestamp,
            created=self._timestamp,
        )

    @override
    async def lstat(self, path: PathLike) -> FileInfo:
        normalized = self._normalize(path)
        if self._intermediate_target(normalized) is not None:
            if self.intermediate_error == "value_error":
                raise ValueError(f"Path contains an intermediate symlink or reparse point: {normalized}")
            raise OSError(errno.ELOOP, f"Path contains a symlink: {normalized}")
        if normalized in self.links:
            return self._info(normalized, EntryKind.SYMLINK, size=len(self.links[normalized]))
        if normalized in self.files:
            return self._info(normalized, EntryKind.FILE, size=len(self.files[normalized]))
        if normalized in self.directories:
            return self._info(normalized, EntryKind.DIRECTORY)
        raise FileNotFoundError(f"Path not found: {normalized}")

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        normalized = self._normalize(path)
        target = self._target(normalized)
        if target in self.files:
            return self._info(normalized, EntryKind.FILE, size=len(self.files[target]))
        if target in self.directories:
            return self._info(normalized, EntryKind.DIRECTORY)
        raise FileNotFoundError(f"Path not found: {normalized}")

    @override
    async def exists(self, path: PathLike) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    @override
    async def is_file(self, path: PathLike) -> bool:
        try:
            return (await self.stat(path)).kind is EntryKind.FILE
        except FileNotFoundError:
            return False

    @override
    async def is_dir(self, path: PathLike) -> bool:
        try:
            return (await self.stat(path)).kind is EntryKind.DIRECTORY
        except FileNotFoundError:
            return False

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        normalized = self._normalize(remote_path)
        target = self._target(normalized)
        if self._is_link_path(normalized):
            self.dangerous_calls.append(("upload", normalized, target))
        if target in self.directories:
            raise IsADirectoryError(f"Is a directory: {normalized}")
        if target in self.files and not overwrite:
            raise FileExistsError(f"File exists: {normalized}")
        data = bytearray()
        async for chunk in stream:
            data.extend(chunk)
        self.files[target] = bytes(data)

    @override
    async def download_stream(self, remote_path: PathLike, *, offset: int = 0) -> AsyncGenerator[bytes]:
        normalized = self._normalize(remote_path)
        target = self._target(normalized)
        if self._is_link_path(normalized):
            self.dangerous_calls.append(("download", normalized, target))
        try:
            data = self.files[target]
        except KeyError:
            raise FileNotFoundError(f"File not found: {normalized}") from None
        if offset < len(data):
            yield data[offset:]

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        normalized = self._normalize(path)
        target = self._target(normalized)
        if self._is_link_path(normalized):
            self.dangerous_calls.append(("unlink", normalized, target))
        if target in self.files:
            del self.files[target]
            return
        if missing_ok:
            return
        raise FileNotFoundError(f"File not found: {normalized}")

    @override
    async def rmdir(self, path: PathLike) -> None:
        normalized = self._normalize(path)
        if normalized not in self.directories:
            raise FileNotFoundError(f"Directory not found: {normalized}")
        if any(PurePosixPath(entry).parent.as_posix() == normalized for entry in self.files | self.links):
            raise OSError(f"Directory not empty: {normalized}")
        self.directories.remove(normalized)

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._normalize(src)
        destination = self._normalize(dst)
        source_target = self._target(source)
        destination_target = self._target(destination)
        if self._is_link_path(source) or self._is_link_path(destination):
            self.dangerous_calls.append(("move", source, destination))
        if source_target not in self.files:
            raise FileNotFoundError(f"File not found: {source}")
        if destination_target in self.files and not overwrite:
            raise FileExistsError(f"File exists: {destination}")
        self.files[destination_target] = self.files.pop(source_target)

    @override
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._normalize(src)
        destination = self._normalize(dst)
        source_target = self._target(source)
        destination_target = self._target(destination)
        if self._is_link_path(source) or self._is_link_path(destination):
            self.dangerous_calls.append(("copy", source, destination))
        if source_target not in self.files:
            raise FileNotFoundError(f"File not found: {source}")
        if destination_target in self.files and not overwrite:
            raise FileExistsError(f"File exists: {destination}")
        self.files[destination_target] = self.files[source_target]

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        normalized = self._normalize(path)
        if self._is_link_path(normalized):
            self.dangerous_calls.append(("mkdir", normalized, self._target(normalized)))
        if normalized in self.directories:
            if exist_ok:
                return
            raise FileExistsError(f"Directory exists: {normalized}")
        parent = PurePosixPath(normalized).parent.as_posix()
        if parent not in self.directories and not parents:
            raise FileNotFoundError(f"Parent not found: {parent}")
        if parents:
            current = PurePosixPath("/")
            for part in PurePosixPath(normalized).parts[1:]:
                current /= part
                self.directories.add(current.as_posix())
        else:
            self.directories.add(normalized)

    @override
    async def rmtree(self, path: PathLike) -> None:
        normalized = self._normalize(path)
        if self._is_link_path(normalized):
            self.dangerous_calls.append(("rmtree", normalized, self._target(normalized)))
        prefix = normalized.rstrip("/") + "/"
        self.files = {
            path: data for path, data in self.files.items() if path != normalized and not path.startswith(prefix)
        }
        self.directories = {path for path in self.directories if path != normalized and not path.startswith(prefix)}

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._normalize(src)
        destination = self._normalize(dst)
        if self._is_link_path(source) or self._is_link_path(destination):
            self.dangerous_calls.append(("copytree", source, destination))
        if source not in self.directories:
            raise NotADirectoryError(f"Not a directory: {source}")
        if destination in self.directories and not overwrite:
            raise FileExistsError(f"Directory exists: {destination}")
        self.directories.add(destination)

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        await self.copytree(src, dst, overwrite=overwrite)
        await self.rmtree(src)

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        normalized = self._normalize(path)
        if normalized not in self.directories:
            raise NotADirectoryError(f"Not a directory: {normalized}")
        entries = sorted(self.files.keys() | self.directories | self.links.keys())
        for entry in entries:
            if entry == normalized or PurePosixPath(entry).parent.as_posix() != normalized:
                continue
            yield await self.lstat(entry)

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        normalized = self._normalize(path)
        entries = tuple([entry async for entry in self.iterdir(normalized)])
        yield WalkEntry(path=normalized, entries=entries)
