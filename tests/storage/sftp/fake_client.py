from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, cast

import asyncssh

from app.storage.sftp.pool import SFTPChannelPool


@dataclass(slots=True)
class FakeNode:
    kind: str
    data: bytearray = field(default_factory=bytearray)
    target: str = ""


@dataclass(slots=True)
class FailureRule:
    operation: str
    matches: Callable[[tuple[object, ...]], bool]
    error: BaseException
    after: bool = False


class FakeSFTPHandle:
    def __init__(self, node: FakeNode) -> None:
        self._node = node
        self.closed = False

    async def write(self, data: bytes) -> None:
        self._node.data.extend(data)

    async def read(self, size: int, *, offset: int = 0) -> bytes:
        return bytes(self._node.data[offset : offset + size])

    async def close(self) -> None:
        self.closed = True


class FakeSFTPClient:
    def __init__(self) -> None:
        self.nodes: dict[PurePosixPath, FakeNode] = {
            PurePosixPath("/storage"): FakeNode("directory"),
            PurePosixPath("/outside"): FakeNode("directory"),
        }
        self.failures: list[FailureRule] = []
        self.readlink_calls: list[str] = []
        self.symlink_calls: list[tuple[str, str]] = []

    @staticmethod
    def _path(path: str | PurePosixPath) -> PurePosixPath:
        value = PurePosixPath(path)
        if not value.is_absolute():
            raise AssertionError(f"fake SFTP path must be absolute: {value.as_posix()}")
        parts: list[str] = []
        for part in value.parts[1:]:
            if part in {"", "."}:
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        return PurePosixPath("/", *parts)

    @staticmethod
    def _attrs(node: FakeNode) -> asyncssh.SFTPAttrs:
        if node.kind == "directory":
            return asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_DIRECTORY, size=0)
        if node.kind == "file":
            return asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_REGULAR, size=len(node.data))
        if node.kind == "symlink":
            return asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SYMLINK, size=len(node.target.encode()))
        return asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SPECIAL)

    def add_dir(self, path: str) -> None:
        value = self._path(path)
        self.nodes[value] = FakeNode("directory")

    def add_file(self, path: str, data: bytes) -> None:
        value = self._path(path)
        self.nodes[value] = FakeNode("file", bytearray(data))

    def add_link(self, path: str, target: str) -> None:
        value = self._path(path)
        self.nodes[value] = FakeNode("symlink", target=target)

    def fail(
        self,
        operation: str,
        matches: Callable[[tuple[object, ...]], bool],
        error: BaseException,
        *,
        after: bool = False,
    ) -> None:
        self.failures.append(FailureRule(operation, matches, error, after))

    def _check_failure(self, operation: str, args: tuple[object, ...], *, after: bool) -> None:
        for index, rule in enumerate(self.failures):
            if rule.operation == operation and rule.after is after and rule.matches(args):
                self.failures.pop(index)
                raise rule.error

    def temporary_paths(self) -> list[str]:
        return sorted(path.as_posix() for path in self.nodes if ".storegate-" in path.name)

    def _require_parent(self, path: PurePosixPath) -> None:
        parent = self.nodes.get(path.parent)
        if parent is None:
            raise asyncssh.SFTPNoSuchFile("missing parent")
        if parent.kind != "directory":
            raise asyncssh.SFTPNotADirectory("parent is not a directory")

    async def lstat(self, path: str) -> asyncssh.SFTPAttrs:
        value = self._path(path)
        node = self.nodes.get(value)
        if node is None:
            raise asyncssh.SFTPNoSuchFile("missing")
        return self._attrs(node)

    async def readlink(self, path: str) -> str:
        value = self._path(path)
        node = self.nodes.get(value)
        if node is None:
            raise asyncssh.SFTPNoSuchFile("missing")
        if node.kind != "symlink":
            raise asyncssh.SFTPFailure("not a symbolic link")
        self.readlink_calls.append(value.as_posix())
        return node.target

    async def symlink(self, target: str, path: str) -> None:
        args: tuple[object, ...] = (target, path)
        self._check_failure("symlink", args, after=False)
        value = self._path(path)
        self._require_parent(value)
        if value in self.nodes:
            raise asyncssh.SFTPFileAlreadyExists("already exists")
        self.nodes[value] = FakeNode("symlink", target=target)
        self.symlink_calls.append((target, value.as_posix()))
        self._check_failure("symlink", args, after=True)

    def scandir(self, path: str) -> AsyncIterator[asyncssh.SFTPName]:
        directory = self._path(path)
        node = self.nodes.get(directory)
        if node is None:
            raise asyncssh.SFTPNoSuchFile("missing")
        if node.kind != "directory":
            raise asyncssh.SFTPNotADirectory("not a directory")

        async def iterate() -> AsyncIterator[asyncssh.SFTPName]:
            unknown = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_UNKNOWN)
            yield asyncssh.SFTPName(".", attrs=unknown)
            yield asyncssh.SFTPName("..", attrs=unknown)
            for child in sorted((item for item in self.nodes if item.parent == directory), key=str):
                yield asyncssh.SFTPName(child.name, attrs=self._attrs(self.nodes[child]))

        return iterate()

    async def mkdir(self, path: str) -> None:
        args: tuple[object, ...] = (path,)
        self._check_failure("mkdir", args, after=False)
        value = self._path(path)
        self._require_parent(value)
        if value in self.nodes:
            raise asyncssh.SFTPFileAlreadyExists("already exists")
        self.nodes[value] = FakeNode("directory")
        self._check_failure("mkdir", args, after=True)

    async def remove(self, path: str) -> None:
        args: tuple[object, ...] = (path,)
        self._check_failure("remove", args, after=False)
        value = self._path(path)
        node = self.nodes.get(value)
        if node is None:
            raise asyncssh.SFTPNoSuchFile("missing")
        if node.kind == "directory":
            raise asyncssh.SFTPFileIsADirectory("is a directory")
        del self.nodes[value]
        self._check_failure("remove", args, after=True)

    async def rmdir(self, path: str) -> None:
        args: tuple[object, ...] = (path,)
        self._check_failure("rmdir", args, after=False)
        value = self._path(path)
        node = self.nodes.get(value)
        if node is None:
            raise asyncssh.SFTPNoSuchFile("missing")
        if node.kind != "directory":
            raise asyncssh.SFTPNotADirectory("not a directory")
        if any(child.parent == value for child in self.nodes if child != value):
            raise asyncssh.SFTPDirNotEmpty("directory not empty")
        del self.nodes[value]
        self._check_failure("rmdir", args, after=True)

    def _rename_nodes(self, source: PurePosixPath, destination: PurePosixPath) -> None:
        moved: dict[PurePosixPath, FakeNode] = {}
        for path, node in self.nodes.items():
            if path == source:
                moved[destination] = node
                continue
            try:
                relative = path.relative_to(source)
            except ValueError:
                continue
            moved[destination / relative] = node
        for path in list(self.nodes):
            if path == source:
                del self.nodes[path]
                continue
            try:
                path.relative_to(source)
            except ValueError:
                continue
            del self.nodes[path]
        self.nodes.update(moved)

    async def rename(self, source: str, destination: str) -> None:
        args: tuple[object, ...] = (source, destination)
        self._check_failure("rename", args, after=False)
        src = self._path(source)
        dst = self._path(destination)
        if src not in self.nodes:
            raise asyncssh.SFTPNoSuchFile("missing source")
        self._require_parent(dst)
        if dst in self.nodes:
            raise asyncssh.SFTPFileAlreadyExists("destination exists")
        self._rename_nodes(src, dst)
        self._check_failure("rename", args, after=True)

    async def posix_rename(self, source: str, destination: str) -> None:
        args: tuple[object, ...] = (source, destination)
        self._check_failure("posix_rename", args, after=False)
        src = self._path(source)
        dst = self._path(destination)
        if src not in self.nodes:
            raise asyncssh.SFTPNoSuchFile("missing source")
        self._require_parent(dst)
        existing = self.nodes.get(dst)
        if existing is not None:
            if existing.kind == "directory":
                raise asyncssh.SFTPFileIsADirectory("destination is a directory")
            del self.nodes[dst]
        self._rename_nodes(src, dst)
        self._check_failure("posix_rename", args, after=True)

    async def open(self, path: str, mode: str, *, encoding: None) -> asyncssh.SFTPClientFile[bytes]:
        del encoding
        args: tuple[object, ...] = (path, mode)
        self._check_failure("open", args, after=False)
        value = self._path(path)
        if mode == "wb":
            self._require_parent(value)
            existing = self.nodes.get(value)
            if existing is not None and existing.kind == "directory":
                raise asyncssh.SFTPFileIsADirectory("is a directory")
            node = FakeNode("file")
            self.nodes[value] = node
        elif mode == "rb":
            node = self.nodes.get(value)
            if node is None:
                raise asyncssh.SFTPNoSuchFile("missing")
            if node.kind != "file":
                raise asyncssh.SFTPFailure("not a regular file")
        else:
            raise AssertionError(f"unexpected fake SFTP open mode: {mode}")
        self._check_failure("open", args, after=True)
        return cast("asyncssh.SFTPClientFile[bytes]", FakeSFTPHandle(node))


class FakeLease:
    def __init__(self, client: FakeSFTPClient) -> None:
        self.client = cast("asyncssh.SFTPClient", client)
        self.invalid = False
        self.transport_invalid = False

    def invalidate(self) -> None:
        self.invalid = True

    def invalidate_transport(self) -> None:
        self.invalid = True
        self.transport_invalid = True


class FakePool:
    def __init__(self, client: FakeSFTPClient) -> None:
        self.lease = FakeLease(client)
        self.active_leases = 0

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[FakeLease]:
        self.active_leases += 1
        try:
            yield self.lease
        finally:
            self.active_leases -= 1


def install_fake_pool(storage: object, client: FakeSFTPClient) -> FakePool:
    pool = FakePool(client)
    cast("Any", storage)._pool = cast("SFTPChannelPool", pool)
    return pool
