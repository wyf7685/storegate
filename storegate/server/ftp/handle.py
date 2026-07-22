from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from pathlib import PurePosixPath
from typing import override

import anyio

from storegate.storage import AbstractStorage, EntryKind


async def _guard_not_symlink(
    storage: AbstractStorage,
    path: PurePosixPath,
    *,
    missing_ok: bool = False,
) -> None:
    try:
        info = await storage.lstat(path)
    except FileNotFoundError:
        if missing_ok:
            return
        raise
    if info.kind is EntryKind.SYMLINK:
        raise FileNotFoundError(f"File unavailable: {path}")


class FileHandle(ABC):
    def __init__(self, storage: AbstractStorage, path: PurePosixPath):
        self.storage = storage
        self.path = path

    @abstractmethod
    async def seek(self, offset: int) -> int:
        raise NotImplementedError

    @abstractmethod
    async def read(self, size: int) -> bytes:
        raise NotImplementedError

    @abstractmethod
    async def write(self, data: memoryview[int]) -> int:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class ReadHandle(FileHandle):
    @override
    def __init__(self, storage: AbstractStorage, path: PurePosixPath):
        super().__init__(storage, path)
        self.agen: AsyncGenerator[bytes] | None = None
        self.offset = 0
        self.buffer = bytearray()
        self.closed = False

    @override
    async def seek(self, offset: int) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.agen is not None:
            raise RuntimeError("Cannot seek after reading has started")
        if offset < 0:
            raise ValueError("Negative seek offset")
        self.offset = offset
        return offset

    @override
    async def read(self, size: int) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.agen is None:
            await _guard_not_symlink(self.storage, self.path)
            self.agen = self.storage.download_stream(self.path, offset=self.offset)

        while len(self.buffer) < size:
            try:
                chunk = await anext(self.agen)
            except StopAsyncIteration:
                break
            else:
                self.buffer.extend(chunk)

        if not self.buffer:
            return b""

        result = self.buffer[:size]
        del self.buffer[:size]
        return bytes(result)

    @override
    async def write(self, data: memoryview[int]) -> int:
        raise NotImplementedError("Write operation is not supported for ReadHandle")

    @override
    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.agen is not None:
            await self.agen.aclose()


class WriteHandle(FileHandle):
    @override
    def __init__(self, storage: AbstractStorage, path: PurePosixPath):
        super().__init__(storage, path)
        self.closed = False
        self.task_group = None
        self.send, self.recv = anyio.create_memory_object_stream[memoryview[int]](1)

    @override
    async def seek(self, offset: int) -> int:
        raise OSError("Seek operation is not supported for WriteHandle")

    @override
    async def read(self, size: int) -> bytes:
        raise OSError("Read operation is not supported for WriteHandle")

    async def _writer(self) -> None:
        await _guard_not_symlink(self.storage, self.path, missing_ok=True)
        async with self.recv as stream:
            await self.storage.upload_stream(stream, self.path)

    @override
    async def write(self, data: memoryview[int]) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.task_group is None:
            self.task_group = anyio.create_task_group()
            await self.task_group.__aenter__()
            self.task_group.start_soon(self._writer)
        await self.send.send(data)
        return len(data)

    @override
    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        await self.send.aclose()
        if self.task_group is not None:
            await self.task_group.__aexit__(None, None, None)
        else:
            # Create an empty file if no data was written.
            await _guard_not_symlink(self.storage, self.path, missing_ok=True)
            await self.storage.upload_bytes(b"", self.path)
