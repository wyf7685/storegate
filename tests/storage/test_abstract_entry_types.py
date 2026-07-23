import errno
import inspect
from collections.abc import AsyncGenerator, AsyncIterable, Callable
from dataclasses import FrozenInstanceError
from typing import cast, get_type_hints

import pytest

from storegate.storage import (
    AbstractStorage,
    EntryKind,
    FileInfo,
    StorageCapabilities,
    UnsupportedOperationError,
    WalkEntry,
)
from storegate.storage.abstract import BytesLike, PathLike


class ProbeStorage(AbstractStorage):
    def __init__(self, info: FileInfo | None) -> None:
        super().__init__()
        self.info = info
        self.is_dir_result = False
        self.stat_calls: list[PathLike] = []
        self.unlink_calls: list[tuple[PathLike, bool]] = []
        self.rmdir_calls: list[PathLike] = []

    @property
    def display_id(self) -> str:
        return "probe"

    @property
    def namespace_identity(self) -> str:
        return "probe:sha256:test"

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def ping(self) -> bool:
        return True

    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        del stream, remote_path, overwrite
        raise AssertionError("not used")

    async def download_stream(self, remote_path: PathLike, *, offset: int = 0) -> AsyncGenerator[bytes]:
        del remote_path, offset
        raise AssertionError("not used")
        yield b""

    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        self.unlink_calls.append((path, missing_ok))

    async def rmdir(self, path: PathLike) -> None:
        self.rmdir_calls.append(path)

    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        del src, dst, overwrite
        raise AssertionError("not used")

    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        del src, dst, overwrite
        raise AssertionError("not used")

    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        del path, parents, exist_ok
        raise AssertionError("not used")

    async def rmtree(self, path: PathLike) -> None:
        del path
        raise AssertionError("not used")

    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        del src, dst, overwrite
        raise AssertionError("not used")

    async def exists(self, path: PathLike) -> bool:
        del path
        return self.info is not None

    async def is_file(self, path: PathLike) -> bool:
        del path
        return self.info is not None and self.info.is_file

    async def is_dir(self, path: PathLike) -> bool:
        del path
        return self.is_dir_result

    async def stat(self, path: PathLike) -> FileInfo:
        self.stat_calls.append(path)
        if self.info is None:
            raise FileNotFoundError(path)
        return self.info

    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        del path
        raise AssertionError("not used")
        yield FileInfo(path="/unused", name="unused", kind=EntryKind.FILE)

    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        del path
        raise AssertionError("not used")
        yield WalkEntry(path="/unused", entries=())


@pytest.mark.parametrize(
    ("kind", "flags"),
    [
        (EntryKind.FILE, (True, False, False)),
        (EntryKind.DIRECTORY, (False, True, False)),
        (EntryKind.SYMLINK, (False, False, True)),
    ],
)
def test_file_info_kind_properties_are_mutually_exclusive(
    kind: EntryKind,
    flags: tuple[bool, bool, bool],
) -> None:
    info = FileInfo(path="/entry", name="entry", kind=kind)
    assert (info.is_file, info.is_dir, info.is_symlink) == flags


def test_file_info_has_no_legacy_is_dir_constructor() -> None:
    assert "is_dir" not in inspect.signature(FileInfo).parameters
    constructor = cast("Callable[..., FileInfo]", FileInfo)
    with pytest.raises(TypeError):
        constructor(path="/entry", name="entry", is_dir=True)


def test_walk_entry_uses_an_immutable_tuple() -> None:
    info = FileInfo(path="/entry", name="entry", kind=EntryKind.FILE)
    walk_entry = WalkEntry(path="/", entries=(info,))
    assert walk_entry.entries == (info,)
    assert get_type_hints(WalkEntry)["entries"] == tuple[FileInfo, ...]
    with pytest.raises(FrozenInstanceError):
        type(walk_entry).__setattr__(walk_entry, "entries", ())


def test_default_capabilities_are_frozen_and_reused() -> None:
    first = ProbeStorage(None).capabilities
    second = ProbeStorage(None).capabilities
    assert first is second
    assert first == StorageCapabilities()
    with pytest.raises(FrozenInstanceError):
        type(first).__setattr__(first, "readlink", True)


async def test_default_lstat_and_is_symlink_use_lexical_metadata() -> None:
    info = FileInfo(path="/link", name="link", kind=EntryKind.SYMLINK)
    storage = ProbeStorage(info)
    assert await storage.lstat("/link") is info
    assert await storage.is_symlink("/link") is True
    assert storage.stat_calls == ["/link", "/link"]

    missing = ProbeStorage(None)
    assert await missing.is_symlink("/missing") is False


async def test_default_link_operations_raise_unified_unsupported_error() -> None:
    storage = ProbeStorage(None)
    unsupported_errno = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)

    with pytest.raises(UnsupportedOperationError) as readlink_error:
        await storage.readlink("/entry")
    assert readlink_error.value.errno == unsupported_errno

    with pytest.raises(UnsupportedOperationError) as symlink_error:
        await storage.symlink("target", "/link")
    assert symlink_error.value.errno == unsupported_errno


def test_default_symlink_signature_matches_public_contract() -> None:
    signature = inspect.signature(AbstractStorage.symlink)
    assert list(signature.parameters) == [
        "self",
        "target",
        "link_path",
        "target_is_directory",
        "overwrite",
    ]
    assert signature.parameters["target_is_directory"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["target_is_directory"].default is False
    assert signature.parameters["overwrite"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["overwrite"].default is False

    hints = get_type_hints(AbstractStorage.symlink)
    assert hints["target"] == PathLike
    assert hints["link_path"] == PathLike
    assert hints["target_is_directory"] is bool
    assert hints["overwrite"] is bool
    assert hints["return"] is type(None)


def test_foundation_method_return_types_match_public_contract() -> None:
    assert get_type_hints(AbstractStorage.lstat)["return"] is FileInfo
    assert get_type_hints(AbstractStorage.is_symlink)["return"] is bool
    assert get_type_hints(AbstractStorage.readlink)["return"] is str
    assert get_type_hints(AbstractStorage.walk)["return"] == AsyncGenerator[WalkEntry]


async def test_delete_dispatches_from_lstat_and_unlinks_symlink() -> None:
    info = FileInfo(path="/link", name="link", kind=EntryKind.SYMLINK)
    storage = ProbeStorage(info)
    storage.is_dir_result = True

    await storage.delete("/link")

    assert storage.stat_calls == ["/link"]
    assert storage.unlink_calls == [("/link", False)]
    assert storage.rmdir_calls == []
