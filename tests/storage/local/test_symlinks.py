"""Focused LocalStorage symlink and tree semantics."""

from __future__ import annotations

import errno
import os
import stat
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from storegate.storage import EntryKind, UnsupportedOperationError, WalkEntry
from storegate.storage.local import LocalStorage
from storegate.storage.local.storage import _RawKind


async def _storage(root: Path) -> LocalStorage:
    storage = LocalStorage(root)
    await storage.connect()
    return storage


async def _symlink(
    storage: LocalStorage,
    target: str,
    link_path: str,
    *,
    target_is_directory: bool = False,
    overwrite: bool = False,
) -> None:
    try:
        await storage.symlink(
            target,
            link_path,
            target_is_directory=target_is_directory,
            overwrite=overwrite,
        )
    except PermissionError as exc:
        pytest.skip(f"OS symlink creation unavailable: {exc}")


async def test_capabilities_are_complete_and_reused(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    assert storage.capabilities is storage.capabilities
    assert storage.capabilities.symlink_metadata
    assert storage.capabilities.readlink
    assert storage.capabilities.symlink_create


async def test_create_lstat_stat_readlink_and_identity(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.upload_bytes(b"payload", "/target.txt")
    await _symlink(storage, "target.txt", "/link.txt")

    lexical = await storage.lstat("/link.txt")
    followed = await storage.stat("/link.txt")
    assert lexical.kind is EntryKind.SYMLINK
    assert lexical.size >= len("target.txt")
    assert followed.kind is EntryKind.FILE
    assert followed.size == len(b"payload")
    assert followed.path == "/link.txt"
    assert followed.name == "link.txt"
    assert await storage.readlink("/link.txt") == "target.txt"
    assert await storage.is_symlink("/link.txt")
    assert await storage.is_file("/link.txt")
    assert not await storage.is_dir("/link.txt")


async def test_symlink_overwrite_false_rejects_directory(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.mkdir("/directory")
    with pytest.raises(FileExistsError):
        await storage.symlink("target", "/directory", overwrite=False)
    assert await storage.is_dir("/directory")
    with pytest.raises(IsADirectoryError):
        await storage.symlink("target", "/directory", overwrite=True)
    assert await storage.is_dir("/directory")


async def test_absolute_creation_rejected_without_os_call(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    with pytest.raises(ValueError, match="relative"):
        await storage.symlink("/absolute/target", "/link")
    assert not await storage.is_symlink("/link")


@pytest.mark.parametrize(
    "target",
    [
        "C:/outside.txt",
        "C:outside.txt",
        "C:",
        r"\outside.txt",
        r"\\server\share\outside.txt",
        r"\\?\C:\outside.txt",
        r"\\.\C:\outside.txt",
        "//server/share/outside.txt",
        "//?/C:/outside.txt",
    ],
)
async def test_windows_native_absolute_targets_are_rejected_before_os_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    storage = LocalStorage(tmp_path / "root")
    create_called = False

    def fail_if_called(_target: str, _link_path: Path, *, target_is_directory: bool) -> None:
        nonlocal create_called
        _ = target_is_directory
        create_called = True

    monkeypatch.setattr(LocalStorage, "_create_symlink_sync", staticmethod(fail_if_called))
    with pytest.raises(ValueError, match="relative POSIX"):
        await storage.symlink(target, "/link")
    assert not create_called


async def test_dangling_link_and_directory_hint(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await _symlink(storage, "missing/target", "/dangling", target_is_directory=True)

    assert (await storage.lstat("/dangling")).kind is EntryKind.SYMLINK
    assert await storage.readlink("/dangling") == "missing/target"
    assert await storage.is_symlink("/dangling")
    assert not await storage.exists("/dangling")
    assert not await storage.is_file("/dangling")
    assert not await storage.is_dir("/dangling")
    with pytest.raises(FileNotFoundError):
        await storage.stat("/dangling")


async def test_link_chain_and_download_follow_upload_reject(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.upload_bytes(b"original", "/target.txt")
    await _symlink(storage, "target.txt", "/middle")
    await _symlink(storage, "middle", "/alias")

    assert await storage.download_bytes("/alias") == b"original"
    with pytest.raises(FileExistsError, match="refuses a symlink"):
        await storage.upload_bytes(b"replacement", "/alias", overwrite=True)
    assert await storage.download_bytes("/target.txt") == b"original"
    assert await storage.readlink("/alias") == "middle"


async def test_unlink_and_rmdir_are_lexical(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.mkdir("/directory")
    await storage.upload_bytes(b"keep", "/directory/file.txt")
    await _symlink(storage, "directory", "/directory-link", target_is_directory=True)

    with pytest.raises(NotADirectoryError):
        await storage.rmdir("/directory-link")
    await storage.unlink("/directory-link")
    assert await storage.download_bytes("/directory/file.txt") == b"keep"


async def test_copy_and_move_preserve_raw_link_and_same_path_semantics(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await _symlink(storage, "missing/../raw-target", "/source")

    await storage.copy("/source", "/copy")
    assert await storage.readlink("/copy") == "missing/../raw-target"
    await storage.move("/copy", "/moved")
    assert not await storage.is_symlink("/copy")
    assert await storage.readlink("/moved") == "missing/../raw-target"

    await storage.copy("/source", "/source", overwrite=True)
    await storage.move("/moved", "/moved", overwrite=True)
    with pytest.raises(FileExistsError):
        await storage.copy("/source", "/source", overwrite=False)
    with pytest.raises(FileExistsError):
        await storage.move("/moved", "/moved", overwrite=False)


async def test_file_and_link_overwrite_each_other(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.upload_bytes(b"file", "/destination")
    await _symlink(storage, "missing", "/source-link")

    with pytest.raises(FileExistsError):
        await storage.copy("/source-link", "/destination", overwrite=False)
    await storage.copy("/source-link", "/destination", overwrite=True)
    assert await storage.readlink("/destination") == "missing"

    await storage.upload_bytes(b"source", "/source-file")
    await storage.copy("/source-file", "/destination", overwrite=True)
    assert (await storage.lstat("/destination")).kind is EntryKind.FILE
    assert await storage.download_bytes("/destination") == b"source"


async def test_iterdir_and_walk_snapshot_include_links_without_recursing(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.mkdir("/tree/real", parents=True)
    await storage.upload_bytes(b"file", "/tree/file.txt")
    await storage.upload_bytes(b"nested", "/tree/real/nested.txt")
    await _symlink(storage, "real", "/tree/link-dir", target_is_directory=True)

    entries = [entry async for entry in storage.iterdir("/tree")]
    assert [entry.name for entry in entries] == ["file.txt", "link-dir", "real"]
    assert next(entry for entry in entries if entry.name == "link-dir").kind is EntryKind.SYMLINK

    walked = [entry async for entry in storage.walk("/tree")]
    assert all(isinstance(entry, WalkEntry) for entry in walked)
    assert [entry.path for entry in walked] == ["/tree", "/tree/real"]
    assert any(item.kind is EntryKind.SYMLINK for item in walked[0].entries)
    with pytest.raises(NotADirectoryError):
        _ = [entry async for entry in storage.walk("/tree/link-dir")]


async def test_tree_copy_move_and_delete_preserve_links_as_leaves(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.mkdir("/source/sub", parents=True)
    await storage.upload_bytes(b"payload", "/source/sub/file.txt")
    await _symlink(storage, "../sub/file.txt", "/source/link")
    await _symlink(storage, "missing", "/source/dangling")

    await storage.copytree("/source", "/copy")
    assert await storage.readlink("/copy/link") == "../sub/file.txt"
    assert await storage.readlink("/copy/dangling") == "missing"
    await storage.movetree("/copy", "/moved")
    assert not await storage.exists("/copy")
    assert await storage.readlink("/moved/link") == "../sub/file.txt"
    await storage.rmtree("/source")
    await storage.rmtree("/moved")
    assert not await storage.exists("/source")
    assert not await storage.exists("/moved")


async def test_copytree_failure_rolls_back_created_destination_parents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    storage = await _storage(root)
    await storage.upload_bytes(b"source", "/source/file.txt")

    def fail_copy(_source: Path, _destination: Path) -> None:
        raise OSError("forced copy failure")

    monkeypatch.setattr("storegate.storage.local.storage.shutil.copy2", fail_copy)
    with pytest.raises(OSError, match="forced copy failure"):
        await storage.copytree("/source", "/created/parent/destination")

    assert not (root / "created").exists()
    assert await storage.download_bytes("/source/file.txt") == b"source"
    assert not any(path.name.startswith(".storegate-") for path in root.iterdir())


async def test_rmdir_counts_symlink_as_raw_nonempty(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.mkdir("/directory")
    await _symlink(storage, "missing", "/directory/dangling")

    with pytest.raises(OSError, match="Directory not empty") as caught:
        await storage.rmdir("/directory")
    assert caught.value.errno == errno.ENOTEMPTY
    await storage.rmtree("/directory")


async def test_posix_special_entry_discovery_and_strict_preflight(tmp_path: Path) -> None:
    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO creation is unavailable on this platform")
    root = tmp_path / "root"
    storage = await _storage(root)
    await storage.mkdir("/source")
    await storage.upload_bytes(b"keep", "/source/file.txt")
    os.mkfifo(root / "source" / "fifo")

    entries = [entry async for entry in storage.iterdir("/source")]
    assert [entry.name for entry in entries] == ["file.txt"]
    with pytest.raises(OSError, match="Directory not empty") as nonempty:
        await storage.rmdir("/source")
    assert nonempty.value.errno == errno.ENOTEMPTY

    with pytest.raises(UnsupportedOperationError) as copy_error:
        await storage.copytree("/source", "/copy")
    assert copy_error.value.errno in {getattr(errno, "ENOTSUP", errno.EOPNOTSUPP), errno.EOPNOTSUPP}
    assert not await storage.exists("/copy")
    with pytest.raises(UnsupportedOperationError):
        await storage.rmtree("/source")
    assert await storage.download_bytes("/source/file.txt") == b"keep"


def _install_fake_resolver_filesystem(
    monkeypatch: pytest.MonkeyPatch,
    entries: dict[Path, int],
    links: dict[Path, str],
) -> None:
    normalized_entries = {
        os.path.normcase(os.path.abspath(path)): mode  # noqa: PTH100
        for path, mode in entries.items()
    }
    normalized_links = {
        os.path.normcase(os.path.abspath(path)): target  # noqa: PTH100
        for path, target in links.items()
    }

    def fake_lstat(path: os.PathLike[str] | str) -> object:
        key = os.path.normcase(os.path.abspath(path))  # noqa: PTH100
        try:
            mode = normalized_entries[key]
        except KeyError:
            raise FileNotFoundError(errno.ENOENT, "missing fake entry", path) from None
        return SimpleNamespace(st_mode=mode, st_file_attributes=0)

    def fake_readlink(path: os.PathLike[str] | str) -> str:
        return normalized_links[os.path.normcase(os.path.abspath(path))]  # noqa: PTH100

    def fake_classify(_path: Path, result: object) -> _RawKind:
        mode = cast("SimpleNamespace", result).st_mode
        if stat.S_ISLNK(mode):
            return _RawKind.SYMLINK
        if stat.S_ISDIR(mode):
            return _RawKind.DIRECTORY
        return _RawKind.FILE

    monkeypatch.setattr("storegate.storage.local.storage.os.lstat", fake_lstat)
    monkeypatch.setattr("storegate.storage.local.storage.os.readlink", fake_readlink)
    monkeypatch.setattr(LocalStorage, "_classify_entry", staticmethod(fake_classify))


def test_pure_follow_resolver_follows_final_chain_inside_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = LocalStorage(tmp_path / "root")
    root = storage._root
    alias = root / "alias"
    target = root / "target.txt"
    _install_fake_resolver_filesystem(
        monkeypatch,
        {
            root: stat.S_IFDIR | 0o700,
            alias: stat.S_IFLNK | 0o777,
            target: stat.S_IFREG | 0o600,
        },
        {alias: "target.txt"},
    )

    logical, resolved, _result, kind = storage._follow("/alias")
    assert logical.as_posix() == "/alias"
    assert resolved == target
    assert kind is EntryKind.FILE


def test_pure_follow_resolver_rejects_root_escape_with_eacces(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = LocalStorage(tmp_path / "root")
    root = storage._root
    alias = root / "alias"
    _install_fake_resolver_filesystem(
        monkeypatch,
        {root: stat.S_IFDIR | 0o700, alias: stat.S_IFLNK | 0o777},
        {alias: os.fspath(tmp_path / "outside.txt")},
    )

    with pytest.raises(PermissionError) as caught:
        storage._follow("/alias")
    assert caught.value.errno == errno.EACCES


def test_pure_follow_resolver_maps_cycle_to_eloop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = LocalStorage(tmp_path / "root")
    root = storage._root
    first = root / "first"
    second = root / "second"
    _install_fake_resolver_filesystem(
        monkeypatch,
        {
            root: stat.S_IFDIR | 0o700,
            first: stat.S_IFLNK | 0o777,
            second: stat.S_IFLNK | 0o777,
        },
        {first: "second", second: "first"},
    )

    with pytest.raises(OSError, match="Symlink cycle") as caught:
        storage._follow("/first")
    assert caught.value.errno == errno.ELOOP


class _FakePath:
    def __init__(self, *, symlink: bool = False, junction: bool = False) -> None:
        self._symlink = symlink
        self._junction = junction

    def is_symlink(self) -> bool:
        return self._symlink

    def is_junction(self) -> bool:
        return self._junction


def test_pure_metadata_classification_distinguishes_symlink_junction_and_unknown_reparse() -> None:
    regular_mode = stat.S_IFREG | 0o600
    symlink_result = cast("os.stat_result", SimpleNamespace(st_mode=stat.S_IFLNK | 0o777, st_file_attributes=0))
    junction_result = cast("os.stat_result", SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_file_attributes=0x400))
    unknown_result = cast("os.stat_result", SimpleNamespace(st_mode=regular_mode, st_file_attributes=0x400))

    assert LocalStorage._classify_entry(cast("Path", _FakePath(symlink=True)), symlink_result) is _RawKind.SYMLINK
    assert LocalStorage._classify_entry(cast("Path", _FakePath(junction=True)), junction_result) is _RawKind.JUNCTION
    assert LocalStorage._classify_entry(cast("Path", _FakePath()), unknown_result) is _RawKind.SPECIAL
