"""LocalStorage lexical and canonical containment tests."""

import errno
import os
import subprocess
import sys
from pathlib import Path

import pytest

from app.storage import EntryKind, UnsupportedOperationError
from app.storage.local import LocalStorage


async def _storage(root: Path) -> LocalStorage:
    storage = LocalStorage(root)
    await storage.connect()
    return storage


def _directory_symlink(link: Path, target: Path | str) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {exc}")


def _file_symlink(link: Path, target: Path | str) -> None:
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"file symlinks unavailable: {exc}")


def _directory_junction(link: Path, target: Path) -> None:
    if sys.platform != "win32":
        pytest.skip("junctions are Windows-only")
    result = subprocess.run(  # noqa: S603
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],  # noqa: S607
        capture_output=True,
        check=False,
    )
    if result.returncode:
        output = (result.stderr or result.stdout).decode(errors="replace").strip()
        pytest.skip(f"directory junctions unavailable: {output}")


@pytest.mark.asyncio
async def test_normal_nested_operations_remain_supported(tmp_path: Path) -> None:
    storage = await _storage(tmp_path / "root")
    await storage.mkdir("/a/b", parents=True)
    await storage.upload_bytes(b"payload", "/a/b/file.txt")
    assert await storage.download_bytes("/a/b/file.txt") == b"payload"

    await storage.copy("/a/b/file.txt", "/a/b/copy.txt")
    await storage.move("/a/b/copy.txt", "/a/moved.txt")
    await storage.copytree("/a", "/tree-copy")
    await storage.movetree("/tree-copy", "/tree-moved")
    assert await storage.exists("/tree-moved/b/file.txt")

    await storage.unlink("/a/moved.txt")
    await storage.rmtree("/tree-moved")
    await storage.unlink("/a/b/file.txt")
    await storage.rmdir("/a/b")
    await storage.rmdir("/a")


@pytest.mark.asyncio
async def test_final_symlink_is_inspectable_and_followed_inside_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    storage = await _storage(root)
    await storage.upload_bytes(b"inside", "/target.txt")
    _file_symlink(root / "alias.txt", "target.txt")

    link_info = await storage.lstat("/alias.txt")
    followed_info = await storage.stat("/alias.txt")
    assert link_info.kind is EntryKind.SYMLINK
    assert followed_info.kind is EntryKind.FILE
    assert followed_info.path == "/alias.txt"
    assert followed_info.name == "alias.txt"
    assert await storage.readlink("/alias.txt") == "target.txt"
    assert await storage.download_bytes("/alias.txt") == b"inside"


@pytest.mark.asyncio
async def test_caller_supplied_intermediate_symlink_is_rejected_for_all_operations(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "victim.txt"
    victim.write_bytes(b"keep")
    storage = await _storage(root)
    _directory_symlink(root / "link", outside)

    operations = (
        storage.upload_bytes(b"escape", "/link/new.txt"),
        storage.download_bytes("/link/victim.txt"),
        storage.stat("/link/victim.txt"),
        storage.lstat("/link/victim.txt"),
        storage.unlink("/link/victim.txt"),
        storage.move("/link/victim.txt", "/moved.txt"),
        storage.copy("/link/victim.txt", "/copied.txt"),
        storage.copytree("/link/child", "/copied-tree"),
        storage.movetree("/link/child", "/moved-tree"),
    )
    for operation in operations:
        with pytest.raises(ValueError, match="intermediate symlink or reparse"):
            await operation
    assert victim.read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_symlink_destination_component_is_rejected_without_writing_outside(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    storage = await _storage(root)
    await storage.upload_bytes(b"source", "/source.txt")
    _directory_symlink(root / "link", outside)

    with pytest.raises(ValueError, match="intermediate symlink or reparse"):
        await storage.upload_bytes(b"escape", "/link/new.txt")
    with pytest.raises(ValueError, match="intermediate symlink or reparse"):
        await storage.copy("/source.txt", "/link/copied.txt")
    with pytest.raises(ValueError, match="intermediate symlink or reparse"):
        await storage.move("/source.txt", "/link/moved.txt")
    with pytest.raises(ValueError, match="intermediate symlink or reparse"):
        await storage.copytree("/", "/link/tree")
    assert not (outside / "new.txt").exists()
    assert not (outside / "copied.txt").exists()
    assert not (outside / "moved.txt").exists()


@pytest.mark.asyncio
async def test_final_symlink_root_escape_uses_eacces_and_unlink_is_lexical(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "victim.txt"
    victim.write_bytes(b"keep")
    storage = await _storage(root)
    _file_symlink(root / "escape", victim)

    assert (await storage.lstat("/escape")).kind is EntryKind.SYMLINK
    assert await storage.readlink("/escape") == os.fspath(victim)
    for operation in (storage.stat("/escape"), storage.download_bytes("/escape"), storage.exists("/escape")):
        with pytest.raises(PermissionError) as caught:
            await operation
        assert caught.value.errno == errno.EACCES

    await storage.unlink("/escape")
    assert victim.read_bytes() == b"keep"
    assert not (root / "escape").exists()


@pytest.mark.asyncio
async def test_final_symlink_cycle_uses_eloop(tmp_path: Path) -> None:
    root = tmp_path / "root"
    storage = await _storage(root)
    _file_symlink(root / "a", "b")
    _file_symlink(root / "b", "a")

    assert (await storage.lstat("/a")).kind is EntryKind.SYMLINK
    with pytest.raises(OSError, match="Symlink cycle") as caught:
        await storage.stat("/a")
    assert caught.value.errno == errno.ELOOP


@pytest.mark.asyncio
async def test_tree_operations_preserve_nested_symlink_without_following_target(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "victim.txt"
    victim.write_bytes(b"keep")
    storage = await _storage(root)
    await storage.mkdir("/source")
    _file_symlink(root / "source" / "link", victim)

    await storage.copytree("/source", "/copy")
    assert (await storage.lstat("/copy/link")).kind is EntryKind.SYMLINK
    assert await storage.readlink("/copy/link") == os.fspath(victim)
    await storage.rmtree("/source")
    assert victim.read_bytes() == b"keep"
    await storage.rmtree("/copy")
    assert victim.read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_windows_junction_is_never_exposed_as_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "victim.txt").write_bytes(b"keep")
    storage = await _storage(root)
    _directory_junction(root / "junction", outside)

    with pytest.raises(UnsupportedOperationError):
        await storage.lstat("/junction")
    entries = [entry async for entry in storage.iterdir("/")]
    assert "junction" not in {entry.name for entry in entries}
    with pytest.raises(ValueError, match="intermediate symlink or reparse"):
        await storage.upload_bytes(b"escape", "/junction/new.txt")
    assert (outside / "victim.txt").read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_symlinked_configured_root_is_canonicalized_once(tmp_path: Path) -> None:
    real_root = tmp_path / "real-root"
    real_root.mkdir()
    root_alias = tmp_path / "root-alias"
    _directory_symlink(root_alias, real_root)

    storage = await _storage(root_alias)
    await storage.upload_bytes(b"inside", "/file.txt")
    assert (real_root / "file.txt").read_bytes() == b"inside"
    assert await storage.download_bytes("/file.txt") == b"inside"
