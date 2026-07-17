"""LocalStorage link containment and root policy tests."""

import subprocess
import sys
from pathlib import Path

import pytest

from app.storage.local import LocalStorage


async def _storage(root: Path) -> LocalStorage:
    storage = LocalStorage(root)
    await storage.connect()
    return storage


def _directory_symlink(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {exc}")


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
async def test_symlink_components_are_rejected_for_all_operations(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "victim.txt"
    victim.write_bytes(b"keep")
    storage = await _storage(root)
    _directory_symlink(root / "link", outside)

    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.upload_bytes(b"escape", "/link/new.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.download_bytes("/link/victim.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.stat("/link/victim.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.unlink("/link/victim.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.rmtree("/link")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.move("/link/victim.txt", "/moved.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.copy("/link/victim.txt", "/copied.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.copytree("/link", "/copied-tree")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.movetree("/link", "/moved-tree")

    assert victim.read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_symlink_destination_is_rejected_without_writing_outside(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    storage = await _storage(root)
    await storage.upload_bytes(b"source", "/source.txt")
    _directory_symlink(root / "link", outside)

    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.upload_bytes(b"escape", "/link/new.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.copy("/source.txt", "/link/copied.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.move("/source.txt", "/link/moved.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.copytree("/", "/link/tree")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.movetree("/", "/link/tree")
    assert not (outside / "new.txt").exists()
    assert not (outside / "copied.txt").exists()
    assert not (outside / "moved.txt").exists()


@pytest.mark.asyncio
async def test_windows_junction_components_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "victim.txt").write_bytes(b"keep")
    storage = await _storage(root)
    _directory_junction(root / "junction", outside)

    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.upload_bytes(b"escape", "/junction/new.txt")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.rmtree("/junction")
    assert (outside / "victim.txt").read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_tree_operations_reject_nested_symlink_components(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "victim.txt").write_bytes(b"keep")
    storage = await _storage(root)
    await storage.mkdir("/source", parents=True)
    _directory_symlink(root / "source" / "link", outside)

    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.copytree("/source", "/copy")
    with pytest.raises(ValueError, match="symlink or reparse"):
        await storage.rmtree("/source")
    assert (outside / "victim.txt").read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_symlinked_root_is_canonicalized_once(tmp_path: Path) -> None:
    real_root = tmp_path / "real-root"
    real_root.mkdir()
    root_alias = tmp_path / "root-alias"
    _directory_symlink(root_alias, real_root)

    storage = await _storage(root_alias)
    await storage.upload_bytes(b"inside", "/file.txt")
    assert (real_root / "file.txt").read_bytes() == b"inside"
    assert await storage.download_bytes("/file.txt") == b"inside"
