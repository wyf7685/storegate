from collections.abc import AsyncGenerator
from pathlib import PurePosixPath
from typing import cast
from unittest.mock import AsyncMock

import pytest

from storegate.storage import AbstractStorage, EntryKind, FileInfo, WalkEntry
from storegate.storage.cached import CachedStorage
from storegate.storage.memory import MemoryStorage
from tests.storage.cached.helpers import _clear_path

_FOLLOW_NAMESPACES = ("stat", "exists", "is_file", "is_dir", "download")


def _assert_no_follow_cache(cached: CachedStorage, path: str) -> None:
    snapshot = cached.dump_cache()
    for namespace in _FOLLOW_NAMESPACES:
        assert path not in snapshot.get(namespace, {}), f"{namespace} cached a lexical symlink"


async def test_capabilities_are_proxied_without_allocation(cached: CachedStorage) -> None:
    assert cached.capabilities is cached._storage.capabilities


async def test_dangling_symlink_caches_only_lexical_state(cached: CachedStorage) -> None:
    await cached.symlink("missing.txt", "dangling")

    assert not await cached.exists("dangling")
    assert not await cached.is_file("dangling")
    assert not await cached.is_dir("dangling")
    assert await cached.is_symlink("dangling")
    assert (await cached.lstat("dangling")).kind is EntryKind.SYMLINK
    with pytest.raises(FileNotFoundError):
        await cached.stat("dangling")

    snapshot = cached.dump_cache()
    assert snapshot["is_symlink"]["dangling"] is True
    assert cast("FileInfo", snapshot["lstat"]["dangling"]).kind is EntryKind.SYMLINK
    _assert_no_follow_cache(cached, "dangling")


async def test_symlink_follow_queries_and_download_do_not_stale_across_alias_mutation(cached: CachedStorage) -> None:
    await cached.upload_bytes(b"one", "target.txt")
    await cached.symlink("target.txt", "alias.txt")

    followed = await cached.stat("alias.txt")
    assert (followed.path, followed.name, followed.kind, followed.size) == (
        "/alias.txt",
        "alias.txt",
        EntryKind.FILE,
        3,
    )
    assert await cached.exists("alias.txt")
    assert await cached.is_file("alias.txt")
    assert not await cached.is_dir("alias.txt")
    assert await cached.download_bytes("alias.txt") == b"one"
    _assert_no_follow_cache(cached, "alias.txt")

    await cached.upload_bytes(b"updated", "target.txt", overwrite=True)

    assert (await cached.stat("alias.txt")).size == 7
    assert await cached.download_bytes("alias.txt") == b"updated"
    _assert_no_follow_cache(cached, "alias.txt")


async def test_discovery_symlink_cross_fills_only_lexical_namespaces(cached: CachedStorage) -> None:
    await cached.mkdir("root")
    await cached.upload_bytes(b"data", "root/file.txt")
    await cached.symlink("file.txt", "root/link.txt")
    await _clear_path(cached, "root/file.txt")
    await _clear_path(cached, "root/link.txt")

    entries = await cached.list_("root")
    assert [entry.kind for entry in entries] == [EntryKind.FILE, EntryKind.SYMLINK]

    snapshot = cached.dump_cache()
    assert "root/file.txt" in snapshot["stat"]
    assert "root/file.txt" in snapshot["lstat"]
    assert snapshot["is_symlink"]["root/file.txt"] is False
    assert "root/link.txt" in snapshot["lstat"]
    assert snapshot["is_symlink"]["root/link.txt"] is True
    _assert_no_follow_cache(cached, "root/link.txt")


async def test_walk_delegates_and_preserves_snapshot_identity_and_order(monkeypatch: pytest.MonkeyPatch) -> None:
    underlying = MemoryStorage("/")
    cached = CachedStorage(underlying)
    snapshot = WalkEntry(
        path="/root",
        entries=(
            FileInfo(path="/root/z-link", name="z-link", kind=EntryKind.SYMLINK, size=8),
            FileInfo(path="/root/a-file", name="a-file", kind=EntryKind.FILE, size=1),
        ),
    )
    calls: list[object] = []

    async def walk(path: object) -> AsyncGenerator[WalkEntry]:
        calls.append(path)
        yield snapshot

    monkeypatch.setattr(underlying, "walk", walk)

    results = [entry async for entry in cached.walk("root")]

    # CachedStorage normalizes caller paths before delegating (public path contract).
    assert calls == [PurePosixPath("/root")]
    assert results == [snapshot]
    assert results[0] is snapshot
    assert results[0].entries is snapshot.entries
    assert [entry.name for entry in results[0].entries] == ["z-link", "a-file"]
    cache_snapshot = cached.dump_cache()
    _assert_no_follow_cache(cached, "root/z-link")
    assert cast("FileInfo", cache_snapshot["lstat"]["root/z-link"]).kind is EntryKind.SYMLINK
    assert cast("FileInfo", cache_snapshot["stat"]["root/a-file"]).kind is EntryKind.FILE


async def test_readlink_is_always_uncached(monkeypatch: pytest.MonkeyPatch) -> None:
    underlying = MemoryStorage("/")
    async with CachedStorage(underlying) as cached:
        await cached.symlink("target.txt", "link.txt")
        original = underlying.readlink
        spy = AsyncMock(side_effect=original)
        monkeypatch.setattr(underlying, "readlink", spy)

        assert await cached.readlink("link.txt") == "target.txt"
        assert await cached.readlink("link.txt") == "target.txt"
        assert spy.await_count == 2


async def test_unlink_and_recreate_invalidates_lexical_kind(cached: CachedStorage) -> None:
    await cached.symlink("missing.txt", "entry")
    assert await cached.is_symlink("entry")
    assert (await cached.lstat("entry")).kind is EntryKind.SYMLINK

    await cached.unlink("entry")
    assert cached.dump_cache()["is_symlink"]["entry"] is False

    await cached.upload_bytes(b"regular", "entry")
    assert not await cached.is_symlink("entry")
    assert (await cached.lstat("entry")).kind is EntryKind.FILE

    await cached.unlink("entry")
    await cached.symlink("other.txt", "entry")
    assert await cached.is_symlink("entry")
    assert await cached.readlink("entry") == "other.txt"
    _assert_no_follow_cache(cached, "entry")


async def test_copy_and_move_preserve_link_without_file_backfill(cached: CachedStorage) -> None:
    await cached.symlink("missing.txt", "source-link")

    await cached.copy("source-link", "copied-link")
    assert await cached.readlink("copied-link") == "missing.txt"
    assert cached.dump_cache()["is_symlink"]["copied-link"] is True
    _assert_no_follow_cache(cached, "copied-link")

    await cached.move("copied-link", "moved-link")
    assert not await cached.is_symlink("copied-link")
    assert await cached.readlink("moved-link") == "missing.txt"
    assert cached.dump_cache()["is_symlink"]["moved-link"] is True
    _assert_no_follow_cache(cached, "moved-link")


async def test_symlink_failure_invalidates_overwrite_rollback_state(
    cached: CachedStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    await cached.symlink("old-target", "link")
    await cached.lstat("link")
    await cached._cache.set("stat", "link", FileInfo(path="/link", name="link", kind=EntryKind.FILE))
    await cached._cache.set("download", "link", b"stale")
    original = cached._storage.symlink

    async def failed_overwrite(
        target: str,
        link_path: str,
        *,
        target_is_directory: bool = False,
        overwrite: bool = False,
    ) -> None:
        del target, target_is_directory, overwrite
        await cached._storage.unlink(link_path)
        await original("temporary", link_path)
        await cached._storage.unlink(link_path)
        await original("old-target", link_path)
        raise OSError("injected overwrite failure after rollback")

    monkeypatch.setattr(cached._storage, "symlink", failed_overwrite)

    with pytest.raises(OSError, match="injected overwrite failure"):
        await cached.symlink("new-target", "link", overwrite=True)

    cache_snapshot = cached.dump_cache()
    for namespace in ("stat", "lstat", "exists", "is_file", "is_dir", "is_symlink", "download"):
        assert "link" not in cache_snapshot.get(namespace, {})
    assert await cached.readlink("link") == "old-target"
    assert (await cached.lstat("link")).kind is EntryKind.SYMLINK


async def test_symlink_invalidates_parent_listing_and_prior_path_namespaces(cached: CachedStorage) -> None:
    await cached.mkdir("root")
    await cached.upload_bytes(b"old", "root/link")
    assert [entry.name async for entry in cached.iterdir("root")] == ["link"]
    await cached.stat("root/link")
    await cached.download_bytes("root/link")

    await cached.symlink("missing", "root/link", overwrite=True)

    snapshot = cached.dump_cache()
    assert "root" not in snapshot.get("iterdir", {})
    assert snapshot["is_symlink"]["root/link"] is True
    _assert_no_follow_cache(cached, "root/link")


async def _listing_paths(storage: AbstractStorage, path: str) -> list[str]:
    return [entry.path async for entry in storage.iterdir(path)]


@pytest.mark.parametrize("operation", ["copy", "move"])
@pytest.mark.parametrize("source_kind", [EntryKind.FILE, EntryKind.SYMLINK])
async def test_nested_destination_invalidates_all_ancestor_listings(
    cached: CachedStorage,
    operation: str,
    source_kind: EntryKind,
) -> None:
    source = f"{operation}-source"
    destination = f"nested/{operation}/leaf"
    if source_kind is EntryKind.FILE:
        await cached.upload_bytes(b"source", source)
    else:
        await cached.symlink("missing-target", source)

    assert await _listing_paths(cached, "") == [f"/{source}"]
    await cached._cache.set("iterdir", "nested", [])
    await cached._cache.set("iterdir", f"nested/{operation}", [])

    await getattr(cached, operation)(source, destination)

    invalidated = cached.dump_cache()
    for ancestor in ("", "nested", f"nested/{operation}"):
        assert ancestor not in invalidated.get("iterdir", {})
    assert destination not in invalidated.get("stat", {})
    assert invalidated["is_symlink"][destination] is (source_kind is EntryKind.SYMLINK)
    if source_kind is EntryKind.SYMLINK:
        _assert_no_follow_cache(cached, destination)

    for ancestor in ("", "nested", f"nested/{operation}"):
        assert await _listing_paths(cached, ancestor) == await _listing_paths(cached._storage, ancestor)


@pytest.mark.parametrize("operation", ["copy", "move"])
@pytest.mark.parametrize("source_kind", [EntryKind.FILE, EntryKind.SYMLINK])
async def test_nested_destination_failure_invalidates_all_ancestor_listings(
    cached: CachedStorage,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    source_kind: EntryKind,
) -> None:
    source = f"failed-{operation}-source"
    destination = f"partial/{operation}/leaf"
    if source_kind is EntryKind.FILE:
        await cached.upload_bytes(b"source", source)
    else:
        await cached.symlink("missing-target", source)

    assert await _listing_paths(cached, "") == [f"/{source}"]
    await cached._cache.set("iterdir", "partial", [])
    await cached._cache.set("iterdir", f"partial/{operation}", [])
    original = getattr(cached._storage, operation)

    async def partial_failure(src: str, dst: str, *, overwrite: bool = True) -> None:
        await original(src, dst, overwrite=overwrite)
        raise OSError("injected nested destination failure")

    monkeypatch.setattr(cached._storage, operation, partial_failure)

    with pytest.raises(OSError, match="nested destination failure"):
        await getattr(cached, operation)(source, destination)

    for ancestor in ("", "partial", f"partial/{operation}"):
        assert ancestor not in cached.dump_cache().get("iterdir", {})
        assert await _listing_paths(cached, ancestor) == await _listing_paths(cached._storage, ancestor)
