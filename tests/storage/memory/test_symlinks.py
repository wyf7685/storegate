import errno

import pytest

from app.storage import EntryKind, WalkEntry
from app.storage.memory import MemoryStorage


async def test_capabilities_and_mutually_exclusive_namespace() -> None:
    storage = MemoryStorage("/")
    assert storage.capabilities.symlink_metadata
    assert storage.capabilities.readlink
    assert storage.capabilities.symlink_create
    assert storage.capabilities is storage.capabilities

    await storage.upload_bytes(b"file", "/entry")
    with pytest.raises(FileExistsError):
        await storage.symlink("target", "/entry")
    await storage.symlink("./target", "/entry", overwrite=True)
    assert await storage.readlink("/entry") == "./target"
    assert await storage.is_symlink("/entry")
    assert not await storage.exists("/entry")

    with pytest.raises(FileExistsError):
        await storage.mkdir("/entry")
    with pytest.raises(OSError, match="Refusing to upload through symlink"):
        await storage.upload_bytes(b"replacement", "/entry", overwrite=True)

    await storage.mkdir("/directory")
    with pytest.raises(FileExistsError):
        await storage.symlink("target", "/directory", overwrite=False)
    assert await storage.is_dir("/directory")
    with pytest.raises(IsADirectoryError):
        await storage.symlink("target", "/directory", overwrite=True)
    assert await storage.is_dir("/directory")
    assert await storage.list_("/directory") == []

    assert storage._files.keys().isdisjoint(storage._dirs)
    assert storage._files.keys().isdisjoint(storage._links)
    assert storage._dirs.isdisjoint(storage._links)


async def test_lstat_stat_dangling_chain_and_query_identity() -> None:
    storage = MemoryStorage("/")
    await storage.mkdir("/data")
    await storage.upload_bytes(b"payload", "/data/file.bin")
    await storage.symlink("data/file.bin", "/target")
    await storage.symlink("target", "/chain")
    await storage.symlink("missing", "/dangling")

    lexical = await storage.lstat("/chain")
    assert lexical.kind is EntryKind.SYMLINK
    assert lexical.path == "/chain"
    assert lexical.name == "chain"

    followed = await storage.stat("/chain")
    assert followed.kind is EntryKind.FILE
    assert followed.path == "/chain"
    assert followed.name == "chain"
    assert followed.size == len(b"payload")
    assert await storage.download_bytes("/chain") == b"payload"

    assert await storage.is_symlink("/dangling")
    assert not await storage.exists("/dangling")
    assert not await storage.is_file("/dangling")
    assert not await storage.is_dir("/dangling")
    with pytest.raises(FileNotFoundError):
        await storage.stat("/dangling")


async def test_cycles_hop_guard_and_absolute_fixture_targets() -> None:
    storage = MemoryStorage("/sandbox")
    await storage.upload_bytes(b"inside", "/inside")
    storage._links[storage._resolve("/absolute")] = "/sandbox/inside"
    storage._links[storage._resolve("/absolute-escape")] = "/outside"
    await storage.symlink("cycle-b", "/cycle-a")
    await storage.symlink("cycle-a", "/cycle-b")

    assert await storage.download_bytes("/absolute") == b"inside"
    with pytest.raises(PermissionError) as escape:
        await storage.stat("/absolute-escape")
    assert escape.value.errno == errno.EACCES

    with pytest.raises(OSError, match="Too many symbolic links") as cycle:
        await storage.stat("/cycle-a")
    assert cycle.value.errno == errno.ELOOP

    for index in range(41):
        await storage.symlink(f"hop-{index + 1}" if index < 40 else "inside", f"/hop-{index}")
    with pytest.raises(OSError, match="Too many symbolic links") as too_many:
        await storage.stat("/hop-0")
    assert too_many.value.errno == errno.ELOOP


async def test_relative_root_escape_and_public_absolute_creation_rejected() -> None:
    storage = MemoryStorage("/sandbox")
    await storage.mkdir("/safe")
    await storage.symlink("../../outside", "/safe/escape")

    with pytest.raises(PermissionError) as escape:
        await storage.stat("/safe/escape")
    assert escape.value.errno == errno.EACCES
    with pytest.raises(ValueError, match="must be relative"):
        await storage.symlink("/sandbox/target", "/absolute-create")


async def test_caller_intermediate_symlink_is_rejected_without_target_mutation() -> None:
    storage = MemoryStorage("/")
    await storage.mkdir("/real")
    await storage.upload_bytes(b"original", "/real/file")
    await storage.symlink("real", "/alias")

    with pytest.raises(OSError, match="Intermediate path is a symlink"):
        await storage.download_bytes("/alias/file")
    with pytest.raises(OSError, match="Intermediate path is a symlink"):
        await storage.upload_bytes(b"changed", "/alias/file")
    with pytest.raises(OSError, match="Intermediate path is a symlink"):
        await storage.unlink("/alias/file")

    assert await storage.download_bytes("/real/file") == b"original"


async def test_download_follows_final_link_and_upload_always_rejects_it() -> None:
    storage = MemoryStorage("/")
    await storage.upload_bytes(b"original", "/target")
    await storage.symlink("target", "/link")

    assert await storage.download_bytes("/link") == b"original"
    with pytest.raises(OSError, match="Refusing to upload through symlink"):
        await storage.upload_bytes(b"changed", "/link", overwrite=False)
    with pytest.raises(OSError, match="Refusing to upload through symlink"):
        await storage.upload_bytes(b"changed", "/link", overwrite=True)
    assert await storage.download_bytes("/target") == b"original"
    assert await storage.readlink("/link") == "target"


async def test_unlink_delete_copy_and_move_preserve_lexical_link() -> None:
    storage = MemoryStorage("/")
    await storage.upload_bytes(b"target", "/target")
    await storage.symlink("target", "/source")

    await storage.copy("/source", "/copied")
    await storage.move("/copied", "/moved")
    assert await storage.readlink("/source") == "target"
    assert await storage.readlink("/moved") == "target"
    assert not await storage.is_symlink("/copied")

    await storage.delete("/source")
    assert await storage.download_bytes("/target") == b"target"
    await storage.unlink("/moved")
    assert await storage.download_bytes("/target") == b"target"


async def test_walk_is_sorted_structured_and_never_recurses_links() -> None:
    storage = MemoryStorage("/")
    await storage.mkdir("/tree/real/sub", parents=True)
    await storage.upload_bytes(b"z", "/tree/z-file")
    await storage.upload_bytes(b"a", "/tree/a-file")
    await storage.symlink("real", "/tree/link-directory")
    await storage.symlink("missing", "/tree/dangling")

    walked = [entry async for entry in storage.walk("/tree")]
    assert all(isinstance(entry, WalkEntry) for entry in walked)
    assert [entry.path for entry in walked] == ["/tree", "/tree/real", "/tree/real/sub"]
    assert [entry.path for entry in walked[0].entries] == sorted(entry.path for entry in walked[0].entries)
    by_name = {entry.name: entry for entry in walked[0].entries}
    assert by_name["link-directory"].kind is EntryKind.SYMLINK
    assert by_name["dangling"].kind is EntryKind.SYMLINK
    assert "/tree/link-directory" not in {entry.path for entry in walked}

    with pytest.raises(NotADirectoryError):
        _ = [entry async for entry in storage.walk("/tree/link-directory")]


async def test_rmtree_treats_links_as_leaves_and_preserves_external_target() -> None:
    storage = MemoryStorage("/")
    await storage.mkdir("/outside")
    await storage.upload_bytes(b"sentinel", "/outside/file")
    await storage.mkdir("/tree/sub", parents=True)
    await storage.symlink("../../outside", "/tree/sub/link")

    await storage.rmtree("/tree")
    assert not await storage.exists("/tree")
    assert await storage.download_bytes("/outside/file") == b"sentinel"

    await storage.symlink("outside", "/root-link")
    with pytest.raises(NotADirectoryError):
        await storage.rmtree("/root-link")
    assert await storage.download_bytes("/outside/file") == b"sentinel"


async def test_copytree_and_movetree_preserve_raw_links_including_dangling() -> None:
    storage = MemoryStorage("/")
    await storage.mkdir("/source/sub", parents=True)
    await storage.upload_bytes(b"payload", "/source/sub/file")
    await storage.symlink("./sub/file", "/source/file-link")
    await storage.symlink("../missing", "/source/sub/dangling")

    await storage.copytree("/source", "/copied")
    assert await storage.readlink("/copied/file-link") == "./sub/file"
    assert await storage.readlink("/copied/sub/dangling") == "../missing"
    assert not await storage.exists("/copied/sub/dangling")

    await storage.movetree("/copied", "/moved")
    assert not await storage.exists("/copied")
    assert await storage.readlink("/moved/file-link") == "./sub/file"
    assert await storage.readlink("/moved/sub/dangling") == "../missing"
    assert await storage.download_bytes("/source/sub/file") == b"payload"


async def test_copytree_preflight_failure_leaves_destination_unchanged() -> None:
    storage = MemoryStorage("/")
    await storage.mkdir("/source")
    await storage.upload_bytes(b"first", "/source/a-file")
    await storage.upload_bytes(b"blocked", "/source/z-conflict")
    await storage.mkdir("/destination/z-conflict", parents=True)
    await storage.upload_bytes(b"sentinel", "/destination/z-conflict/sentinel")

    with pytest.raises(IsADirectoryError):
        await storage.copytree("/source", "/destination", overwrite=True)

    assert not await storage.exists("/destination/a-file")
    assert await storage.download_bytes("/destination/z-conflict/sentinel") == b"sentinel"
    assert await storage.download_bytes("/source/a-file") == b"first"
