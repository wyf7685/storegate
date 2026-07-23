import errno
import json

import pytest
from pydantic import ValidationError

from storegate.storage import EntryKind, FileInfo, StorageCapabilities, UnsupportedOperationError, WalkEntry
from storegate.storage.index import IndexStorage
from storegate.storage.index.ref import hash_to_path
from storegate.storage.index.storage import CHUNKS_INDEX_FILE, FileMeta
from storegate.storage.memory import MemoryStorage


def _inject_memory_symlink(storage: MemoryStorage, link_path: str, target: str) -> None:
    key = storage._resolve(link_path)
    storage._files.pop(key, None)
    storage._dirs.discard(key)
    storage._links[key] = target


def test_file_meta_uses_strict_file_kind_schema() -> None:
    meta = FileMeta(
        info=FileInfo(path="/file.txt", name="file.txt", kind=EntryKind.FILE, size=7),
        chunks=["abc"],
    )

    payload = meta.model_dump_json()
    assert '"kind":"file"' in payload
    assert "is_dir" not in payload
    assert FileMeta.model_validate_json(payload) == meta

    legacy_payload = json.dumps(
        {
            "info": {"path": "/file.txt", "name": "file.txt", "is_dir": False, "size": 7},
            "chunks": ["abc"],
        }
    )
    with pytest.raises(ValidationError, match="Legacy FileInfo is_dir metadata is not supported"):
        FileMeta.model_validate_json(legacy_payload)

    directory_payload = json.dumps(
        {
            "info": {"path": "/dir", "name": "dir", "kind": "directory", "size": 0},
            "chunks": [],
        }
    )
    with pytest.raises(ValidationError, match="must describe a regular file"):
        FileMeta.model_validate_json(directory_payload)


async def test_public_namespace_has_no_symlink_capabilities(index_storage: IndexStorage) -> None:
    await index_storage.mkdir("/dir")
    await index_storage.upload_bytes(b"data", "/file.txt")

    assert index_storage.capabilities == StorageCapabilities()
    for path in ("/dir", "/file.txt"):
        assert await index_storage.lstat(path) == await index_storage.stat(path)
        assert await index_storage.is_symlink(path) is False

    unsupported_errno = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)
    with pytest.raises(UnsupportedOperationError) as readlink_error:
        await index_storage.readlink("/file.txt")
    assert readlink_error.value.errno == unsupported_errno

    with pytest.raises(UnsupportedOperationError) as symlink_error:
        await index_storage.symlink("file.txt", "/link")
    assert symlink_error.value.errno == unsupported_errno


async def test_walk_returns_walk_entries_with_explicit_kinds(index_storage: IndexStorage) -> None:
    await index_storage.mkdir("/tree/child", parents=True)
    await index_storage.upload_bytes(b"root", "/tree/root.txt")
    await index_storage.upload_bytes(b"child", "/tree/child/nested.txt")

    walked = [entry async for entry in index_storage.walk("/tree")]

    assert all(isinstance(entry, WalkEntry) for entry in walked)
    assert walked[0].path == "/tree"
    assert [(entry.path, entry.kind) for entry in walked[0].entries] == [
        ("/tree/child", EntryKind.DIRECTORY),
        ("/tree/root.txt", EntryKind.FILE),
    ]
    assert walked[1].path == "/tree/child"
    assert [(entry.path, entry.kind) for entry in walked[1].entries] == [
        ("/tree/child/nested.txt", EntryKind.FILE),
    ]


async def test_connect_rejects_symlink_binding_before_reading_target() -> None:
    index = MemoryStorage("/")
    chunks = MemoryStorage("/")
    await chunks.upload_bytes(
        json.dumps(
            {"version": 2, "index_namespace_identity": index.namespace_identity},
            separators=(",", ":"),
        ).encode(),
        "/binding-target",
    )
    _inject_memory_symlink(chunks, CHUNKS_INDEX_FILE, "/binding-target")
    storage = IndexStorage(index, chunks)

    with pytest.raises(UnsupportedOperationError, match="binding file"):
        await storage.connect()


async def test_metadata_symlink_is_rejected_before_parse(index_storage: IndexStorage) -> None:
    index = index_storage._index
    assert isinstance(index, MemoryStorage)
    valid_meta = FileMeta(
        info=FileInfo(path="/target", name="target", kind=EntryKind.FILE, size=4),
        chunks=[],
    )
    await index.upload_bytes(valid_meta.model_dump_json().encode(), "/metadata-target")
    _inject_memory_symlink(index, "/linked-meta", "/metadata-target")

    with pytest.raises(UnsupportedOperationError, match="metadata entry"):
        await index_storage._get_file_meta("/linked-meta")


async def test_chunk_data_and_refs_reject_symlinks(index_storage: IndexStorage) -> None:
    data = b"chunk payload"
    await index_storage.upload_bytes(data, "/file.txt")
    meta = await index_storage._get_file_meta("/file.txt")
    assert meta is not None
    chunk_hash = meta.chunks[0]
    chunks = index_storage._chunks
    assert isinstance(chunks, MemoryStorage)

    bin_path = hash_to_path(chunk_hash, "bin")
    bin_key = chunks._resolve(bin_path)
    original_chunk = chunks._files.pop(bin_key)
    await chunks.upload_bytes(original_chunk, "/chunk-target")
    _inject_memory_symlink(chunks, bin_path, "/chunk-target")

    with pytest.raises(UnsupportedOperationError, match="chunk data"):
        await index_storage.download_bytes("/file.txt")

    chunks._links.pop(bin_key)
    chunks._files[bin_key] = original_chunk
    ref_path = hash_to_path(chunk_hash, "ref")
    ref_key = chunks._resolve(ref_path)
    original_refs = chunks._files.pop(ref_key)
    await chunks.upload_bytes(original_refs, "/ref-target")
    _inject_memory_symlink(chunks, ref_path, "/ref-target")

    with pytest.raises(UnsupportedOperationError, match="chunk reference file"):
        await index_storage._refs.load_refs(chunk_hash)


async def test_lock_symlink_is_rejected_before_optimistic_read(index_storage: IndexStorage) -> None:
    index = index_storage._index
    assert isinstance(index, MemoryStorage)
    await index.upload_bytes(b'{"owner":"other"}', "/lock-target")
    _inject_memory_symlink(index, "/guard.lock", "/lock-target")

    with pytest.raises(UnsupportedOperationError, match="lock file"):
        await index_storage._locker.acquire_lock(index, "/guard.lock")


async def test_tree_symlink_is_rejected_before_copy_or_delete(index_storage: IndexStorage) -> None:
    await index_storage.mkdir("/tree")
    await index_storage.upload_bytes(b"keep", "/tree/file.txt")
    index = index_storage._index
    assert isinstance(index, MemoryStorage)
    await index.upload_bytes(b"target", "/target")
    _inject_memory_symlink(index, "/tree/link", "/target")

    with pytest.raises(UnsupportedOperationError, match="tree entry"):
        await index_storage.copytree("/tree", "/copy")
    assert not await index_storage.exists("/copy")

    with pytest.raises(UnsupportedOperationError, match="tree entry"):
        await index_storage.rmtree("/tree")
    assert await index_storage.download_bytes("/tree/file.txt") == b"keep"
