"""DavStorage integration tests against a local wsgidav server."""

from __future__ import annotations

import contextlib

import pytest

from storegate.storage.abstract import EntryKind, WalkEntry
from storegate.storage.dav import DavStorage
from tests.support.ids import uid

pytestmark = [pytest.mark.integration, pytest.mark.httpx]


class TestDavStorageIntegration:
    async def test_root_stat(self, dav_storage: DavStorage) -> None:
        info = await dav_storage.stat("/")
        assert info.kind is EntryKind.DIRECTORY
        assert info.path == "/"

    async def test_move_overwrites_destination(self, dav_storage: DavStorage) -> None:
        src = f"itd-move-src-{uid()}"
        dst = f"itd-move-dst-{uid()}"
        try:
            await dav_storage.upload_bytes(b"src-content", src)
            await dav_storage.upload_bytes(b"dst-content", dst)
            await dav_storage.move(src, dst)
            # WebDAV MOVE must overwrite the destination (aligns with S3/Local).
            assert await dav_storage.download_bytes(dst) == b"src-content"
            assert not await dav_storage.exists(src)
        finally:
            with contextlib.suppress(Exception):
                await dav_storage.delete(dst)

    async def test_large_streaming_upload(self, dav_storage: DavStorage) -> None:
        path = f"itd-large-{uid()}"
        data = b"x" * (5 * 1024 * 1024)  # > default 4 MiB chunk_size
        try:
            await dav_storage.upload_bytes(data, path)
            assert await dav_storage.download_bytes(path) == data
        finally:
            with contextlib.suppress(Exception):
                await dav_storage.delete(path)

    async def test_range_download(self, dav_storage: DavStorage) -> None:
        path = f"itd-range-{uid()}"
        data = bytes(range(256)) * 40  # 10240 bytes
        try:
            await dav_storage.upload_bytes(data, path)
            chunks = [c async for c in dav_storage.download_stream(path, offset=100)]
            assert b"".join(chunks) == data[100:]
        finally:
            with contextlib.suppress(Exception):
                await dav_storage.delete(path)

    async def test_protocol_download_reads_to_eof(self, dav_storage: DavStorage) -> None:
        path = f"itd-eof-{uid()}"
        payload = b"webdav-reader-eof" * 1024
        try:
            await dav_storage.upload_bytes(payload, path)
            assert await dav_storage.download_bytes(path) == payload
        finally:
            with contextlib.suppress(Exception):
                await dav_storage.delete(path)

    async def test_copytree_deep_structure(self, dav_storage: DavStorage) -> None:
        src = f"itd-ct-{uid()}"
        dst = f"itd-ct-dst-{uid()}"
        try:
            await dav_storage.mkdir(f"{src}/a/b", parents=True)
            await dav_storage.upload_bytes(b"1", f"{src}/f.txt")
            await dav_storage.upload_bytes(b"2", f"{src}/a/f.txt")
            await dav_storage.upload_bytes(b"3", f"{src}/a/b/f.txt")
            await dav_storage.copytree(src, dst)
            assert await dav_storage.download_bytes(f"{dst}/f.txt") == b"1"
            assert await dav_storage.download_bytes(f"{dst}/a/f.txt") == b"2"
            assert await dav_storage.download_bytes(f"{dst}/a/b/f.txt") == b"3"
        finally:
            with contextlib.suppress(Exception):
                await dav_storage.rmtree(src)
            with contextlib.suppress(Exception):
                await dav_storage.rmtree(dst)

    async def test_walk_returns_sorted_structured_entries(self, dav_storage: DavStorage) -> None:
        base = f"itd-walk-{uid()}"
        try:
            await dav_storage.mkdir(f"{base}/sub", parents=True)
            await dav_storage.upload_bytes(b"z", f"{base}/z.txt")
            await dav_storage.upload_bytes(b"a", f"{base}/a.txt")
            await dav_storage.upload_bytes(b"nested", f"{base}/sub/nested.txt")

            walked = [entry async for entry in dav_storage.walk(base)]

            assert all(isinstance(entry, WalkEntry) for entry in walked)
            assert [entry.path for entry in walked] == [f"/{base}", f"/{base}/sub"]
            assert [(entry.name, entry.kind) for entry in walked[0].entries] == [
                ("a.txt", EntryKind.FILE),
                ("sub", EntryKind.DIRECTORY),
                ("z.txt", EntryKind.FILE),
            ]
            assert [(entry.name, entry.kind) for entry in walked[1].entries] == [("nested.txt", EntryKind.FILE)]
        finally:
            with contextlib.suppress(Exception):
                await dav_storage.rmtree(base)

    async def test_rmtree_recursive_single_request(self, dav_storage: DavStorage) -> None:
        base = f"itd-rm-{uid()}"
        await dav_storage.mkdir(f"{base}/a/b", parents=True)
        await dav_storage.upload_bytes(b"x", f"{base}/a/b/f.txt")
        await dav_storage.rmtree(base)
        assert not await dav_storage.exists(base)
