"""IndexStorage behavior tests."""

import hashlib
import os

from app.storage.index import IndexStorage
from tests.storage.index.helpers import BLOCK_SIZE
from tests.support.ids import uid


class TestUploadDownloadIntegrity:
    """End-to-end upload/download with SHA-256 verification."""

    async def test_small_file_roundtrip(self, index_storage: IndexStorage):
        data = b"hello index storage"
        path = f"test-idx-small-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            downloaded = await index_storage.download_bytes(path)
            assert downloaded == data
        finally:
            await index_storage.delete(path)

    async def test_multi_block_roundtrip(self, index_storage: IndexStorage):
        # 48 KB → 3 blocks with 16 KB block_size
        data = os.urandom(48 * 1024)
        path = f"test-idx-multi-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            downloaded = await index_storage.download_bytes(path)
            assert downloaded == data
            assert hashlib.sha256(downloaded).hexdigest() == hashlib.sha256(data).hexdigest()
        finally:
            await index_storage.delete(path)


class TestDownloadStreamOffset:
    """download_stream with offset > 0."""

    async def test_offset_middle(self, index_storage: IndexStorage):
        data = b"0123456789" * 100  # 1000 bytes
        path = f"test-dl-offset-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            chunks = [chunk async for chunk in index_storage.download_stream(path, offset=500)]
            result = b"".join(chunks)
            assert result == data[500:]
        finally:
            await index_storage.delete(path)

    async def test_offset_beyond_file(self, index_storage: IndexStorage):
        data = b"short"
        path = f"test-dl-offbeyond-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            chunks = [chunk async for chunk in index_storage.download_stream(path, offset=100)]
            assert b"".join(chunks) == b""
        finally:
            await index_storage.delete(path)

    async def test_offset_at_chunk_boundary(self, index_storage: IndexStorage):
        # upload 2+ blocks so offset falls at exact chunk boundary
        block_a = b"A" * BLOCK_SIZE
        block_b = b"B" * BLOCK_SIZE
        data = block_a + block_b
        path = f"test-dl-offchunk-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            chunks = [chunk async for chunk in index_storage.download_stream(path, offset=BLOCK_SIZE)]
            result = b"".join(chunks)
            assert result == block_b
        finally:
            await index_storage.delete(path)
