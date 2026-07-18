"""S3 marker-directory integration tests."""

import pytest

from app.storage.s3 import S3Storage
from tests.support.ids import uid

pytestmark = pytest.mark.s3


class TestMarkerDirectories:
    """Verify that uploading a nested file automatically creates parent directories."""

    async def test_upload_creates_parent_dirs(self, real_s3_storage: S3Storage):
        base = f"test-upload-pdir-{uid()}"
        try:
            await real_s3_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            assert await real_s3_storage.is_dir(base)
            assert await real_s3_storage.is_dir(f"{base}/sub")
            assert await real_s3_storage.exists(base)
        finally:
            await real_s3_storage.rmtree(base)

    async def test_uploaded_subdir_appears_in_iterdir(self, real_s3_storage: S3Storage):
        base = f"test-upload-itd-{uid()}"
        try:
            await real_s3_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            entries = [e async for e in real_s3_storage.iterdir(base)]
            names = {e.name for e in entries}
            assert "sub" in names, f"sub not found in {names}"
            sub_info = next(e for e in entries if e.name == "sub")
            assert sub_info.is_dir
        finally:
            await real_s3_storage.rmtree(base)

    async def test_uploaded_subdir_appears_in_walk(self, real_s3_storage: S3Storage):
        base = f"test-upload-walk-{uid()}"
        try:
            await real_s3_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            walked_dirs = [walked.path async for walked in real_s3_storage.walk(base)]
            assert len(walked_dirs) >= 2  # base + sub
        finally:
            await real_s3_storage.rmtree(base)
