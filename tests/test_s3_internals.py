"""S3-specific tests — marker-object semantics and upload behavior."""

from pathlib import Path

import pytest

from app.storage import S3Storage
from tests.conftest import uid

pytestmark = pytest.mark.s3


@pytest.fixture
async def s3_storage():
    config_path = Path("data/s3/mock.json")
    if not config_path.exists():
        pytest.skip("S3 config file not found")
    async with S3Storage(config_path) as s:
        yield s


class TestUploadCreatesParentDirs:
    """Verify that uploading a nested file automatically creates parent directories."""

    async def test_upload_creates_parent_dirs(self, s3_storage: S3Storage):
        base = f"test-upload-pdir-{uid()}"
        try:
            await s3_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            assert await s3_storage.is_dir(base)
            assert await s3_storage.is_dir(f"{base}/sub")
            assert await s3_storage.exists(base)
        finally:
            await s3_storage.rmtree(base)

    async def test_uploaded_subdir_appears_in_iterdir(self, s3_storage: S3Storage):
        base = f"test-upload-itd-{uid()}"
        try:
            await s3_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            entries = [e async for e in s3_storage.iterdir(base)]
            names = {e.name for e in entries}
            assert "sub" in names, f"sub not found in {names}"
            sub_info = next(e for e in entries if e.name == "sub")
            assert sub_info.is_dir
        finally:
            await s3_storage.rmtree(base)

    async def test_uploaded_subdir_appears_in_walk(self, s3_storage: S3Storage):
        base = f"test-upload-walk-{uid()}"
        try:
            await s3_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            walked_dirs: list[str] = []
            async for sp, _sd, _sf in s3_storage.walk(base):
                walked_dirs.append(sp)
            assert len(walked_dirs) >= 2  # base + sub
        finally:
            await s3_storage.rmtree(base)
