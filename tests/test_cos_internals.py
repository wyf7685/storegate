"""COS-specific tests — marker-object semantics and upload behavior."""

from pathlib import Path

import pytest

from app.storage import CosStorage
from tests.conftest import uid

pytestmark = pytest.mark.cos


@pytest.fixture
async def cos_storage():
    config_path = Path("data/cos/mock.json")
    if not config_path.exists():
        pytest.skip("COS config file not found")
    async with CosStorage(config_path) as s:
        yield s


class TestUploadCreatesParentDirs:
    """Verify that uploading a nested file automatically creates parent directories."""

    async def test_upload_creates_parent_dirs(self, cos_storage: CosStorage):
        base = f"test-upload-pdir-{uid()}"
        try:
            await cos_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            assert await cos_storage.is_dir(base)
            assert await cos_storage.is_dir(f"{base}/sub")
            assert await cos_storage.exists(base)
        finally:
            await cos_storage.rmtree(base)

    async def test_uploaded_subdir_appears_in_iterdir(self, cos_storage: CosStorage):
        base = f"test-upload-itd-{uid()}"
        try:
            await cos_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            entries = [e async for e in cos_storage.iterdir(base)]
            names = {e.name for e in entries}
            assert "sub" in names, f"sub not found in {names}"
            sub_info = next(e for e in entries if e.name == "sub")
            assert sub_info.is_dir
        finally:
            await cos_storage.rmtree(base)

    async def test_uploaded_subdir_appears_in_walk(self, cos_storage: CosStorage):
        base = f"test-upload-walk-{uid()}"
        try:
            await cos_storage.upload_bytes(b"hello", f"{base}/sub/file.txt")
            walked_dirs: list[str] = []
            async for sp, _sd, _sf in cos_storage.walk(base):
                walked_dirs.append(sp)
            assert len(walked_dirs) >= 2  # base + sub
        finally:
            await cos_storage.rmtree(base)
