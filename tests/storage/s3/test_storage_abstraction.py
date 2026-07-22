import errno
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr
from pytest_mock import MockerFixture

from storegate.storage.abstract import (
    AbstractStorage,
    EntryKind,
    FileInfo,
    UnsupportedOperationError,
    WalkEntry,
)
from storegate.storage.s3 import S3Config, S3Storage
from storegate.storage.s3.utils import deserialize_file_info, serialize_file_info
from tests.support.ids import uid


def _config(*, endpoint: str | None = None, bucket: str = "test-bucket") -> S3Config:
    return S3Config(
        access_key_id=SecretStr("test"),
        secret_access_key=SecretStr("test"),
        region="us-east-1",
        bucket=bucket,
        endpoint_url=endpoint.removeprefix("http://") if endpoint is not None else None,
        path_style=endpoint is not None,
        scheme="http" if endpoint is not None else "https",
    )


@pytest.fixture
async def moto_s3_storage(_s3_server: tuple[str, str]) -> AsyncIterator[S3Storage]:
    endpoint, bucket = _s3_server
    async with S3Storage(_config(endpoint=endpoint, bucket=bucket)) as storage:
        yield storage


def test_directory_marker_roundtrip_uses_kind_only() -> None:
    modified = datetime(2026, 7, 18, 8, 0, tzinfo=UTC)
    created = datetime(2026, 7, 18, 7, 0, tzinfo=UTC)
    info = FileInfo(
        path="/parent/child",
        name="child",
        kind=EntryKind.DIRECTORY,
        size=0,
        modified=modified,
        created=created,
    )

    payload = serialize_file_info(info)
    decoded_json = json.loads(payload)

    assert decoded_json == {
        "path": "/parent/child",
        "name": "child",
        "kind": "directory",
        "size": 0,
        "modified": modified.isoformat(),
        "created": created.isoformat(),
    }
    assert deserialize_file_info("/ignored", payload) == info


def test_directory_marker_serializer_rejects_file() -> None:
    info = FileInfo(path="/file.txt", name="file.txt", kind=EntryKind.FILE, size=4)

    with pytest.raises(ValueError, match="require directory"):
        serialize_file_info(info)


def test_directory_marker_rejects_legacy_is_dir_without_kind() -> None:
    legacy = json.dumps({"path": "/legacy", "name": "legacy", "is_dir": True, "size": 0}).encode()

    with pytest.raises(KeyError, match="kind"):
        deserialize_file_info("/legacy", legacy)


@pytest.mark.parametrize("kind", ["file", "symlink", "unknown"])
def test_directory_marker_rejects_non_directory_kind(kind: str) -> None:
    payload = json.dumps({"path": "/invalid", "name": "invalid", "kind": kind, "size": 0}).encode()

    with pytest.raises(ValueError, match=r"directory|EntryKind"):
        deserialize_file_info("/invalid", payload)


async def test_s3_uses_default_unsupported_symlink_contract(mocker: MockerFixture) -> None:
    storage = S3Storage(_config())
    ordinary = FileInfo(path="/file.txt", name="file.txt", kind=EntryKind.FILE, size=7)
    stat = mocker.patch.object(storage, "stat", new=AsyncMock(return_value=ordinary))

    assert storage.capabilities.symlink_metadata is False
    assert storage.capabilities.readlink is False
    assert storage.capabilities.symlink_create is False
    assert S3Storage.lstat is AbstractStorage.lstat
    assert S3Storage.readlink is AbstractStorage.readlink
    assert S3Storage.symlink is AbstractStorage.symlink
    assert await storage.lstat("/file.txt") is ordinary
    assert await storage.is_symlink("/file.txt") is False
    assert stat.await_count == 2

    with pytest.raises(UnsupportedOperationError) as readlink_error:
        await storage.readlink("/file.txt")
    assert readlink_error.value.errno in {errno.ENOTSUP, errno.EOPNOTSUPP}

    with pytest.raises(UnsupportedOperationError) as symlink_error:
        await storage.symlink("file.txt", "/link")
    assert symlink_error.value.errno in {errno.ENOTSUP, errno.EOPNOTSUPP}


@pytest.mark.httpx
@pytest.mark.integration
async def test_moto_file_directory_walk_and_strict_marker_cutover(moto_s3_storage: S3Storage) -> None:
    storage = moto_s3_storage
    base = f"abstraction-{uid()}"
    legacy = f"legacy-{uid()}"
    client = storage._ensure_client()

    try:
        await storage.mkdir(f"/{base}/a-dir", parents=True)
        await storage.upload_bytes(b"b", f"/{base}/b.txt")
        await storage.upload_bytes(b"c", f"/{base}/a-dir/c.txt")

        directory = await storage.stat(f"/{base}")
        file = await storage.stat(f"/{base}/b.txt")
        walked = [entry async for entry in storage.walk(f"/{base}")]

        assert directory.kind is EntryKind.DIRECTORY
        assert file.kind is EntryKind.FILE
        assert all(isinstance(entry, WalkEntry) for entry in walked)
        assert walked[0].path == f"/{base}"
        assert [entry.path for entry in walked[0].entries] == [f"/{base}/a-dir", f"/{base}/b.txt"]
        assert walked[1].path == f"/{base}/a-dir"
        assert [entry.path for entry in walked[1].entries] == [f"/{base}/a-dir/c.txt"]

        marker = json.loads(await client.get_object(key=f"{base}/"))
        assert marker["kind"] == "directory"
        assert "is_dir" not in marker

        await client.put_object(
            key=f"{legacy}/",
            data=json.dumps({"path": f"/{legacy}", "name": legacy, "is_dir": True, "size": 0}).encode(),
        )
        with pytest.raises(KeyError, match="kind"):
            await storage.stat(f"/{legacy}")
    finally:
        await storage.rmtree(f"/{base}")
        await client.delete_object(key=f"{legacy}/")


@pytest.mark.httpx
@pytest.mark.integration
async def test_moto_walk_uses_file_precedence_for_file_marker_collision(moto_s3_storage: S3Storage) -> None:
    storage = moto_s3_storage
    path = f"collision-{uid()}"
    client = storage._ensure_client()
    marker = serialize_file_info(FileInfo(path=f"/{path}", name=path, kind=EntryKind.DIRECTORY))

    try:
        await client.put_object(key=path, data=b"file")
        await client.put_object(key=f"{path}/", data=marker)

        assert (await storage.lstat(f"/{path}")).kind is EntryKind.FILE
        assert await storage.exists(f"/{path}") is True
        assert await storage.is_dir(f"/{path}") is False
        with pytest.raises(NotADirectoryError, match="Not a directory"):
            _ = [entry async for entry in storage.walk(f"/{path}")]
    finally:
        await client.delete_object(key=path)
        await client.delete_object(key=f"{path}/")


@pytest.mark.httpx
@pytest.mark.integration
async def test_moto_directory_recognition_rejects_incompatible_root_markers(
    moto_s3_storage: S3Storage,
) -> None:
    storage = moto_s3_storage
    client = storage._ensure_client()
    cases: tuple[tuple[str, dict[str, object], type[Exception], str], ...] = (
        ("legacy", {"path": "/legacy", "name": "legacy", "is_dir": True, "size": 0}, KeyError, "kind"),
        ("missing", {"path": "/missing", "name": "missing", "size": 0}, KeyError, "kind"),
        (
            "non-directory",
            {"path": "/non-directory", "name": "non-directory", "kind": "file", "size": 0},
            ValueError,
            "directory",
        ),
        (
            "invalid",
            {"path": "/invalid", "name": "invalid", "kind": "unknown", "size": 0},
            ValueError,
            "EntryKind",
        ),
    )

    for label, marker, error, match in cases:
        path = f"strict-marker-{label}-{uid()}"
        try:
            await client.put_object(key=f"{path}/", data=json.dumps(marker).encode())

            with pytest.raises(error, match=match):
                await storage.exists(f"/{path}")
            with pytest.raises(error, match=match):
                await storage.is_dir(f"/{path}")
            with pytest.raises(error, match=match):
                _ = [entry async for entry in storage.walk(f"/{path}")]
        finally:
            await client.delete_object(key=f"{path}/")
