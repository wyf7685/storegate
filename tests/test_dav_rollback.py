"""DavStorage error-path unit tests with a mocked client (no network)."""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from app.storage.abstract import FileInfo
from app.storage.dav import DavStorage
from app.storage.dav.dav_client.errors import DavHttpStatusError


@pytest.fixture
def dav_mocked(mocker: MockerFixture) -> DavStorage:
    from app.storage.dav import DavConfig

    cfg = DavConfig(base_url="http://localhost/dav", auth_mode="anonymous")
    s = DavStorage(cfg)
    mocker.patch.object(s, "connect", AsyncMock())
    mocker.patch.object(s, "close", AsyncMock())
    s._client = MagicMock()
    return s


async def _stream(*chunks: bytes) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


class TestStatErrorMapping:
    async def test_stat_404_raises_filenotfound(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            AsyncMock(side_effect=DavHttpStatusError("PROPFIND", "http://u/x", 404, "not found")),
        )
        with pytest.raises(FileNotFoundError):
            await dav_mocked.stat("x")

    async def test_stat_403_raises_permission(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            AsyncMock(side_effect=DavHttpStatusError("PROPFIND", "http://u/x", 403, "forbidden")),
        )
        with pytest.raises(PermissionError):
            await dav_mocked.stat("x")


class TestUploadStatGateway:
    async def test_upload_to_directory_raises(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/d", name="d", is_dir=True)),
        )
        put_mock = mocker.patch.object(dav_mocked._client, "put", AsyncMock())
        with pytest.raises(IsADirectoryError):
            await dav_mocked.upload_stream(_stream(b"data"), "d")
        put_mock.assert_not_called()

    async def test_upload_no_overwrite_raises(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/f", name="f", is_dir=False, size=3)),
        )
        put_mock = mocker.patch.object(dav_mocked._client, "put", AsyncMock())
        with pytest.raises(FileExistsError):
            await dav_mocked.upload_stream(_stream(b"data"), "f", overwrite=False)
        put_mock.assert_not_called()


class TestRmdir:
    async def test_nonempty_does_not_delete(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/d", name="d", is_dir=True)),
        )
        mocker.patch.object(dav_mocked, "_is_dir_empty", new=AsyncMock(return_value=False))
        delete_mock = mocker.patch.object(dav_mocked._client, "delete", AsyncMock())
        with pytest.raises(OSError):  # noqa: PT011
            await dav_mocked.rmdir("d")
        delete_mock.assert_not_called()

    async def test_file_raises_not_a_directory(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/f", name="f", is_dir=False, size=1)),
        )
        delete_mock = mocker.patch.object(dav_mocked._client, "delete", AsyncMock())
        with pytest.raises(NotADirectoryError):
            await dav_mocked.rmdir("f")
        delete_mock.assert_not_called()


class TestMkdirErrorMapping:
    async def test_mkdir_405_raises_fileexists(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(side_effect=FileNotFoundError("x")))
        mocker.patch.object(
            dav_mocked._client,
            "mkcol",
            AsyncMock(side_effect=DavHttpStatusError("MKCOL", "http://u/d", 405, "exists")),
        )
        with pytest.raises(FileExistsError):
            await dav_mocked.mkdir("d")

    async def test_mkdir_409_raises_filenotfound(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(side_effect=FileNotFoundError("x")))
        mocker.patch.object(
            dav_mocked._client,
            "mkcol",
            AsyncMock(side_effect=DavHttpStatusError("MKCOL", "http://u/d", 409, "conflict")),
        )
        with pytest.raises(FileNotFoundError):
            await dav_mocked.mkdir("d")


class TestMoveOverwrite:
    async def test_move_412_retries_after_delete(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        move_mock = mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(side_effect=[DavHttpStatusError("MOVE", "http://u/dst", 412, "overwrite"), None]),
        )
        delete_mock = mocker.patch.object(dav_mocked._client, "delete", AsyncMock())
        await dav_mocked.move("src", "dst")
        delete_mock.assert_awaited_once_with("dst")
        assert move_mock.await_count == 2

    async def test_move_404_raises_filenotfound(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(side_effect=DavHttpStatusError("MOVE", "http://u/src", 404, "not found")),
        )
        with pytest.raises(FileNotFoundError):
            await dav_mocked.move("src", "dst")


class TestCopytreeFallback:
    async def test_server_copy_501_triggers_fallback(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        src_info = FileInfo(path="/src", name="src", is_dir=True)
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(return_value=src_info))
        mocker.patch.object(dav_mocked, "exists", new=AsyncMock(return_value=False))
        mocker.patch.object(
            dav_mocked._client,
            "copy",
            AsyncMock(side_effect=DavHttpStatusError("COPY", "http://u/dst", 501, "not supported")),
        )
        fallback_mock = mocker.patch.object(dav_mocked, "_copytree_fallback", AsyncMock())
        await dav_mocked.copytree("src", "dst")
        # Server-side COPY failed with 501 → fallback path must be used.
        fallback_mock.assert_awaited_once()

    async def test_server_copy_success_skips_fallback(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        src_info = FileInfo(path="/src", name="src", is_dir=True)
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(return_value=src_info))
        mocker.patch.object(dav_mocked, "exists", new=AsyncMock(return_value=False))
        mocker.patch.object(dav_mocked._client, "copy", AsyncMock())
        fallback_mock = mocker.patch.object(dav_mocked, "_copytree_fallback", AsyncMock())
        await dav_mocked.copytree("src", "dst")
        fallback_mock.assert_not_awaited()
