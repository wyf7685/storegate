"""DavStorage error-path unit tests with a mocked client (no network)."""

from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from app.storage.abstract import FileInfo
from app.storage.dav import DavStorage
from app.storage.dav.client.errors import DavHttpStatusError


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

    async def test_empty_upload_uses_empty_bytes_body(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(side_effect=FileNotFoundError))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        put_mock = mocker.patch.object(dav_mocked._client, "put", AsyncMock())

        await dav_mocked.upload_stream(_stream(), "empty.bin")

        put_mock.assert_awaited_once_with("empty.bin", b"")


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
    async def test_move_412_stages_destination_before_retry(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(side_effect=[FileInfo(path="/src", name="src", is_dir=False), FileNotFoundError("dst")]),
        )
        mocker.patch.object(dav_mocked, "is_dir", new=AsyncMock(return_value=False))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        move_mock = mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(side_effect=[DavHttpStatusError("MOVE", "http://u/dst", 412, "overwrite"), None, None]),
        )
        delete_mock = mocker.patch.object(dav_mocked._client, "delete", AsyncMock())

        await dav_mocked.move("src", "dst")

        assert move_mock.await_count == 3
        assert delete_mock.await_args is not None
        assert delete_mock.await_args.args[0].startswith("dst.storegate-move-backup-")

    async def test_move_404_raises_filenotfound(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(side_effect=FileNotFoundError("src")))
        move_mock = mocker.patch.object(dav_mocked._client, "move", AsyncMock())

        with pytest.raises(FileNotFoundError):
            await dav_mocked.move("src", "dst")

        move_mock.assert_not_awaited()

    async def test_move_412_retries_when_destination_races_missing(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(side_effect=[FileInfo(path="/src", name="src", is_dir=False), FileNotFoundError("dst")]),
        )
        mocker.patch.object(dav_mocked, "is_dir", new=AsyncMock(return_value=False))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        move_mock = mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(
                side_effect=[
                    DavHttpStatusError("MOVE", "http://u/dst", 412, "overwrite"),
                    DavHttpStatusError("MOVE", "http://u/dst", 404, "missing"),
                    None,
                ]
            ),
        )
        delete_mock = mocker.patch.object(dav_mocked._client, "delete", AsyncMock())

        await dav_mocked.move("src", "dst")

        assert move_mock.await_count == 3
        delete_mock.assert_not_awaited()

    async def test_move_412_stage_failure_preserves_destination(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(side_effect=[FileInfo(path="/src", name="src", is_dir=False), FileNotFoundError("dst")]),
        )
        mocker.patch.object(dav_mocked, "is_dir", new=AsyncMock(return_value=False))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        move_mock = mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(
                side_effect=[
                    DavHttpStatusError("MOVE", "http://u/dst", 412, "overwrite"),
                    DavHttpStatusError("MOVE", "http://u/dst", 500, "failed"),
                ]
            ),
        )
        delete_mock = mocker.patch.object(dav_mocked._client, "delete", AsyncMock())

        with pytest.raises(OSError, match="Failed to stage destination"):
            await dav_mocked.move("src", "dst")

        assert move_mock.await_count == 2
        delete_mock.assert_not_awaited()

    async def test_move_retry_failure_reconciles_staged_destination(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(side_effect=[FileInfo(path="/src", name="src", is_dir=False), FileNotFoundError("dst")]),
        )
        mocker.patch.object(dav_mocked, "is_dir", new=AsyncMock(return_value=False))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        move_mock = mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(
                side_effect=[
                    DavHttpStatusError("MOVE", "http://u/dst", 412, "overwrite"),
                    None,
                    DavHttpStatusError("MOVE", "http://u/src", 500, "retry failed"),
                ]
            ),
        )
        reconcile_mock = mocker.patch.object(dav_mocked, "_reconcile_failed_move", new=AsyncMock())

        with pytest.raises(OSError, match="Failed to move"):
            await dav_mocked.move("src", "dst")

        assert move_mock.await_count == 3
        reconcile_mock.assert_awaited_once()

    async def test_retry_and_restore_response_loss_reconciles_all_paths(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
    ) -> None:
        state = {"src": b"source", "dst": b"old destination"}
        move_count = 0

        async def _stat(path: str) -> FileInfo:
            if path not in state:
                raise FileNotFoundError(path)
            return FileInfo(path=f"/{path}", name=path, is_dir=False)

        async def _move(old: str, new: str, *, overwrite: bool) -> None:
            nonlocal move_count
            _ = overwrite
            move_count += 1
            if move_count == 1:
                raise DavHttpStatusError("MOVE", "http://u/dst", 412, "overwrite")
            state[new] = state.pop(old)
            if move_count in {3, 4}:
                raise OSError("response lost after mutation")

        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(side_effect=_stat))
        mocker.patch.object(dav_mocked, "is_dir", new=AsyncMock(return_value=False))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(dav_mocked._client, "move", new=AsyncMock(side_effect=_move))

        with pytest.raises(OSError, match="response lost after mutation"):
            await dav_mocked.move("src", "dst")

        assert state == {"src": b"source", "dst": b"old destination"}
        assert move_count == 5
        assert not any(".storegate-move-backup-" in path for path in state)

    @pytest.mark.parametrize("delete_after_mutation", [False, True])
    async def test_committed_move_confirms_backup_cleanup(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        delete_after_mutation: bool,
    ) -> None:
        state = {"src": b"source", "dst": b"old destination"}
        move_count = 0
        delete_attempts = 0

        async def _stat(path: str) -> FileInfo:
            if path not in state:
                raise FileNotFoundError(path)
            return FileInfo(path=f"/{path}", name=path, is_dir=False)

        async def _move(old: str, new: str, *, overwrite: bool) -> None:
            nonlocal move_count
            _ = overwrite
            move_count += 1
            if move_count == 1:
                raise DavHttpStatusError("MOVE", "http://u/dst", 412, "overwrite")
            state[new] = state.pop(old)

        async def _delete(path: str) -> None:
            nonlocal delete_attempts
            delete_attempts += 1
            if delete_attempts == 1:
                if delete_after_mutation:
                    del state[path]
                raise OSError("injected cleanup response loss")
            del state[path]

        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(side_effect=_stat))
        mocker.patch.object(dav_mocked, "is_dir", new=AsyncMock(return_value=False))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(dav_mocked._client, "move", new=AsyncMock(side_effect=_move))
        mocker.patch.object(dav_mocked._client, "delete", new=AsyncMock(side_effect=_delete))

        await dav_mocked.move("src", "dst")

        assert state == {"dst": b"source"}
        assert move_count == 3
        assert delete_attempts == (1 if delete_after_mutation else 2)

    async def test_move_directory_source_is_rejected_before_request(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/src", name="src", is_dir=True)),
        )
        move_mock = mocker.patch.object(dav_mocked._client, "move", AsyncMock())

        with pytest.raises(IsADirectoryError):
            await dav_mocked.move("src", "dst")

        move_mock.assert_not_awaited()


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


class TestMovetreeFallback:
    @pytest.mark.parametrize("status_code", [403, 405, 501])
    async def test_unsupported_recursive_move_falls_back(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        status_code: int,
    ) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/src", name="src", is_dir=True)),
        )
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(side_effect=DavHttpStatusError("MOVE", "http://u/dst", status_code, "unsupported")),
        )
        copytree_mock = mocker.patch.object(dav_mocked, "copytree", new=AsyncMock())
        rmtree_mock = mocker.patch.object(dav_mocked, "rmtree", new=AsyncMock())

        await dav_mocked.movetree("src", "dst")

        copytree_mock.assert_awaited_once_with("src", "dst", overwrite=True)
        rmtree_mock.assert_awaited_once_with("src")

    async def test_move_tree_404_maps_to_filenotfound(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/src", name="src", is_dir=True)),
        )
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(side_effect=DavHttpStatusError("MOVE", "http://u/src", 404, "missing")),
        )

        with pytest.raises(FileNotFoundError):
            await dav_mocked.movetree("src", "dst")


class TestCopytreeFallbackRollback:
    async def test_failure_rolls_back_destination(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        async def walk(_path: PurePosixPath) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
            yield "/src", [FileInfo(path="/src/sub", name="sub", is_dir=True)], []

        mocker.patch.object(dav_mocked, "walk", new=walk)
        mocker.patch.object(
            dav_mocked,
            "mkdir",
            new=AsyncMock(side_effect=[None, OSError("injected mkdir failure")]),
        )
        rmtree_mock = mocker.patch.object(dav_mocked, "rmtree", new=AsyncMock())

        with pytest.raises(OSError, match="Failed to copy tree"):
            await dav_mocked._copytree_fallback(PurePosixPath("/src"), PurePosixPath("/dst"), overwrite=True)

        rmtree_mock.assert_awaited_once_with(PurePosixPath("/dst"))
