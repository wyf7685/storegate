"""DavStorage error-path unit tests with a mocked client (no network)."""

from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from storegate.storage.abstract import EntryKind, FileInfo, UnsupportedOperationError, WalkEntry
from storegate.storage.dav import DavStorage
from storegate.storage.dav.client import DavResource
from storegate.storage.dav.client.errors import DavHttpStatusError
from storegate.utils import flatten_exception_group


@pytest.fixture
def dav_mocked(mocker: MockerFixture) -> DavStorage:
    from storegate.storage.dav import DavConfig

    cfg = DavConfig(base_url="http://localhost/dav", auth_mode="anonymous")
    s = DavStorage(cfg)
    mocker.patch.object(s, "connect", AsyncMock())
    mocker.patch.object(s, "close", AsyncMock())
    s._client = MagicMock()
    return s


async def _stream(*chunks: bytes) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


def _resource(href: str, *resource_types: str) -> DavResource:
    return DavResource(
        href=href,
        resource_types=resource_types,
        content_length=None,
        last_modified=None,
        creation_date=None,
        display_name=None,
    )


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


class TestUnsupportedResources:
    async def test_capabilities_and_symlink_primitives_are_unsupported(self, dav_mocked: DavStorage) -> None:
        assert dav_mocked.capabilities.symlink_metadata is False
        assert dav_mocked.capabilities.readlink is False
        assert dav_mocked.capabilities.symlink_create is False

        with pytest.raises(UnsupportedOperationError):
            await dav_mocked.readlink("link")
        with pytest.raises(UnsupportedOperationError):
            await dav_mocked.symlink("target", "link")

    async def test_direct_stat_rejects_nonstandard_resource_type(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            new=AsyncMock(return_value=[_resource("/dav/link", "{urn:example:links}symlink")]),
        )

        with pytest.raises(UnsupportedOperationError):
            await dav_mocked.stat("link")

    async def test_discovery_skips_nonstandard_resource_type(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            new=AsyncMock(
                return_value=[
                    _resource("/dav/", "{DAV:}collection"),
                    _resource("/dav/file.txt"),
                    _resource("/dav/link", "{urn:example:links}symlink"),
                ]
            ),
        )

        entries = [entry async for entry in dav_mocked.iterdir("/")]
        assert [(entry.path, entry.kind) for entry in entries] == [("/file.txt", EntryKind.FILE)]

    async def test_walk_returns_structured_discovery_snapshot(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            new=AsyncMock(
                return_value=[
                    _resource("/dav/", "{DAV:}collection"),
                    _resource("/dav/z.txt"),
                    _resource("/dav/a.txt"),
                    _resource("/dav/link", "{urn:example:links}symlink"),
                ]
            ),
        )

        walked = [entry async for entry in dav_mocked.walk("/")]
        assert walked == [
            WalkEntry(
                path="/",
                entries=(
                    FileInfo(path="/a.txt", name="a.txt", kind=EntryKind.FILE),
                    FileInfo(path="/z.txt", name="z.txt", kind=EntryKind.FILE),
                ),
            )
        ]

    async def test_rmdir_counts_hidden_special_resource_as_nonempty(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        directory = FileInfo(path="/dir", name="dir", kind=EntryKind.DIRECTORY)
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(return_value=directory))
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            new=AsyncMock(
                return_value=[
                    _resource("/dav/dir/", "{DAV:}collection"),
                    _resource("/dav/dir/link", "{urn:example:links}symlink"),
                ]
            ),
        )
        delete_mock = mocker.patch.object(dav_mocked._client, "delete", new=AsyncMock())

        with pytest.raises(OSError, match="Directory not empty"):
            await dav_mocked.rmdir("dir")

        delete_mock.assert_not_awaited()

    async def test_strict_snapshot_rejects_special_before_tree_mutation(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        directory = FileInfo(path="/src", name="src", kind=EntryKind.DIRECTORY)
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(return_value=directory))
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            new=AsyncMock(
                return_value=[
                    _resource("/dav/src/", "{DAV:}collection"),
                    _resource("/dav/src/link", "{urn:example:links}symlink"),
                ]
            ),
        )
        copy_mock = mocker.patch.object(dav_mocked._client, "copy", new=AsyncMock())

        with pytest.raises(UnsupportedOperationError):
            await dav_mocked.copytree("src", "dst")

        copy_mock.assert_not_awaited()


class TestUploadStatGateway:
    async def test_upload_to_directory_raises(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/d", name="d", kind=EntryKind.DIRECTORY)),
        )
        put_mock = mocker.patch.object(dav_mocked._client, "put", AsyncMock())
        with pytest.raises(IsADirectoryError):
            await dav_mocked.upload_stream(_stream(b"data"), "d")
        put_mock.assert_not_called()

    async def test_upload_no_overwrite_raises(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        mocker.patch.object(
            dav_mocked,
            "stat",
            new=AsyncMock(return_value=FileInfo(path="/f", name="f", kind=EntryKind.FILE, size=3)),
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
            new=AsyncMock(return_value=FileInfo(path="/d", name="d", kind=EntryKind.DIRECTORY)),
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
            new=AsyncMock(return_value=FileInfo(path="/f", name="f", kind=EntryKind.FILE, size=1)),
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
            new=AsyncMock(
                side_effect=[FileInfo(path="/src", name="src", kind=EntryKind.FILE), FileNotFoundError("dst")]
            ),
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
            new=AsyncMock(
                side_effect=[FileInfo(path="/src", name="src", kind=EntryKind.FILE), FileNotFoundError("dst")]
            ),
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
            new=AsyncMock(
                side_effect=[FileInfo(path="/src", name="src", kind=EntryKind.FILE), FileNotFoundError("dst")]
            ),
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
            new=AsyncMock(
                side_effect=[FileInfo(path="/src", name="src", kind=EntryKind.FILE), FileNotFoundError("dst")]
            ),
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
            return FileInfo(path=f"/{path}", name=path, kind=EntryKind.FILE)

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
            return FileInfo(path=f"/{path}", name=path, kind=EntryKind.FILE)

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
            new=AsyncMock(return_value=FileInfo(path="/src", name="src", kind=EntryKind.DIRECTORY)),
        )
        move_mock = mocker.patch.object(dav_mocked._client, "move", AsyncMock())

        with pytest.raises(IsADirectoryError):
            await dav_mocked.move("src", "dst")

        move_mock.assert_not_awaited()


class TestCopytreeFallback:
    async def test_server_copy_501_triggers_fallback(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        src_info = FileInfo(path="/src", name="src", kind=EntryKind.DIRECTORY)
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(return_value=src_info))
        mocker.patch.object(dav_mocked, "exists", new=AsyncMock(return_value=False))
        snapshot = (WalkEntry(path="/src", entries=()),)
        mocker.patch.object(dav_mocked, "_strict_walk_snapshot", new=AsyncMock(return_value=snapshot))
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
        src_info = FileInfo(path="/src", name="src", kind=EntryKind.DIRECTORY)
        mocker.patch.object(dav_mocked, "stat", new=AsyncMock(return_value=src_info))
        mocker.patch.object(dav_mocked, "exists", new=AsyncMock(return_value=False))
        mocker.patch.object(
            dav_mocked,
            "_strict_walk_snapshot",
            new=AsyncMock(return_value=(WalkEntry(path="/src", entries=()),)),
        )
        mocker.patch.object(dav_mocked._client, "copy", AsyncMock())
        fallback_mock = mocker.patch.object(dav_mocked, "_copytree_fallback", AsyncMock())
        await dav_mocked.copytree("src", "dst")
        fallback_mock.assert_not_awaited()

    async def test_special_snapshot_fails_before_destination_mutation(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        snapshot = (
            WalkEntry(
                path="/src",
                entries=(FileInfo(path="/src/link", name="link", kind=EntryKind.SYMLINK),),
            ),
        )
        exists_mock = mocker.patch.object(dav_mocked, "exists", new=AsyncMock())
        mkdir_mock = mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())

        with pytest.raises(UnsupportedOperationError):
            await dav_mocked._copytree_fallback(
                PurePosixPath("/src"), PurePosixPath("/dst"), overwrite=True, snapshot=snapshot
            )

        exists_mock.assert_not_awaited()
        mkdir_mock.assert_not_awaited()

    async def test_fallback_creates_directories_before_copying_sibling_files(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        snapshot = (
            WalkEntry(
                path="/src",
                entries=(
                    FileInfo(path="/src/a.txt", name="a.txt", kind=EntryKind.FILE),
                    FileInfo(path="/src/z", name="z", kind=EntryKind.DIRECTORY),
                ),
            ),
        )
        events: list[str] = []

        async def exists(path: str | PurePosixPath) -> bool:
            events.append(f"exists:{path}")
            return str(path) == "/dst"

        async def mkdir(path: str, *, parents: bool = False, exist_ok: bool = False) -> None:
            _ = parents, exist_ok
            events.append(f"mkdir:{path}")

        async def stat(path: str | PurePosixPath) -> FileInfo:
            events.append(f"stat:{path}")
            raise FileNotFoundError(str(path))

        async def copy(source: str, destination: str, *, overwrite: bool = True) -> None:
            _ = overwrite
            events.append(f"copy:{source}->{destination}")

        mocker.patch.object(dav_mocked, "exists", new=exists)
        mocker.patch.object(dav_mocked, "mkdir", new=mkdir)
        mocker.patch.object(dav_mocked, "stat", new=stat)
        mocker.patch.object(dav_mocked, "copy", new=copy)

        await dav_mocked._copytree_fallback(
            PurePosixPath("/src"), PurePosixPath("/dst"), overwrite=True, snapshot=snapshot
        )

        assert events.index("mkdir:/dst/z") < events.index("copy:/src/a.txt->/dst/a.txt")


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
            new=AsyncMock(return_value=FileInfo(path="/src", name="src", kind=EntryKind.DIRECTORY)),
        )
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(
            dav_mocked,
            "_strict_walk_snapshot",
            new=AsyncMock(return_value=(WalkEntry(path="/src", entries=()),)),
        )
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
            new=AsyncMock(return_value=FileInfo(path="/src", name="src", kind=EntryKind.DIRECTORY)),
        )
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(
            dav_mocked,
            "_strict_walk_snapshot",
            new=AsyncMock(return_value=(WalkEntry(path="/src", entries=()),)),
        )
        mocker.patch.object(
            dav_mocked._client,
            "move",
            AsyncMock(side_effect=DavHttpStatusError("MOVE", "http://u/src", 404, "missing")),
        )

        with pytest.raises(FileNotFoundError):
            await dav_mocked.movetree("src", "dst")


class TestCopytreeFallbackRollback:
    async def test_failure_restores_overwritten_destination(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        state = {"src/a.txt": b"new", "dst/a.txt": b"old", "dst/keep.txt": b"keep"}

        snapshot = (
            WalkEntry(
                path="/src",
                entries=(FileInfo(path="/src/a.txt", name="a.txt", kind=EntryKind.FILE),),
            ),
            WalkEntry(
                path="/src/sub",
                entries=(FileInfo(path="/src/sub/b.txt", name="b.txt", kind=EntryKind.FILE),),
            ),
        )

        async def stat(path: str | PurePosixPath) -> FileInfo:
            key = str(path).lstrip("/")
            if key not in state:
                raise FileNotFoundError(key)
            return FileInfo(path=f"/{key}", name=PurePosixPath(key).name, kind=EntryKind.FILE)

        async def exists(path: str | PurePosixPath) -> bool:
            return str(path).lstrip("/") in state or str(path).rstrip("/") in {"/dst", "dst"}

        async def mkdir(_path: str, *, parents: bool = False, exist_ok: bool = False) -> None:
            _ = parents, exist_ok

        async def copy(source: str, destination: str, *, overwrite: bool = True) -> None:
            _ = overwrite
            source_key = source.lstrip("/")
            destination_key = destination.lstrip("/")
            if source_key.endswith("b.txt"):
                raise OSError("injected second copy failure")
            state[destination_key] = state[source_key]

        async def unlink(path: PurePosixPath, *, missing_ok: bool = False) -> None:
            _ = missing_ok
            state.pop(path.as_posix().lstrip("/"), None)

        async def move(source: str, destination: str, *, overwrite: bool = True) -> None:
            _ = overwrite
            state[destination] = state.pop(source)

        mocker.patch.object(dav_mocked, "stat", new=stat)
        mocker.patch.object(dav_mocked, "exists", new=exists)
        mocker.patch.object(dav_mocked, "mkdir", new=mkdir)
        mocker.patch.object(dav_mocked, "copy", new=copy)
        mocker.patch.object(dav_mocked, "unlink", new=unlink)
        mocker.patch.object(dav_mocked._client, "move", new=AsyncMock(side_effect=move))

        with pytest.raises(OSError, match="Failed to copy tree"):
            await dav_mocked._copytree_fallback(
                PurePosixPath("/src"), PurePosixPath("/dst"), overwrite=True, snapshot=snapshot
            )

        assert state["dst/a.txt"] == b"old"
        assert state["dst/keep.txt"] == b"keep"

    async def test_restore_failure_is_grouped(self, dav_mocked: DavStorage, mocker: MockerFixture) -> None:
        snapshot = (
            WalkEntry(
                path="/src",
                entries=(FileInfo(path="/src/a.txt", name="a.txt", kind=EntryKind.FILE),),
            ),
            WalkEntry(
                path="/src/sub",
                entries=(FileInfo(path="/src/sub/b.txt", name="b.txt", kind=EntryKind.FILE),),
            ),
        )

        async def stat(path: str | PurePosixPath) -> FileInfo:
            if str(path).endswith("a.txt"):
                return FileInfo(path="/dst/a.txt", name="a.txt", kind=EntryKind.FILE)
            raise FileNotFoundError(str(path))

        mocker.patch.object(dav_mocked, "stat", new=stat)
        mocker.patch.object(dav_mocked, "exists", new=AsyncMock(return_value=True))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(dav_mocked, "copy", new=AsyncMock(side_effect=[None, OSError("copy failed")]))
        mocker.patch.object(dav_mocked, "unlink", new=AsyncMock())
        mocker.patch.object(dav_mocked._client, "move", new=AsyncMock(side_effect=[None, OSError("restore failed")]))

        with pytest.raises(BaseExceptionGroup) as caught:
            await dav_mocked._copytree_fallback(
                PurePosixPath("/src"), PurePosixPath("/dst"), overwrite=True, snapshot=snapshot
            )
        flattened = list(flatten_exception_group(caught.value))
        assert "copy failed" in str(flattened[0])
        assert "restore failed" in str(flattened[1])

    async def test_public_copytree_restore_failure_preserves_group(
        self, dav_mocked: DavStorage, mocker: MockerFixture
    ) -> None:
        snapshot = (
            WalkEntry(
                path="/src",
                entries=(FileInfo(path="/src/a.txt", name="a.txt", kind=EntryKind.FILE),),
            ),
            WalkEntry(
                path="/src/sub",
                entries=(FileInfo(path="/src/sub/b.txt", name="b.txt", kind=EntryKind.FILE),),
            ),
        )

        async def stat(path: str | PurePosixPath) -> FileInfo:
            text = str(path)
            if text in {"/src", "src"}:
                return FileInfo(path="/src", name="src", kind=EntryKind.DIRECTORY)
            if text.endswith("a.txt"):
                return FileInfo(path="/dst/a.txt", name="a.txt", kind=EntryKind.FILE)
            raise FileNotFoundError(text)

        mocker.patch.object(dav_mocked, "stat", new=stat)
        mocker.patch.object(dav_mocked, "exists", new=AsyncMock(return_value=True))
        mocker.patch.object(dav_mocked, "mkdir", new=AsyncMock())
        mocker.patch.object(dav_mocked, "copy", new=AsyncMock(side_effect=[None, OSError("copy failed")]))
        mocker.patch.object(dav_mocked, "unlink", new=AsyncMock())
        mocker.patch.object(
            dav_mocked,
            "_strict_walk_snapshot",
            new=AsyncMock(return_value=snapshot),
        )
        mocker.patch.object(
            dav_mocked._client,
            "copy",
            new=AsyncMock(side_effect=DavHttpStatusError("COPY", "http://u/src", 501, "not implemented")),
        )
        mocker.patch.object(
            dav_mocked._client,
            "move",
            new=AsyncMock(side_effect=[None, OSError("restore failed")]),
        )

        with pytest.raises(BaseExceptionGroup) as caught:
            await dav_mocked.copytree("/src", "/dst", overwrite=True)
        flattened = list(flatten_exception_group(caught.value))
        assert len(flattened) == 2
        assert "copy failed" in str(flattened[0])
        assert "restore failed" in str(flattened[1])


class TestPathEscapeRejection:
    """Public storage ops reject escape/NUL paths before any client request."""

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_stat_rejects_escape_before_propfind(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        path: str,
    ) -> None:
        propfind = mocker.patch.object(dav_mocked._client, "propfind", AsyncMock())
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await dav_mocked.stat(path)
        propfind.assert_not_awaited()

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_download_rejects_escape_before_get(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        path: str,
    ) -> None:
        stream_get = mocker.patch.object(dav_mocked._client, "stream_get")
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            async for _ in dav_mocked.download_stream(path):
                pass
        stream_get.assert_not_called()

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_upload_rejects_escape_before_put(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        path: str,
    ) -> None:
        put = mocker.patch.object(dav_mocked._client, "put", AsyncMock())
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await dav_mocked.upload_bytes(b"data", path)
        put.assert_not_awaited()

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_unlink_rejects_escape_before_delete(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        path: str,
    ) -> None:
        delete = mocker.patch.object(dav_mocked._client, "delete", AsyncMock())
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await dav_mocked.unlink(path)
        delete.assert_not_awaited()

    @pytest.mark.parametrize(
        ("src", "dst"),
        [
            ("../x", "safe"),
            ("safe", "a/../../x"),
            ("x/../y", "z"),
            ("safe", "bad\x00name"),
        ],
    )
    async def test_copy_rejects_escape_before_request(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        src: str,
        dst: str,
    ) -> None:
        propfind = mocker.patch.object(dav_mocked._client, "propfind", AsyncMock())
        copy = mocker.patch.object(dav_mocked._client, "copy", AsyncMock())
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await dav_mocked.copy(src, dst)
        propfind.assert_not_awaited()
        copy.assert_not_awaited()

    @pytest.mark.parametrize(
        ("src", "dst"),
        [
            ("../x", "safe"),
            ("safe", "a/../../x"),
            ("x/../y", "z"),
            ("safe", "bad\x00name"),
        ],
    )
    async def test_move_rejects_escape_before_request(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        src: str,
        dst: str,
    ) -> None:
        propfind = mocker.patch.object(dav_mocked._client, "propfind", AsyncMock())
        move = mocker.patch.object(dav_mocked._client, "move", AsyncMock())
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await dav_mocked.move(src, dst)
        propfind.assert_not_awaited()
        move.assert_not_awaited()

    @pytest.mark.parametrize(
        "invalid_path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    @pytest.mark.parametrize("invalid_side", ["source", "destination"])
    async def test_movetree_rejects_escape_before_request(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
        invalid_path: str,
        invalid_side: str,
    ) -> None:
        src, dst = (invalid_path, "safe") if invalid_side == "source" else ("safe", invalid_path)
        propfind = mocker.patch.object(dav_mocked._client, "propfind", AsyncMock())
        move = mocker.patch.object(dav_mocked._client, "move", AsyncMock())
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await dav_mocked.movetree(src, dst)
        propfind.assert_not_awaited()
        move.assert_not_awaited()

    async def test_allows_hidden_and_double_dot_names(
        self,
        dav_mocked: DavStorage,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            AsyncMock(
                return_value=[
                    _resource("/dav/.hidden", "{DAV:}collection"),
                ]
            ),
        )
        info = await dav_mocked.stat(".hidden")
        assert info.path == "/.hidden"

        mocker.patch.object(
            dav_mocked._client,
            "propfind",
            AsyncMock(
                return_value=[
                    _resource("/dav/a..b"),
                ]
            ),
        )
        info = await dav_mocked.stat("a..b")
        assert info.path == "/a..b"
