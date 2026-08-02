from __future__ import annotations

import errno
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import PurePosixPath
from typing import cast

import asyncssh
import pytest

from storegate.storage import EntryKind, FileInfo
from storegate.storage.sftp import SFTPStorage
from storegate.storage.sftp.pool import SFTPChannelPool
from storegate.storage.sftp.storage import translate_sftp_error
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (asyncssh.SFTPNoSuchFile("missing"), FileNotFoundError),
        (asyncssh.SFTPPermissionDenied("denied"), PermissionError),
        (asyncssh.SFTPFileAlreadyExists("exists"), FileExistsError),
        (asyncssh.SFTPFileIsADirectory("directory"), IsADirectoryError),
        (asyncssh.SFTPNotADirectory("file"), NotADirectoryError),
        (asyncssh.SFTPFailure("failure"), OSError),
    ],
)
def test_error_translation(source: BaseException, expected: type[BaseException]) -> None:
    translated = translate_sftp_error(source, "operation failed")
    assert isinstance(translated, expected)


def test_business_os_error_is_preserved() -> None:
    source = FileNotFoundError("business error")
    assert translate_sftp_error(source, "ignored") is source


class FakeResolverClient:
    def __init__(self, attrs: dict[str, asyncssh.SFTPAttrs], targets: dict[str, str]) -> None:
        self.attrs = attrs
        self.targets = targets

    async def lstat(self, path: str) -> asyncssh.SFTPAttrs:
        try:
            return self.attrs[path]
        except KeyError:
            raise asyncssh.SFTPNoSuchFile("missing") from None

    async def readlink(self, path: str) -> str:
        return self.targets[path]


@pytest.mark.anyio
async def test_target_resolver_follows_chain_and_enforces_segment_containment(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    directory = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_DIRECTORY)
    symlink = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SYMLINK)
    regular = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_REGULAR, size=4)
    client = FakeResolverClient(
        {
            "/storage": directory,
            "/storage/first": symlink,
            "/storage/second": symlink,
            "/storage/file": regular,
        },
        {"/storage/first": "second", "/storage/second": "file"},
    )

    resolved, attrs = await storage._resolve_remote_target(
        cast("asyncssh.SFTPClient", client),
        PurePosixPath("/storage/first"),
    )
    assert resolved == PurePosixPath("/storage/file")
    assert attrs is regular
    with pytest.raises(PermissionError) as prefix_collision:
        storage._require_remote_containment(PurePosixPath("/storage-other/file"))
    assert prefix_collision.value.errno == errno.EACCES


@pytest.mark.anyio
async def test_target_resolver_reports_cycle(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    client = FakeResolverClient(
        {
            "/storage": asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_DIRECTORY),
            "/storage/self": asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SYMLINK),
        },
        {"/storage/self": "self"},
    )
    with pytest.raises(OSError, match="Too many symbolic link levels") as cycle:
        await storage._resolve_remote_target(cast("asyncssh.SFTPClient", client), PurePosixPath("/storage/self"))
    assert cycle.value.errno == errno.ELOOP


class FailingSymlinkClient:
    def __init__(self) -> None:
        self.calls = 0

    async def symlink(self, target: str, path: str) -> None:
        del target, path
        self.calls += 1
        raise asyncssh.SFTPConnectionLost("connection dropped")


class FakeLease:
    def __init__(self, client: FailingSymlinkClient) -> None:
        self.client = client
        self.transport_invalid = False

    def invalidate_transport(self) -> None:
        self.transport_invalid = True

    def invalidate(self) -> None:
        pass


class FakePool:
    def __init__(self, lease: FakeLease) -> None:
        self.lease = lease

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[FakeLease]:
        yield self.lease


@pytest.mark.anyio
async def test_symlink_connection_failure_is_not_replayed(
    sftp_server: SFTPServerInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    client = FailingSymlinkClient()
    lease = FakeLease(client)
    storage._pool = cast("SFTPChannelPool", FakePool(lease))

    async def fake_lstat_info(client_arg: object, path: PurePosixPath) -> FileInfo:
        del client_arg
        return FileInfo(path=path.as_posix(), name=path.name, kind=EntryKind.DIRECTORY)

    async def fake_lstat_or_none(client_arg: object, path: PurePosixPath) -> FileInfo | None:
        del client_arg, path
        return None

    monkeypatch.setattr(storage, "_lstat_info", fake_lstat_info)
    monkeypatch.setattr(storage, "_lstat_or_none", fake_lstat_or_none)
    with pytest.raises(OSError, match="connection dropped"):
        await storage.symlink("target", "/link")
    assert client.calls == 1
    assert lease.transport_invalid
