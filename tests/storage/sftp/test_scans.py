from __future__ import annotations

import stat
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from typing import cast

import asyncssh
import pytest

from storegate.storage import EntryKind, FileInfo, UnsupportedOperationError
from storegate.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config


class FakeScanClient:
    def __init__(self, entries: list[asyncssh.SFTPName], fallback: dict[str, asyncssh.SFTPAttrs]) -> None:
        self.entries = entries
        self.fallback = fallback
        self.lstat_calls: list[str] = []
        self.readlink_calls: list[str] = []

    async def lstat(self, path: str) -> asyncssh.SFTPAttrs:
        self.lstat_calls.append(path)
        return self.fallback[path]

    async def readlink(self, path: str) -> str:
        self.readlink_calls.append(path)
        raise AssertionError("listing must not read symlink targets")

    def scandir(self, path: str) -> AsyncIterator[asyncssh.SFTPName]:
        del path

        async def iterate() -> AsyncIterator[asyncssh.SFTPName]:
            for entry in self.entries:
                yield entry

        return iterate()


def directory_info(path: PurePosixPath) -> FileInfo:
    return FileInfo(path=path.as_posix(), name=path.name, kind=EntryKind.DIRECTORY)


@pytest.mark.anyio
async def test_discovery_filters_dots_falls_back_only_for_incomplete_attrs_and_skips_special(
    sftp_server: SFTPServerInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    root = PurePosixPath("/scan")
    unknown = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_UNKNOWN)
    entries = [
        asyncssh.SFTPName(".", attrs=unknown),
        asyncssh.SFTPName("..", attrs=unknown),
        asyncssh.SFTPName("link", attrs=asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SYMLINK, size=4)),
        asyncssh.SFTPName("file", attrs=unknown),
        asyncssh.SFTPName("fifo", attrs=asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SPECIAL)),
    ]
    client = FakeScanClient(
        entries,
        {"/storage/scan/file": asyncssh.SFTPAttrs(permissions=stat.S_IFREG | 0o600, size=3)},
    )

    async def fake_lstat_info(client_arg: object, path: PurePosixPath) -> FileInfo:
        del client_arg
        return directory_info(path)

    monkeypatch.setattr(storage, "_lstat_info", fake_lstat_info)
    discovered = await storage._discovery_scan(cast("asyncssh.SFTPClient", client), root)

    assert [(entry.name, entry.kind) for entry in discovered] == [
        ("file", EntryKind.FILE),
        ("link", EntryKind.SYMLINK),
    ]
    assert client.lstat_calls == ["/storage/scan/file"]
    assert client.readlink_calls == []


@pytest.mark.anyio
async def test_strict_scan_rejects_special_entry(
    sftp_server: SFTPServerInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    root = PurePosixPath("/scan")
    client = FakeScanClient(
        [asyncssh.SFTPName("fifo", attrs=asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SPECIAL))],
        {},
    )

    async def fake_lstat_info(client_arg: object, path: PurePosixPath) -> FileInfo:
        del client_arg
        return directory_info(path)

    monkeypatch.setattr(storage, "_lstat_info", fake_lstat_info)
    with pytest.raises(UnsupportedOperationError):
        await storage._scan_raw(cast("asyncssh.SFTPClient", client), root, strict=True)


@pytest.mark.anyio
async def test_emptiness_scan_counts_special_without_classification(
    sftp_server: SFTPServerInfo,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    root = PurePosixPath("/scan")
    unknown = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_UNKNOWN)
    client = FakeScanClient(
        [
            asyncssh.SFTPName(".", attrs=unknown),
            asyncssh.SFTPName("..", attrs=unknown),
            asyncssh.SFTPName("socket", attrs=unknown),
        ],
        {},
    )

    async def fake_lstat_info(client_arg: object, path: PurePosixPath) -> FileInfo:
        del client_arg
        return directory_info(path)

    monkeypatch.setattr(storage, "_lstat_info", fake_lstat_info)
    assert await storage._directory_has_raw_child(cast("asyncssh.SFTPClient", client), root)
    assert client.lstat_calls == []
