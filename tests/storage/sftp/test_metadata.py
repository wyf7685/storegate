import stat

import asyncssh
import pytest

from storegate.storage import EntryKind, UnsupportedOperationError
from storegate.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config


@pytest.mark.integration
async def test_root_metadata(sftp_server: SFTPServerInfo) -> None:
    async with SFTPStorage(make_config(sftp_server)) as storage:
        info = await storage.stat("/")
        assert info.path == "/"
        assert info.name == ""
        assert info.kind is EntryKind.DIRECTORY
        assert info.size == 0


def test_capabilities_are_all_true_and_reused(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    capabilities = storage.capabilities
    assert capabilities.symlink_metadata
    assert capabilities.readlink
    assert capabilities.symlink_create
    assert storage.capabilities is capabilities


def test_classifies_symlink_attrs(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    attrs = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SYMLINK, size=7)
    info = storage._file_info_from_attrs(storage.normalize_path("/link"), attrs)
    assert info.kind is EntryKind.SYMLINK
    assert info.size == 7


def test_classifies_permissions_when_type_is_incomplete(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    attrs = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_UNKNOWN, permissions=stat.S_IFREG | 0o600)
    info = storage._file_info_from_attrs(storage.normalize_path("/file"), attrs)
    assert info.kind is EntryKind.FILE


def test_rejects_special_attrs(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    attrs = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SPECIAL, permissions=stat.S_IFIFO | 0o600)
    with pytest.raises(UnsupportedOperationError):
        storage._file_info_from_attrs(storage.normalize_path("/fifo"), attrs)
