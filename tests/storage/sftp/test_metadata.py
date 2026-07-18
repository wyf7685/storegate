import asyncssh
import pytest

from app.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config


@pytest.mark.integration
async def test_root_metadata(sftp_server: SFTPServerInfo) -> None:
    async with SFTPStorage(make_config(sftp_server)) as storage:
        info = await storage.stat("/")
        assert info.path == "/"
        assert info.name == ""
        assert info.is_dir
        assert info.size == 0


def test_rejects_symlink_attrs(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    attrs = asyncssh.SFTPAttrs(type=asyncssh.FILEXFER_TYPE_SYMLINK)
    with pytest.raises(OSError, match="Symbolic links"):
        storage._file_info_from_attrs(storage.normalize_path("/link"), attrs)
