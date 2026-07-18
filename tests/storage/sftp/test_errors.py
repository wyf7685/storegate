import asyncssh
import pytest

from app.storage.sftp.storage import translate_sftp_error


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
