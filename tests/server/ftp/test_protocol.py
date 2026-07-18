"""Protocol integration tests for the aioftp-backed FTP server."""

from pathlib import PurePosixPath

import aioftp
import pytest

from app.server.ftp.handle import ReadHandle, WriteHandle
from app.server.ftp.pathio import _file_info_to_stat
from app.storage import AbstractStorage, EntryKind, FileInfo

pytestmark = pytest.mark.integration


async def _upload(client: aioftp.Client, path: str, content: bytes) -> None:
    async with client.upload_stream(path) as stream:
        await stream.write(content)


async def _download(client: aioftp.Client, path: str) -> bytes:
    content = bytearray()
    async with client.download_stream(path) as stream:
        async for chunk in stream.iter_by_block():
            content.extend(chunk)
    return bytes(content)


async def test_upload_and_download_round_trip(ftp_endpoint: tuple[str, int]) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await _upload(client, "/hello.txt", b"hello, FTP")
        assert await _download(client, "/hello.txt") == b"hello, FTP"


async def test_rmd_removes_empty_directory(ftp_endpoint: tuple[str, int]) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await client.make_directory("/empty")
        await client.remove_directory("/empty")

    async with aioftp.Client.context(host, port) as client:
        with pytest.raises(aioftp.StatusCodeError):
            await client.stat("/empty")


async def test_rmd_rejects_nonempty_directory_without_deleting_contents(
    ftp_endpoint: tuple[str, int],
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await client.make_directory("/nonempty")
        await _upload(client, "/nonempty/sentinel.txt", b"keep me")
        code, _ = await client.command("RMD /nonempty", expected_codes="4xx")

        assert code.matches("4xx")
        assert await _download(client, "/nonempty/sentinel.txt") == b"keep me"


@pytest.mark.parametrize("path", ["/", "."])
async def test_rmd_rejects_root_directory_without_deleting_contents(ftp_endpoint: tuple[str, int], path: str) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await _upload(client, "/sentinel.txt", b"keep me")
        code, _ = await client.command(f"RMD {path}", expected_codes="4xx")

        assert code.matches("4xx")
        assert await _download(client, "/sentinel.txt") == b"keep me"


async def test_appe_is_not_implemented(ftp_endpoint: tuple[str, int]) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        code, _ = await client.command("APPE /append.txt", expected_codes="502")

        assert str(code) == "502"


async def test_rest_retr_downloads_from_offset(ftp_endpoint: tuple[str, int]) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await _upload(client, "/source.txt", b"0123456789")

        content = bytearray()
        async with client.download_stream("/source.txt", offset=4) as stream:
            async for chunk in stream.iter_by_block():
                content.extend(chunk)

        assert content == b"456789"


async def test_rest_stor_is_rejected_without_modifying_file(
    ftp_endpoint: tuple[str, int],
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await _upload(client, "/target.txt", b"original")

        with pytest.raises(aioftp.StatusCodeError) as exc_info:
            async with client.upload_stream("/target.txt", offset=4):
                pass

        assert exc_info.value.received_codes[-1].matches("504")
        assert await _download(client, "/target.txt") == b"original"

        await _upload(client, "/after-rejection.txt", b"written normally")
        assert await _download(client, "/after-rejection.txt") == b"written normally"


@pytest.mark.parametrize("marker", ["-1", "invalid"])
async def test_rest_rejects_invalid_marker(
    ftp_endpoint: tuple[str, int],
    marker: str,
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        code, _ = await client.command(f"REST {marker}", expected_codes="501")

        assert str(code) == "501"


async def test_rest_zero_allows_regular_stor(ftp_endpoint: tuple[str, int]) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        _, writer = await client.get_passive_connection()
        rest_code, _ = await client.command("REST 0", expected_codes="350")
        stor_code, _ = await client.command("STOR /rest-zero.txt", expected_codes="150")

        writer.write(b"complete replacement")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        complete_code, _ = await client.command(expected_codes="226")

        assert str(rest_code) == "350"
        assert str(stor_code) == "150"
        assert str(complete_code) == "226"
        assert await _download(client, "/rest-zero.txt") == b"complete replacement"


async def _command_with_data_connection(client: aioftp.Client, command: str) -> aioftp.Code:
    _reader, writer = await client.get_passive_connection()
    try:
        code, _info = await client.command(command, expected_codes="550")
        return code
    finally:
        writer.close()
        await writer.wait_closed()


async def test_list_hides_symlinks(ftp_endpoint: tuple[str, int]) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        names = {path.name for path, _info in await client.list("/")}

    assert "target.txt" in names
    assert "target-dir" in names
    assert "file-link" not in names
    assert "directory-link" not in names
    assert "rename-destination-link" not in names


async def test_direct_symlink_transfer_and_rename_commands_return_550_without_modifying_targets(
    ftp_endpoint: tuple[str, int],
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        assert str(await _command_with_data_connection(client, "RETR /file-link")) == "550"
        assert str(await _command_with_data_connection(client, "STOR /file-link")) == "550"

        rnfr_code, _ = await client.command("RNFR /file-link", expected_codes="550")
        assert str(rnfr_code) == "550"

        source_code, _ = await client.command("RNFR /rename-source.txt", expected_codes="350")
        destination_code, _ = await client.command("RNTO /rename-destination-link", expected_codes="550")
        assert str(source_code) == "350"
        assert str(destination_code) == "550"

        assert await _download(client, "/target.txt") == b"original target"
        assert await _download(client, "/rename-source.txt") == b"rename source"


async def test_mkd_hidden_symlink_returns_550_without_modifying_link_or_target(
    ftp_endpoint: tuple[str, int],
    ftp_protocol_server: tuple[str, int, AbstractStorage],
) -> None:
    host, port = ftp_endpoint
    _fixture_host, _fixture_port, storage = ftp_protocol_server

    async with aioftp.Client.context(host, port) as client:
        code, _ = await client.command("MKD /file-link", expected_codes="550")

        assert str(code) == "550"
        assert await _download(client, "/target.txt") == b"original target"
        assert (await storage.lstat("/file-link")).kind is EntryKind.SYMLINK


async def test_stor_preflight_lstat_error_returns_451_and_control_connection_survives(
    ftp_endpoint: tuple[str, int],
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        _reader, writer = await client.get_passive_connection()
        code, _ = await client.command("STOR /stor-lstat-error", expected_codes="451")
        writer.close()
        await writer.wait_closed()

        assert str(code) == "451"
        await _upload(client, "/after-stor-metadata-error.txt", b"still connected")
        assert await _download(client, "/after-stor-metadata-error.txt") == b"still connected"


async def test_rnto_without_fresh_rnfr_is_always_503_and_session_remains_usable(
    ftp_endpoint: tuple[str, int],
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        for destination in ("/ordinary-no-rnfr.txt", "/rename-destination-link", "/rnto-lstat-error"):
            code, _ = await client.command(f"RNTO {destination}", expected_codes="503")
            assert str(code) == "503"

        await _upload(client, "/after-no-rnfr.txt", b"still connected")
        assert await _download(client, "/after-no-rnfr.txt") == b"still connected"


async def test_rnto_preflight_lstat_error_resets_transaction_and_session_remains_usable(
    ftp_endpoint: tuple[str, int],
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        source_code, _ = await client.command("RNFR /rename-source.txt", expected_codes="350")
        error_code, _ = await client.command("RNTO /rnto-lstat-error", expected_codes="451")

        assert str(source_code) == "350"
        assert str(error_code) == "451"
        assert await _download(client, "/rename-source.txt") == b"rename source"

        stale_code, _ = await client.command("RNTO /stale-destination.txt", expected_codes="503")
        fresh_source_code, _ = await client.command("RNFR /rename-source.txt", expected_codes="350")
        success_code, _ = await client.command("RNTO /renamed-after-error.txt", expected_codes="250")

        assert str(stale_code) == "503"
        assert str(fresh_source_code) == "350"
        assert str(success_code) == "250"
        assert await _download(client, "/renamed-after-error.txt") == b"rename source"

        await _upload(client, "/after-rnto-metadata-error.txt", b"still connected")
        assert await _download(client, "/after-rnto-metadata-error.txt") == b"still connected"


async def test_symlink_only_directory_lists_empty_but_rmd_preserves_link_target(
    ftp_endpoint: tuple[str, int],
) -> None:
    host, port = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        assert await client.list("/links-only") == []

        code, _ = await client.command("RMD /links-only", expected_codes="4xx")
        assert code.matches("4xx")
        assert await _download(client, "/target.txt") == b"original target"

        listing = {path.name for path, _info in await client.list("/")}
        assert "links-only" in listing


def test_file_info_to_stat_rejects_symlink() -> None:
    info = FileInfo(path="/hidden", name="hidden", kind=EntryKind.SYMLINK)

    with pytest.raises(FileNotFoundError, match="File unavailable"):
        _file_info_to_stat(info)


async def test_handles_recheck_symlink_kind_before_backend_stream_access(
    ftp_protocol_server: tuple[str, int, AbstractStorage],
) -> None:
    _host, _port, storage = ftp_protocol_server

    read_handle = ReadHandle(storage, PurePosixPath("/file-link"))
    with pytest.raises(FileNotFoundError, match="File unavailable"):
        await read_handle.read(1)

    write_handle = WriteHandle(storage, PurePosixPath("/file-link"))
    with pytest.raises(FileNotFoundError, match="File unavailable"):
        await write_handle.close()
