"""Protocol integration tests for the aioftp-backed FTP server."""

import aioftp
import pytest


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
