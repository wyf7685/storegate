"""Protocol integration tests for the aioftp-backed FTP server."""

from collections.abc import AsyncIterator

import aioftp
import pytest

from app.server.ftp import FTPServer
from app.storage.memory import MemoryStorage


@pytest.fixture
async def ftp_endpoint() -> AsyncIterator[tuple[str, int, MemoryStorage]]:
    storage = MemoryStorage("/")
    server = FTPServer(storage, host="127.0.0.1", port=0)

    async with storage:
        await server.server.start(server.host, server.port)
        try:
            yield server.host, server.server.server_port, storage
        finally:
            await server.server.close()


async def _upload(client: aioftp.Client, path: str, content: bytes) -> None:
    async with client.upload_stream(path) as stream:
        await stream.write(content)


async def _download(client: aioftp.Client, path: str) -> bytes:
    content = bytearray()
    async with client.download_stream(path) as stream:
        async for chunk in stream.iter_by_block():
            content.extend(chunk)
    return bytes(content)


async def test_upload_and_download_round_trip(ftp_endpoint: tuple[str, int, MemoryStorage]) -> None:
    host, port, _ = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await _upload(client, "/hello.txt", b"hello, FTP")
        assert await _download(client, "/hello.txt") == b"hello, FTP"


async def test_rmd_removes_empty_directory(ftp_endpoint: tuple[str, int, MemoryStorage]) -> None:
    host, port, storage = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await client.make_directory("/empty")
        await client.remove_directory("/empty")

    assert not await storage.exists("/empty")


async def test_rmd_rejects_nonempty_directory_without_deleting_contents(
    ftp_endpoint: tuple[str, int, MemoryStorage],
) -> None:
    host, port, storage = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await client.make_directory("/nonempty")
        await _upload(client, "/nonempty/sentinel.txt", b"keep me")
        code, _ = await client.command("RMD /nonempty", expected_codes="4xx")

        assert code.matches("4xx")
        assert await _download(client, "/nonempty/sentinel.txt") == b"keep me"

    assert await storage.exists("/nonempty/sentinel.txt")


@pytest.mark.parametrize("path", ["/", "."])
async def test_rmd_rejects_root_directory_without_deleting_contents(
    ftp_endpoint: tuple[str, int, MemoryStorage], path: str
) -> None:
    host, port, storage = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        await _upload(client, "/sentinel.txt", b"keep me")
        code, _ = await client.command(f"RMD {path}", expected_codes="4xx")

        assert code.matches("4xx")
        assert await _download(client, "/sentinel.txt") == b"keep me"

    assert await storage.exists("/sentinel.txt")


async def test_appe_and_rest_are_not_implemented(ftp_endpoint: tuple[str, int, MemoryStorage]) -> None:
    host, port, _ = ftp_endpoint

    async with aioftp.Client.context(host, port) as client:
        appe_code, _ = await client.command("APPE /append.txt", expected_codes="502")
        rest_code, _ = await client.command("REST 1", expected_codes="502")

        assert str(appe_code) == "502"
        assert str(rest_code) == "502"

        await _upload(client, "/after-rest.txt", b"written normally")
        assert await _download(client, "/after-rest.txt") == b"written normally"
