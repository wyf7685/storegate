"""WebDAV client path construction and escape rejection tests."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import pytest

from storegate.storage.dav.client import AsyncDavClient
from storegate.storage.dav.client.models import DavConfig
from storegate.utils import httpx

pytestmark = pytest.mark.httpx


def _client(*, root_prefix: str = "/storegate") -> AsyncDavClient:
    return AsyncDavClient(
        DavConfig(
            base_url="https://host/dav",
            auth_mode="anonymous",
            root_prefix=root_prefix,
        )
    )


class TestBuildPath:
    def test_no_prefix(self) -> None:
        client = AsyncDavClient(DavConfig(base_url="https://host/dav", auth_mode="anonymous"))
        assert client._build_path("foo/bar") == "/foo/bar"
        assert client._build_path("/foo/bar") == "/foo/bar"
        assert client._build_path("") == "/"

    def test_with_prefix(self) -> None:
        client = _client()
        assert client._build_path("foo/bar") == "/storegate/foo/bar"
        assert client._build_path("") == "/storegate"

    def test_build_url_absolute(self) -> None:
        client = _client()
        assert client._build_url("foo") == "https://host/dav/storegate/foo"

    def test_allows_hidden_and_double_dot_names(self) -> None:
        client = _client(root_prefix="/root")
        assert client._build_path(".hidden") == "/root/.hidden"
        assert client._build_path("a..b") == "/root/a..b"
        assert client._build_url(".hidden/a..b") == "https://host/dav/root/.hidden/a..b"

    @pytest.mark.parametrize(
        "path",
        [
            "../x",
            "a/../../x",
            "x/../y",
            "../../escape",
            "bad\x00name",
            "a/\x00/b",
        ],
    )
    def test_build_path_rejects_escape_and_nul(self, path: str) -> None:
        client = _client(root_prefix="/root")
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            client._build_path(path)


@asynccontextmanager
async def _client_with_transport(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    root_prefix: str = "/root",
) -> AsyncIterator[tuple[AsyncDavClient, list[httpx.Request]]]:
    requests: list[httpx.Request] = []

    def capturing(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    client = AsyncDavClient(
        DavConfig(
            base_url="https://example.com/base",
            auth_mode="anonymous",
            root_prefix=root_prefix,
            http2=False,
        )
    )
    transport = httpx.MockTransport(capturing)
    async with httpx.AsyncClient(transport=transport, base_url="https://example.com") as raw:
        client._client = raw
        yield client, requests


class TestRequestCaptureNoEscape:
    """Prove invalid paths never emit HTTP requests for GET/PUT/DELETE/COPY/MOVE/PROPFIND."""

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_stream_get_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(200, content=b"ok")) as (
            client,
            requests,
        ):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                async with client.stream_get(path):
                    pass
        assert requests == []

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_put_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(201)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.put(path, b"data")
        assert requests == []

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_delete_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(204)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.delete(path)
        assert requests == []

    @pytest.mark.parametrize(
        ("src", "dst"),
        [
            ("../x", "safe"),
            ("safe", "a/../../x"),
            ("x/../y", "z"),
            ("safe", "bad\x00name"),
        ],
    )
    async def test_copy_rejects_before_request(self, src: str, dst: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(201)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.copy(src, dst)
        assert requests == []

    @pytest.mark.parametrize(
        ("src", "dst"),
        [
            ("../x", "safe"),
            ("safe", "a/../../x"),
            ("x/../y", "z"),
            ("safe", "bad\x00name"),
        ],
    )
    async def test_move_rejects_before_request(self, src: str, dst: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(201)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.move(src, dst)
        assert requests == []

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_propfind_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(207, content=b"<D:multistatus/>")) as (
            client,
            requests,
        ):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.propfind(path)
        assert requests == []

    async def test_valid_paths_still_emit_expected_urls(self) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(204)) as (client, requests):
            await client.delete(".hidden/a..b")
            await client.put("inside/file", b"ok")

        assert [str(req.url) for req in requests] == [
            "https://example.com/base/root/.hidden/a..b",
            "https://example.com/base/root/inside/file",
        ]
