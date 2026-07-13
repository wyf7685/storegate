import contextlib
import ssl
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Literal, Self
from urllib.parse import quote

import anyio
import httpx

from .auth import build_auth
from .errors import DavClientError, DavHttpStatusError
from .models import DavConfig, DavResource


class AsyncDavClient:
    """Async HTTP client for a WebDAV server.

    A thin wrapper over :mod:`httpx` that speaks the WebDAV HTTP methods
    (PROPFIND / MKCOL / COPY / MOVE alongside standard GET / PUT / DELETE).
    Lifecycle mirrors :class:`AsyncS3Client`: an async context manager that
    lazily creates the underlying ``httpx.AsyncClient`` and bounds concurrent
    in-flight requests with a semaphore.
    """

    def __init__(self, config: DavConfig) -> None:
        self._config = config
        self._client: httpx.AsyncClient | None = None
        self._semaphore = anyio.Semaphore(config.max_concurrency)

    async def __aenter__(self) -> Self:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._config.timeout,
                transport=self._build_transport(),
                auth=build_auth(self._config),
            )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _build_transport(self) -> httpx.AsyncHTTPTransport:
        verify: bool | ssl.SSLContext
        if not self._config.verify_ssl:
            verify = False
        elif self._config.ca_cert_path:
            verify = ssl.create_default_context(cafile=self._config.ca_cert_path)
        else:
            verify = True
        return httpx.AsyncHTTPTransport(retries=3, http2=self._config.http2, verify=verify)

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise DavClientError("AsyncDavClient must be used with 'async with'")
        return self._client

    def _build_path(self, path: str) -> str:
        """Build the URL path for *path*: ``root_prefix`` + URL-encoded segments."""
        rel = path.lstrip("/")
        encoded = quote(rel, safe="/") if rel else ""
        if encoded:
            return f"{self._config.root_prefix}/{encoded}"
        return self._config.root_prefix or "/"

    def _build_url(self, path: str) -> str:
        """Build an absolute URL for *path* (used for requests and COPY/MOVE Destination)."""
        return f"{self._config.base_url}{self._build_path(path)}"

    async def _request(
        self,
        *,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        content: bytes | AsyncIterator[bytes] | None = None,
    ) -> httpx.Response:
        client = self._require_client()
        url = self._build_url(path)
        async with self._semaphore:
            response = await client.request(method=method, url=url, headers=headers or {}, content=content)
        if response.status_code >= 400:
            body = response.text.strip() or "<empty body>"
            raise DavHttpStatusError(method, str(response.request.url), response.status_code, body)
        return response

    @contextlib.asynccontextmanager
    async def stream_get(self, path: str, *, range_start: int | None = None) -> AsyncIterator[httpx.Response]:
        """Stream a GET response body without buffering it fully into memory."""
        client = self._require_client()
        headers: dict[str, str] = {}
        if range_start:
            headers["Range"] = f"bytes={range_start}-"
        request = client.build_request("GET", self._build_url(path), headers=headers)
        response = await client.send(request, stream=True)
        try:
            if response.status_code >= 400:
                await response.aread()
                body = response.text.strip() or "<empty body>"
                raise DavHttpStatusError("GET", str(response.request.url), response.status_code, body)
            yield response
        finally:
            await response.aclose()

    async def put(self, path: str, content: bytes | AsyncIterator[bytes]) -> None:
        await self._request(method="PUT", path=path, content=content)

    async def delete(self, path: str) -> None:
        await self._request(method="DELETE", path=path)

    async def mkcol(self, path: str) -> None:
        await self._request(method="MKCOL", path=path)

    async def copy(
        self,
        src: str,
        dst: str,
        *,
        overwrite: bool = True,
        depth: Literal[0, 1, "infinity"] | None = None,
    ) -> None:
        headers: dict[str, str] = {"Destination": self._build_url(dst), "Overwrite": "T" if overwrite else "F"}
        if depth is not None:
            headers["Depth"] = str(depth)
        await self._request(method="COPY", path=src, headers=headers)

    async def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        headers = {"Destination": self._build_url(dst), "Overwrite": "T" if overwrite else "F"}
        await self._request(method="MOVE", path=src, headers=headers)

    async def propfind(
        self,
        path: str,
        *,
        depth: Literal[0, 1, "infinity"] = 0,
    ) -> list[DavResource]:
        from ..utils import PROPFIND_BODY, parse_multistatus

        headers = {"Depth": str(depth), "Content-Type": "application/xml; charset=utf-8"}
        response = await self._request(method="PROPFIND", path=path, headers=headers, content=PROPFIND_BODY)
        return parse_multistatus(response.content)
