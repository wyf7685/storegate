from __future__ import annotations


class DavClientError(RuntimeError):
    """Base exception raised by WebDAV client operations."""


class DavHttpStatusError(DavClientError):
    """Raised when a WebDAV server returns a 4xx/5xx response."""

    def __init__(
        self,
        method: str,
        url: str,
        status_code: int,
        body: str,
    ) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.body = body
        super().__init__(f"WebDAV request failed: {method} {url} -> {status_code}: {body}")


class DavResponseParseError(DavClientError):
    """Raised when a WebDAV response cannot be parsed as expected."""
