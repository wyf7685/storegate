class S3ClientError(RuntimeError):
    """Base exception raised by S3 client operations."""


class S3HttpStatusError(S3ClientError):
    """Raised when S3 returns a 4xx/5xx response."""

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
        super().__init__(f"S3 request failed: {method} {url} -> {status_code}: {body}")


class S3ResponseParseError(S3ClientError):
    """Raised when S3 response cannot be parsed as expected."""
