import httpx

from .models import DavConfig


class _BearerAuth(httpx.Auth):
    """Inject an ``Authorization: Bearer <token>`` header on every request."""

    def __init__(self, token: str) -> None:
        self._token = token

    def auth_flow(self, request: httpx.Request):
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


def build_auth(config: DavConfig) -> httpx.Auth | None:
    """Build the ``httpx.Auth`` for *config*'s auth mode, or ``None`` for anonymous."""
    match config.auth_mode:
        case "basic":
            assert config.username is not None  # validated by DavConfig
            assert config.password is not None
            return httpx.BasicAuth(config.username, config.password.get_secret_value())
        case "bearer":
            assert config.token is not None
            return _BearerAuth(config.token.get_secret_value())
        case "anonymous":
            return None
