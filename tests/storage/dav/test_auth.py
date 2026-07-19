"""WebDAV client configuration tests."""

import pytest
from pydantic import SecretStr

from app.storage.dav.client import build_auth
from app.storage.dav.client.auth import _BearerAuth
from app.storage.dav.client.models import DavConfig
from app.utils import httpx

pytestmark = pytest.mark.httpx


class TestBuildAuth:
    def test_basic_auth(self) -> None:
        cfg = DavConfig(
            base_url="https://host/dav",
            auth_mode="basic",
            username="user",
            password=SecretStr("secret"),
        )
        auth = build_auth(cfg)
        assert isinstance(auth, httpx.BasicAuth)

    def test_bearer_auth(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="bearer", token=SecretStr("abc123"))
        auth = build_auth(cfg)
        assert isinstance(auth, _BearerAuth)
        request = httpx.Request("GET", "https://host/dav/")
        next(auth.auth_flow(request))
        assert request.headers["Authorization"] == "Bearer abc123"

    def test_anonymous_auth(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="anonymous")
        assert build_auth(cfg) is None
