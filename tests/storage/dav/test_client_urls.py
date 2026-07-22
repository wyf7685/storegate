"""WebDAV client configuration tests."""

from storegate.storage.dav.client import AsyncDavClient
from storegate.storage.dav.client.models import DavConfig


class TestBuildPath:
    def test_no_prefix(self) -> None:
        client = AsyncDavClient(DavConfig(base_url="https://host/dav", auth_mode="anonymous"))
        assert client._build_path("foo/bar") == "/foo/bar"
        assert client._build_path("/foo/bar") == "/foo/bar"
        assert client._build_path("") == "/"

    def test_with_prefix(self) -> None:
        client = AsyncDavClient(DavConfig(base_url="https://host/dav", auth_mode="anonymous", root_prefix="/storegate"))
        assert client._build_path("foo/bar") == "/storegate/foo/bar"
        assert client._build_path("") == "/storegate"

    def test_build_url_absolute(self) -> None:
        client = AsyncDavClient(DavConfig(base_url="https://host/dav", auth_mode="anonymous", root_prefix="/storegate"))
        assert client._build_url("foo") == "https://host/dav/storegate/foo"
