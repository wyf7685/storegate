"""Unit tests for WebDAV multistatus XML parsing (no network)."""

import pytest

from storegate.storage.abstract import EntryKind, UnsupportedOperationError
from storegate.storage.dav.utils import (
    dav_resource_to_file_info,
    href_to_storage_path,
    parse_multistatus,
)

URL_PREFIX = "/dav/storegate"

WSGIDAV_DEPTH1 = b"""<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/storegate/</D:href>
    <D:propstat><D:prop>
      <D:resourcetype><D:collection/></D:resourcetype>
      <D:displayname>storegate</D:displayname>
    </D:prop></D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/storegate/foo.txt</D:href>
    <D:propstat><D:prop>
      <D:resourcetype/>
      <D:getcontentlength>11</D:getcontentlength>
      <D:getlastmodified>Wed, 01 Jan 2025 12:00:00 GMT</D:getlastmodified>
      <D:creationdate>2025-01-01T12:00:00Z</D:creationdate>
    </D:prop></D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/storegate/sub/</D:href>
    <D:propstat><D:prop>
      <D:resourcetype><D:collection/></D:resourcetype>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"""

DEFAULT_NS = b"""<?xml version="1.0"?>
<multistatus xmlns="DAV:">
  <response>
    <href>/dav/storegate/bar.txt</href>
    <propstat><prop>
      <resourcetype/>
      <getcontentlength>5</getcontentlength>
    </prop></propstat>
  </response>
</multistatus>"""

ENCODED_HREF = b"""<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/storegate/%E4%B8%AD%E6%96%87.txt</D:href>
    <D:propstat><D:prop><D:resourcetype/><D:getcontentlength>6</D:getcontentlength></D:prop></D:propstat>
  </D:response>
</D:multistatus>"""

ABSOLUTE_HREF = b"""<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>https://host/dav/storegate/abs.txt</D:href>
    <D:propstat><D:prop><D:resourcetype/><D:getcontentlength>3</D:getcontentlength></D:prop></D:propstat>
  </D:response>
</D:multistatus>"""

SPECIAL_RESOURCE = b"""<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:" xmlns:X="urn:example:links">
  <D:response>
    <D:href>/dav/storegate/link</D:href>
    <D:propstat><D:prop>
      <D:resourcetype><D:collection/><X:symlink/></D:resourcetype>
      <D:getcontentlength>7</D:getcontentlength>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"""

MIXED_PROPSTAT_STATUS = b"""<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/storegate/ok.txt</D:href>
    <D:propstat>
      <D:prop><D:resourcetype/><D:getcontentlength>4</D:getcontentlength></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/storegate/forbidden.txt</D:href>
    <D:propstat>
      <D:prop><D:resourcetype/><D:getcontentlength/></D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""

RESPONSE_LEVEL_404 = b"""<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/storegate/present.txt</D:href>
    <D:propstat>
      <D:prop><D:resourcetype/><D:getcontentlength>9</D:getcontentlength></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/dav/storegate/vanished.txt</D:href>
    <D:status>HTTP/1.1 404 Not Found</D:status>
  </D:response>
</D:multistatus>"""

PARTIAL_PROPSTAT = b"""<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/storegate/partial.txt</D:href>
    <D:propstat>
      <D:prop><D:resourcetype/><D:getcontentlength>12</D:getcontentlength></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
    <D:propstat>
      <D:prop><D:getcontentlength>999</D:getcontentlength><D:displayname/></D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""


class TestParseMultistatus:
    def test_parses_files_and_collections(self) -> None:
        resources = parse_multistatus(WSGIDAV_DEPTH1)
        assert len(resources) == 3

        root, foo, sub = resources
        assert root.href == "/dav/storegate/"
        assert root.is_collection is True
        assert root.display_name == "storegate"

        assert foo.href == "/dav/storegate/foo.txt"
        assert foo.is_collection is False
        assert foo.content_length == 11
        assert foo.last_modified is not None
        assert foo.last_modified.year == 2025
        assert foo.creation_date is not None

        assert sub.href == "/dav/storegate/sub/"
        assert sub.is_collection is True

    def test_default_namespace_supported(self) -> None:
        resources = parse_multistatus(DEFAULT_NS)
        assert len(resources) == 1
        assert resources[0].href == "/dav/storegate/bar.txt"
        assert resources[0].content_length == 5
        assert resources[0].is_collection is False

    def test_url_encoded_href_decoded(self) -> None:
        resources = parse_multistatus(ENCODED_HREF)
        assert resources[0].href == "/dav/storegate/中文.txt"

    def test_absolute_url_href(self) -> None:
        resources = parse_multistatus(ABSOLUTE_HREF)
        assert resources[0].href == "https://host/dav/storegate/abs.txt"

    def test_empty_multistatus(self) -> None:
        resources = parse_multistatus(b'<?xml version="1.0"?><D:multistatus xmlns:D="DAV:"/>')
        assert resources == []

    def test_preserves_nonstandard_resource_types(self) -> None:
        resource = parse_multistatus(SPECIAL_RESOURCE)[0]
        assert resource.resource_types == ("{DAV:}collection", "{urn:example:links}symlink")
        assert resource.is_collection is True

    def test_failed_propstat_resource_is_skipped(self) -> None:
        """A 403 propstat means "inaccessible", not "a zero-byte file".

        Regression: propstat statuses were ignored, so an unreadable child was
        emitted as a normal empty file. walk/copytree then copied a phantom
        empty over a real destination, and _is_dir_empty counted it as present.
        """
        resources = parse_multistatus(MIXED_PROPSTAT_STATUS)
        assert [resource.href for resource in resources] == ["/dav/storegate/ok.txt"]
        assert resources[0].content_length == 4

    def test_response_level_failure_status_is_skipped(self) -> None:
        resources = parse_multistatus(RESPONSE_LEVEL_404)
        assert [resource.href for resource in resources] == ["/dav/storegate/present.txt"]
        assert resources[0].content_length == 9

    def test_properties_come_from_the_successful_propstat(self) -> None:
        """Values must not be harvested out of a 404 propstat in the same response."""
        resource = parse_multistatus(PARTIAL_PROPSTAT)[0]
        assert resource.href == "/dav/storegate/partial.txt"
        assert resource.content_length == 12
        assert resource.display_name is None


class TestHrefToStoragePath:
    def test_strips_url_prefix(self) -> None:
        assert href_to_storage_path("/dav/storegate/foo.txt", URL_PREFIX) == "foo.txt"

    def test_strips_trailing_slash_for_collection(self) -> None:
        assert href_to_storage_path("/dav/storegate/sub/", URL_PREFIX) == "sub"

    def test_root_collection_empty(self) -> None:
        assert href_to_storage_path("/dav/storegate/", URL_PREFIX) == ""

    def test_absolute_url(self) -> None:
        assert href_to_storage_path("https://host/dav/storegate/abs.txt", URL_PREFIX) == "abs.txt"

    def test_no_prefix_match_returns_path(self) -> None:
        assert href_to_storage_path("/other/foo", URL_PREFIX) == "other/foo"


class TestDavResourceToFileInfo:
    def test_file_info(self) -> None:
        resource = parse_multistatus(WSGIDAV_DEPTH1)[1]
        info = dav_resource_to_file_info(resource, "foo.txt")
        assert info.path == "/foo.txt"
        assert info.name == "foo.txt"
        assert info.kind is EntryKind.FILE
        assert info.size == 11
        assert info.modified is not None

    def test_collection_info(self) -> None:
        resource = parse_multistatus(WSGIDAV_DEPTH1)[2]
        info = dav_resource_to_file_info(resource, "sub")
        assert info.path == "/sub"
        assert info.name == "sub"
        assert info.kind is EntryKind.DIRECTORY
        assert info.size == 0

    def test_root_info(self) -> None:
        resource = parse_multistatus(WSGIDAV_DEPTH1)[0]
        info = dav_resource_to_file_info(resource, "")
        assert info.path == "/"
        assert info.name == ""
        assert info.kind is EntryKind.DIRECTORY

    def test_special_resource_is_not_converted_to_file_info(self) -> None:
        resource = parse_multistatus(SPECIAL_RESOURCE)[0]
        with pytest.raises(UnsupportedOperationError):
            dav_resource_to_file_info(resource, "link")
