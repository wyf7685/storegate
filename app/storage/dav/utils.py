"""WebDAV multistatus XML parsing and ``DavResource`` → ``FileInfo`` conversion."""

import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import PurePosixPath
from urllib.parse import unquote, urlparse

from app.storage.abstract import FileInfo

from .client import DavResource, DavResponseParseError

# PROPFIND body requesting the properties mapped to FileInfo fields.
PROPFIND_BODY = (
    b'<?xml version="1.0" encoding="utf-8"?>'
    b'<D:propfind xmlns:D="DAV:">'
    b"<D:prop>"
    b"<D:resourcetype/>"
    b"<D:getcontentlength/>"
    b"<D:getlastmodified/>"
    b"<D:creationdate/>"
    b"<D:displayname/>"
    b"</D:prop>"
    b"</D:propfind>"
)


def _parse_xml(content: bytes) -> ET.Element:
    try:
        return ET.fromstring(content)  # noqa: S314
    except ET.ParseError as err:
        raise DavResponseParseError("Failed to parse WebDAV XML response") from err


def _find_text(elem: ET.Element | None, tag: str) -> str | None:
    if elem is None:
        return None
    value = elem.findtext(f".//{{*}}{tag}")
    return value if value not in (None, "") else None


def _parse_http_date(value: str) -> datetime | None:
    try:
        return parsedate_to_datetime(value)
    except TypeError, ValueError:
        return None


def _parse_iso_date(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return _parse_http_date(value)


def parse_multistatus(content: bytes) -> list[DavResource]:
    """Parse a WebDAV ``multistatus`` body into a list of :class:`DavResource`."""
    root = _parse_xml(content)
    resources: list[DavResource] = []
    for response in root.findall(".//{*}response"):
        href = _find_text(response, "href")
        if not href:
            continue
        href = unquote(href)
        prop = response.find(".//{*}prop")
        is_collection = prop is not None and prop.find(".//{*}collection") is not None

        length_text = _find_text(response, "getcontentlength")
        content_length: int | None = None
        if length_text is not None:
            try:
                content_length = int(length_text)
            except ValueError:
                content_length = None

        last_modified_text = _find_text(response, "getlastmodified")
        last_modified = _parse_http_date(last_modified_text) if last_modified_text else None

        creation_text = _find_text(response, "creationdate")
        creation_date = _parse_iso_date(creation_text) if creation_text else None

        display_name = _find_text(response, "displayname")

        resources.append(
            DavResource(
                href=href,
                is_collection=is_collection,
                content_length=content_length,
                last_modified=last_modified,
                creation_date=creation_date,
                display_name=display_name,
            )
        )
    return resources


def href_to_storage_path(href: str, url_prefix: str) -> str:
    """Strip server/path prefixes from a WebDAV href to get a storage-relative path.

    *url_prefix* is ``base_url``'s path joined with ``root_prefix``
    (e.g. ``/dav/storegate``). The result is a POSIX path without a leading
    slash (empty string for the root collection).
    """
    path = href.split("?", 1)[0].split("#", 1)[0]
    if "://" in path:
        path = urlparse(path).path
    if url_prefix and (idx := path.find(url_prefix)) >= 0:
        path = path[idx + len(url_prefix) :]
    return path.lstrip("/").rstrip("/")


def dav_resource_to_file_info(resource: DavResource, storage_path: str) -> FileInfo:
    """Convert a :class:`DavResource` to :class:`FileInfo`.

    *storage_path* is the storage-relative path (output of
    :func:`href_to_storage_path`); it is normalized to an absolute POSIX path.
    """
    np = PurePosixPath("/") / PurePosixPath(storage_path)
    return FileInfo(
        path=np.as_posix(),
        name=np.name,
        is_dir=resource.is_collection,
        size=resource.content_length or 0,
        modified=resource.last_modified,
        created=resource.creation_date,
    )
