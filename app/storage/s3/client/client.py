import base64
import hashlib
import xml.etree.ElementTree as ET
from collections.abc import AsyncGenerator, Iterable, Mapping
from datetime import UTC, datetime
from types import TracebackType
from typing import Literal, Self
from urllib.parse import quote

import anyio.lowlevel

from app.utils import httpx

from .auth import AWSSigV4Signer, _encode_query_kv, _format_query_kv
from .errors import S3ClientError, S3HttpStatusError, S3ResponseParseError
from .models import (
    CompletedPart,
    CopyObjectResult,
    CopyPartResult,
    HeadObjectOutput,
    ListObjectsCommonPrefix,
    ListObjectsContents,
    S3Config,
)

# SigV4 payload hash indicating the request body is not signed. Avoids hashing
# upload bodies (notably multipart parts) while remaining a valid S3 request.
_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"


def _parse_xml(content: bytes) -> ET.Element:
    try:
        return ET.fromstring(content)  # noqa: S314
    except ET.ParseError as err:
        raise S3ResponseParseError("Failed to parse S3 XML response") from err


def _find_required_text(root: ET.Element, tag: str) -> str:
    # S3 responses carry a default xmlns, so match the tag in any namespace.
    value = root.findtext(f".//{{*}}{tag}")
    if value is None or value == "":
        raise S3ResponseParseError(f"Missing field in S3 response: {tag}")
    return value


def _build_complete_multipart_xml(parts: Iterable[CompletedPart]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for part in parts:
        node = ET.SubElement(root, "Part")
        ET.SubElement(node, "PartNumber").text = str(part["PartNumber"])
        ET.SubElement(node, "ETag").text = part["ETag"]
    return ET.tostring(root, encoding="utf-8")


def _check_copy_error(root: ET.Element, method: str) -> None:
    """Raise S3ResponseParseError if the XML body contains an ``<Error>`` node.

    CopyObject / UploadPartCopy can return HTTP 200 with ``<Error>`` in the
    body when the copy fails *during* execution (not at initiation time).
    """
    if error_nodes := root.findall(".//{*}Error"):
        errors: list[str] = []
        for error_node in error_nodes:
            code = _find_required_text(error_node, "Code")
            message = _find_required_text(error_node, "Message")
            errors.append(f"{code}: {message}")
        raise S3ResponseParseError(f"{method} failed: {", ".join(errors)}")


def _parse_copy_object_result(content: bytes) -> CopyObjectResult:
    root = _parse_xml(content)
    _check_copy_error(root, "CopyObject")
    etag = _find_required_text(root, "ETag")
    last_modified_str = _find_required_text(root, "LastModified")
    return CopyObjectResult(
        etag=etag,
        last_modified=datetime.fromisoformat(last_modified_str),
    )


def _parse_copy_part_result(content: bytes) -> CopyPartResult:
    root = _parse_xml(content)
    _check_copy_error(root, "UploadPartCopy")
    etag = _find_required_text(root, "ETag")
    last_modified_str = _find_required_text(root, "LastModified")
    return CopyPartResult(
        etag=etag,
        last_modified=datetime.fromisoformat(last_modified_str),
    )


class AsyncS3Client:
    """Async HTTP client for an S3-compatible object storage service.

    Uses AWS Signature Version 4. Supports AWS S3 (virtual-hosted-style) and
    custom endpoints (MinIO, Tencent COS, ...) via ``S3Config.endpoint_url``
    and ``S3Config.path_style``.
    """

    def __init__(self, config: S3Config) -> None:
        self._config = config
        self._host = self._compute_host()
        self._base_url = f"{config.scheme}://{self._host}"
        self._timeout = config.timeout
        self._client: httpx.AsyncClient | None = None
        self._signer = AWSSigV4Signer(
            access_key_id=config.access_key_id.get_secret_value(),
            secret_access_key=config.secret_access_key.get_secret_value(),
            region=config.region,
        )
        self._semaphore = anyio.Semaphore(config.max_concurrency)

    def _compute_host(self) -> str:
        """Compute the Host header value per the addressing rules.

        - No ``endpoint_url``: AWS S3 virtual-hosted-style
          ``{bucket}.s3.{region}.amazonaws.com``.
        - ``endpoint_url`` + ``path_style=False``: virtual-hosted over the
          custom endpoint ``{bucket}.{endpoint_url}`` (Tencent COS).
        - ``endpoint_url`` + ``path_style=True``: path-style, host is the bare
          endpoint and the bucket goes into the URL path (MinIO).
        """
        cfg = self._config
        if cfg.endpoint_url:
            if cfg.path_style:
                return cfg.endpoint_url
            return f"{cfg.bucket}.{cfg.endpoint_url}"
        return f"{cfg.bucket}.s3.{cfg.region}.amazonaws.com"

    async def __aenter__(self) -> Self:
        if self._client is None:
            transport = httpx.AsyncHTTPTransport(retries=3, http2=True)
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                transport=transport,
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

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise S3ClientError("AsyncS3Client must be used with 'async with'")
        return self._client

    @staticmethod
    def _normalize_key(key: str) -> str:
        return key.removeprefix("/")

    def _build_request_path(self, key: str) -> str:
        """Build the URL path for *key*, including the bucket prefix when path-style."""
        normalized = self._normalize_key(key)
        encoded = quote(normalized, safe="/-_.~")
        encoded = encoded.replace("./", ".%2F")
        path = f"/{encoded}" if encoded else "/"
        if self._config.path_style:
            # Path-style: prefix the bucket segment.
            return f"/{self._config.bucket}{path}" if path != "/" else f"/{self._config.bucket}"
        return path

    def _build_copy_source(self, source_key: str) -> str:
        """Build the ``x-amz-copy-source`` header value: ``/{bucket}/{encoded-key}``."""
        normalized = self._normalize_key(source_key)
        encoded = quote(normalized, safe="/-_.~")
        return f"/{self._config.bucket}/{encoded}"

    @staticmethod
    def _normalize_params(params: Mapping[str, str | int] | None) -> dict[str, str]:
        if params is None:
            return {}
        return {str(key): str(value) for key, value in params.items()}

    def _build_signed_headers(
        self,
        *,
        method: str,
        canonical_uri: str,
        params: Mapping[str, str],
        headers: Mapping[str, str] | None,
        now: datetime,
    ) -> dict[str, str]:
        """Assemble the headers for a signed request.

        Injects ``host``, ``x-amz-date``, ``x-amz-content-sha256`` and an
        optional ``x-amz-security-token``, then computes the ``Authorization``
        header via SigV4.
        """
        sign_headers: dict[str, str] = {
            "host": self._host,
            "x-amz-date": now.strftime("%Y%m%dT%H%M%SZ"),
            "x-amz-content-sha256": _UNSIGNED_PAYLOAD,
        }
        if self._config.session_token:
            sign_headers["x-amz-security-token"] = self._config.session_token
        if headers:
            # Caller-provided headers win, except for the signing primitives
            # we manage above (callers do not set those directly).
            sign_headers.update(headers)

        authorization = self._signer.build_authorization(
            method=method,
            canonical_uri=canonical_uri,
            params=params,
            headers=sign_headers,
            payload_hash=_UNSIGNED_PAYLOAD,
            now=now,
        )
        sign_headers["Authorization"] = authorization
        return sign_headers

    async def _request(
        self,
        *,
        method: Literal["GET", "POST", "PUT", "DELETE", "HEAD"],
        key: str,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
    ) -> httpx.Response:
        query = self._normalize_params(params)
        request_path = self._build_request_path(key)
        canonical_query = _format_query_kv(_encode_query_kv(query))
        now = datetime.now(UTC)

        signed_headers = self._build_signed_headers(
            method=method,
            canonical_uri=request_path,
            params=query,
            headers=headers,
            now=now,
        )

        url = f"{request_path}?{canonical_query}" if canonical_query else request_path

        async with self._semaphore:
            response = await self._require_client().request(
                method=method,
                url=url,
                headers=signed_headers,
                content=content,
            )

        if response.status_code >= 400:
            body = response.text.strip() or "<empty body>"
            raise S3HttpStatusError(
                method=method,
                url=str(response.request.url),
                status_code=response.status_code,
                body=body,
            )

        return response

    async def head_bucket(self) -> bool:
        try:
            await self._request(method="HEAD", key="")
        except S3HttpStatusError as err:
            if err.status_code == 404:
                return False
            raise
        return True

    async def list_objects(
        self,
        prefix: str | None = None,
        delimiter: str = "/",
        max_keys: int = 1000,
    ) -> AsyncGenerator[ListObjectsContents | ListObjectsCommonPrefix]:
        # ListObjectsV2 — better performance than v1 and uses continuation
        # tokens instead of markers.
        params: dict[str, str | int] = {"list-type": "2", "max-keys": max_keys}
        if prefix is not None:
            if prefix and not prefix.endswith(delimiter):
                prefix += delimiter
            params["prefix"] = prefix
        if delimiter is not None:
            params["delimiter"] = delimiter

        response = await self._request(method="GET", key="", params=params)
        root = _parse_xml(response.content)
        is_truncated = _find_required_text(root, "IsTruncated").lower() == "true"

        while True:
            for common_prefix in root.findall(".//{*}CommonPrefixes"):
                prefix_text = _find_required_text(common_prefix, "Prefix")
                yield ListObjectsCommonPrefix(prefix=prefix_text)
                await anyio.lowlevel.checkpoint()
            for contents in root.findall(".//{*}Contents"):
                key = _find_required_text(contents, "Key")
                last_modified = _find_required_text(contents, "LastModified")
                etag = _find_required_text(contents, "ETag")
                size_str = _find_required_text(contents, "Size")
                try:
                    size = int(size_str)
                except ValueError as err:
                    raise S3ResponseParseError("Invalid Size in list_objects response") from err
                yield ListObjectsContents(
                    key=key, size=size, etag=etag, last_modified=datetime.fromisoformat(last_modified)
                )
                await anyio.lowlevel.checkpoint()

            if not is_truncated:
                break

            next_token = _find_required_text(root, "NextContinuationToken")
            response = await self._request(method="GET", key="", params=params | {"continuation-token": next_token})
            root = _parse_xml(response.content)
            is_truncated = _find_required_text(root, "IsTruncated").lower() == "true"

    async def head_object(self, key: str) -> HeadObjectOutput | None:
        try:
            response = await self._request(method="HEAD", key=key)
        except S3HttpStatusError as err:
            if err.status_code == 404:
                return None
            raise
        content_length_str = response.headers.get("Content-Length")
        if content_length_str is None or content_length_str == "":
            raise S3ResponseParseError("Missing Content-Length in head_object response")
        try:
            content_length = int(content_length_str)
        except ValueError as err:
            raise S3ResponseParseError("Invalid Content-Length in head_object response") from err
        etag = response.headers.get("ETag")
        if etag is None or etag == "":
            raise S3ResponseParseError("Missing ETag in head_object response")
        last_modified_str = response.headers.get("Last-Modified")
        if last_modified_str is None or last_modified_str == "":
            raise S3ResponseParseError("Missing Last-Modified in head_object response")
        last_modified = datetime.strptime(last_modified_str, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=UTC)
        return HeadObjectOutput(content_length=content_length, etag=etag, last_modified=last_modified)

    async def get_object(self, key: str, range: tuple[int, int] | None = None) -> bytes:  # noqa: A002
        headers: dict[str, str] = {}
        if range is not None:
            headers["Range"] = f"bytes={range[0]}-{range[1]}"
        response = await self._request(method="GET", key=key, headers=headers)
        return response.content

    async def put_object(self, key: str, data: bytes) -> None:
        await self._request(method="PUT", key=key, content=data)

    async def put_object_copy(self, source_key: str, target_key: str) -> CopyObjectResult:
        """Copy an existing S3 object to a new key (server-side, no data transfer).

        Suitable for objects up to 5 GiB. For larger sources use
        :meth:`upload_part_copy` via a multipart upload.
        """
        headers: dict[str, str] = {
            "x-amz-copy-source": self._build_copy_source(source_key),
        }
        response = await self._request(
            method="PUT",
            key=target_key,
            headers=headers,
            content=b"",
        )
        return _parse_copy_object_result(response.content)

    async def delete_object(self, key: str) -> None:
        await self._request(method="DELETE", key=key)

    async def delete_objects(self, keys: Iterable[str]) -> list[str]:
        keys = list(keys)
        if not keys:
            return []

        root = ET.Element("Delete")
        for key in keys:
            ET.SubElement(ET.SubElement(root, "Object"), "Key").text = key
        content = ET.tostring(root, encoding="utf-8")
        content_md5 = base64.b64encode(hashlib.md5(content).digest()).decode()  # noqa: S324
        response = await self._request(
            method="POST",
            key="",
            params={"delete": ""},
            headers={
                "Content-Type": "application/xml",
                "Content-MD5": content_md5,
            },
            content=content,
        )

        root = _parse_xml(response.content)
        if error_nodes := root.findall(".//{*}Error"):
            errors: list[str] = []
            for error_node in error_nodes:
                code = _find_required_text(error_node, "Code")
                message = _find_required_text(error_node, "Message")
                errors.append(f"{code}: {message}")
            raise S3ResponseParseError(f"Failed to delete objects: {", ".join(errors)}")
        return [_find_required_text(deleted_node, "Key") for deleted_node in root.findall(".//{*}Deleted")]

    async def create_multipart_upload(self, key: str) -> str:
        response = await self._request(
            method="POST",
            key=key,
            params={"uploads": ""},
        )
        root = _parse_xml(response.content)
        return _find_required_text(root, "UploadId")

    async def upload_part(
        self,
        key: str,
        data: bytes,
        part_number: int,
        upload_id: str,
    ) -> str:
        response = await self._request(
            method="PUT",
            key=key,
            params={"partNumber": part_number, "uploadId": upload_id},
            content=data,
        )
        etag = response.headers.get("ETag")
        if etag is None or etag == "":
            raise S3ResponseParseError("Missing ETag in upload_part response")
        return etag

    async def upload_part_copy(
        self,
        source_key: str,
        target_key: str,
        upload_id: str,
        part_number: int,
        byte_range: tuple[int, int],
    ) -> CopyPartResult:
        """Copy a byte range from an existing S3 object as a multipart upload part.

        *byte_range* is an inclusive ``(first, last)`` pair (0-based).
        """
        headers: dict[str, str] = {
            "x-amz-copy-source": self._build_copy_source(source_key),
            "x-amz-copy-source-range": f"bytes={byte_range[0]}-{byte_range[1]}",
        }
        response = await self._request(
            method="PUT",
            key=target_key,
            params={"partNumber": part_number, "uploadId": upload_id},
            headers=headers,
        )
        return _parse_copy_part_result(response.content)

    async def complete_multipart_upload(
        self,
        key: str,
        upload_id: str,
        parts: list[CompletedPart],
    ) -> None:
        response = await self._request(
            method="POST",
            key=key,
            params={"uploadId": upload_id},
            headers={"Content-Type": "application/xml"},
            content=_build_complete_multipart_xml(parts),
        )
        root = _parse_xml(response.content)
        _find_required_text(root, "ETag")

    async def abort_multipart_upload(self, key: str, upload_id: str) -> None:
        await self._request(
            method="DELETE",
            key=key,
            params={"uploadId": upload_id},
        )
