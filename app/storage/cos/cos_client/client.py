import xml.etree.ElementTree as ET
from collections.abc import AsyncGenerator, Iterable, Mapping, Sequence
from datetime import datetime
from types import TracebackType
from typing import Literal, Self
from urllib.parse import quote, urlencode

import anyio.lowlevel
import httpx

from .auth import CosV5Signer
from .errors import CosClientError, CosHttpStatusError, CosResponseParseError
from .models import (
    CopyObjectResult,
    CopyPartResult,
    CosConfig,
    HeadObjectResponse,
    ListObjectsDir,
    ListObjectsItem,
    MultipartUploadPart,
)


def _parse_xml(content: bytes) -> ET.Element:
    try:
        return ET.fromstring(content)  # noqa: S314
    except ET.ParseError as err:
        raise CosResponseParseError("Failed to parse COS XML response") from err


def _find_required_text(root: ET.Element, tag: str) -> str:
    value = root.findtext(f".//{tag}")
    if value is None or value == "":
        raise CosResponseParseError(f"Missing field in COS response: {tag}")
    return value


def _build_complete_multipart_xml(parts: Sequence[MultipartUploadPart]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for part in parts:
        node = ET.SubElement(root, "Part")
        ET.SubElement(node, "PartNumber").text = str(part["PartNumber"])
        ET.SubElement(node, "ETag").text = part["ETag"]
    return ET.tostring(root, encoding="utf-8")


def _check_copy_error(root: ET.Element, method: str) -> None:
    """Raise CosResponseParseError if the XML body contains an ``<Error>`` node.

    PUT Object - Copy can return HTTP 200 with ``<Error>`` in the body when
    the copy fails *during* execution (not at initiation time).
    """
    if error_nodes := root.findall(".//Error"):
        errors: list[str] = []
        for error_node in error_nodes:
            code = _find_required_text(error_node, "Code")
            message = _find_required_text(error_node, "Message")
            errors.append(f"{code}: {message}")
        raise CosResponseParseError(f"{method} failed: {", ".join(errors)}")


def _parse_copy_object_result(content: bytes) -> CopyObjectResult:
    root = _parse_xml(content)
    _check_copy_error(root, "PUT Object - Copy")
    etag = _find_required_text(root, "ETag")
    crc64_str = _find_required_text(root, "CRC64")
    last_modified_str = _find_required_text(root, "LastModified")
    return CopyObjectResult(
        etag=etag,
        crc64=int(crc64_str),
        last_modified=datetime.fromisoformat(last_modified_str),
    )


def _parse_copy_part_result(content: bytes) -> CopyPartResult:
    root = _parse_xml(content)
    _check_copy_error(root, "Upload Part - Copy")
    etag = _find_required_text(root, "ETag")
    last_modified_str = _find_required_text(root, "LastModified")
    return CopyPartResult(
        etag=etag,
        last_modified=datetime.fromisoformat(last_modified_str),
    )


class AsyncCosClient:
    def __init__(self, config: CosConfig) -> None:
        self._config = config
        self._host = f"{config.bucket}.cos{"-internal" if config.is_internal else ""}.{config.region}.myqcloud.com"
        self._base_url = f"{config.scheme}://{self._host}"
        self._public_host = f"{config.bucket}.cos.{config.region}.myqcloud.com"
        self._presign_base_url = f"{config.scheme}://{self._public_host}"
        self._token = config.token
        self._timeout = config.timeout
        self._client: httpx.AsyncClient | None = None
        self._signer = CosV5Signer(
            secret_id=config.secret_id.get_secret_value(),
            secret_key=config.secret_key.get_secret_value(),
        )
        self._semaphore = anyio.Semaphore(config.max_concurrency)

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
            raise CosClientError("AsyncCosClient must be used with 'async with'")
        return self._client

    @staticmethod
    def _normalize_key(key: str) -> str:
        return key.removeprefix("/")

    def _build_sign_path(self, key: str) -> str:
        normalized = self._normalize_key(key)
        return f"/{normalized}" if normalized else "/"

    def _build_request_path(self, key: str) -> str:
        normalized = self._normalize_key(key)
        encoded = quote(normalized, safe="/-_.~")
        encoded = encoded.replace("./", ".%2F")
        return f"/{encoded}" if encoded else "/"

    def _build_copy_source(self, source_key: str) -> str:
        """Build the ``x-cos-copy-source`` header value.

        Format: ``{host}/{url-encoded-key}`` (no scheme, key is URL-encoded
        with the same rules as ``_build_request_path``).
        """
        normalized = self._normalize_key(source_key)
        encoded = quote(normalized, safe="/-_.~")
        return f"{self._host}/{encoded}"

    @staticmethod
    def _normalize_params(params: Mapping[str, str | int] | None) -> dict[str, str]:
        if params is None:
            return {}
        return {str(key): str(value) for key, value in params.items()}

    @staticmethod
    def _normalize_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
        if headers is None:
            return {}
        return {str(key): str(value) for key, value in headers.items()}

    @staticmethod
    def _has_header(headers: Mapping[str, str], name: str) -> bool:
        lower = name.lower()
        return any(key.lower() == lower for key in headers)

    def _build_signed_headers(
        self,
        *,
        method: str,
        sign_path: str,
        params: Mapping[str, str],
        headers: Mapping[str, str] | None,
        expired: int,
        internal: bool = True,
    ) -> dict[str, str]:
        host = self._host if internal else self._public_host
        sign_headers = self._normalize_headers(headers)
        if not self._has_header(sign_headers, "Host"):
            sign_headers["Host"] = host
        if self._token and not self._has_header(sign_headers, "x-cos-security-token"):
            sign_headers["x-cos-security-token"] = self._token

        authorization = self._signer.build_authorization(
            method=method,
            path=sign_path,
            params=params,
            headers=sign_headers,
            expired=expired,
            host=host,
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
        expired: int = 300,
    ) -> httpx.Response:
        query = self._normalize_params(params)
        sign_path = self._build_sign_path(key)
        request_path = self._build_request_path(key)
        signed_headers = self._build_signed_headers(
            method=method,
            sign_path=sign_path,
            params=query,
            headers=headers,
            expired=expired,
        )

        async with self._semaphore:
            response = await self._require_client().request(
                method=method,
                url=request_path,
                params=query,
                headers=signed_headers,
                content=content,
            )

        if response.status_code >= 400:
            body = response.text.strip() or "<empty body>"
            raise CosHttpStatusError(
                method=method,
                url=str(response.request.url),
                status_code=response.status_code,
                body=body,
            )

        return response

    async def head_bucket(self) -> bool:
        try:
            await self._request(method="HEAD", key="")
        except CosHttpStatusError as err:
            if err.status_code == 404:
                return False
            raise
        return True

    async def list_objects(
        self,
        prefix: str | None = None,
        delimiter: str = "/",
        max_keys: int = 1000,
    ) -> AsyncGenerator[ListObjectsItem | ListObjectsDir]:
        params = {"max-keys": max_keys}
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
            for common_prefix in root.findall(".//CommonPrefixes"):
                prefix_text = _find_required_text(common_prefix, "Prefix")
                yield ListObjectsDir(prefix=prefix_text)
                await anyio.lowlevel.checkpoint()
            for contents in root.findall(".//Contents"):
                key = _find_required_text(contents, "Key")
                last_modified = _find_required_text(contents, "LastModified")
                etag = _find_required_text(contents, "ETag")
                size_str = _find_required_text(contents, "Size")
                try:
                    size = int(size_str)
                except ValueError as err:
                    raise CosResponseParseError("Invalid Size in list_objects response") from err
                yield ListObjectsItem(
                    key=key, size=size, etag=etag, last_modified=datetime.fromisoformat(last_modified)
                )
                await anyio.lowlevel.checkpoint()

            if not is_truncated:
                break

            next_marker = _find_required_text(root, "NextMarker")
            response = await self._request(method="GET", key="", params=params | {"marker": next_marker})
            root = _parse_xml(response.content)
            is_truncated = _find_required_text(root, "IsTruncated").lower() == "true"

    async def head_object(self, key: str) -> HeadObjectResponse | None:
        try:
            response = await self._request(method="HEAD", key=key)
        except CosHttpStatusError as err:
            if err.status_code == 404:
                return None
            raise
        content_length_str = response.headers.get("Content-Length")
        if content_length_str is None or content_length_str == "":
            raise CosResponseParseError("Missing Content-Length in head_object response")
        try:
            content_length = int(content_length_str)
        except ValueError as err:
            raise CosResponseParseError("Invalid Content-Length in head_object response") from err
        etag = response.headers.get("ETag")
        if etag is None or etag == "":
            raise CosResponseParseError("Missing ETag in head_object response")
        return HeadObjectResponse(content_length=content_length, etag=etag)

    async def get_object(self, key: str, range: tuple[int, int] | None = None) -> bytes:  # noqa: A002
        headers: dict[str, str] = {}
        if range is not None:
            headers["Range"] = f"bytes={range[0]}-{range[1]}"
        response = await self._request(method="GET", key=key, headers=headers)
        return response.content

    async def put_object(self, key: str, data: bytes) -> None:
        await self._request(method="PUT", key=key, content=data)

    async def put_object_copy(
        self,
        source_key: str,
        target_key: str,
        *,
        forbid_overwrite: bool = False,
    ) -> CopyObjectResult:
        """Copy an existing COS object to a new key (server-side, no data transfer).

        Suitable for objects up to 5 GiB.  For larger sources use
        :meth:`upload_part_copy` via a multipart upload.
        """
        headers: dict[str, str] = {
            "x-cos-copy-source": self._build_copy_source(source_key),
        }
        if forbid_overwrite:
            headers["x-cos-forbid-overwrite"] = "true"
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
        if not keys:
            return []

        root = ET.Element("Delete")
        for key in keys:
            ET.SubElement(ET.SubElement(root, "Object"), "Key").text = key
        content = ET.tostring(root, encoding="utf-8")
        response = await self._request(
            method="POST",
            key="",
            params={"delete": ""},
            headers={"Content-Type": "application/xml"},
            content=content,
        )

        root = _parse_xml(response.content)
        if error_nodes := root.findall(".//Error"):
            errors: list[str] = []
            for error_node in error_nodes:
                code = _find_required_text(error_node, "Code")
                message = _find_required_text(error_node, "Message")
                errors.append(f"{code}: {message}")
            raise CosResponseParseError(f"Failed to delete objects: {", ".join(errors)}")
        return [_find_required_text(deleted_node, "Key") for deleted_node in root.findall(".//Deleted")]

    async def get_presigned_url(self, key: str, method: str, expired: int) -> str:
        query: dict[str, str] = {}
        headers = self._build_signed_headers(
            method=method,
            sign_path=self._build_sign_path(key),
            params=query,
            headers=None,
            expired=expired,
            internal=False,
        )
        authorization = headers["Authorization"]
        sign_query = urlencode(dict(item.split("=", 1) for item in authorization.split("&")))
        return f"{self._presign_base_url}{self._build_request_path(key)}?{sign_query}"

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
            raise CosResponseParseError("Missing ETag in upload_part response")
        return etag

    async def upload_part_copy(
        self,
        source_key: str,
        target_key: str,
        upload_id: str,
        part_number: int,
        byte_range: tuple[int, int],
    ) -> CopyPartResult:
        """Copy a byte range from an existing COS object as a multipart upload part.

        *byte_range* is an inclusive ``(first, last)`` pair (0-based).
        """
        headers: dict[str, str] = {
            "x-cos-copy-source": self._build_copy_source(source_key),
            "x-cos-copy-source-range": f"bytes={byte_range[0]}-{byte_range[1]}",
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
        parts: list[MultipartUploadPart],
    ) -> None:
        response = await self._request(
            method="POST",
            key=key,
            params={"uploadId": upload_id},
            headers={"Content-Type": "application/xml"},
            content=_build_complete_multipart_xml(parts),
            expired=1200,
        )
        root = _parse_xml(response.content)
        _find_required_text(root, "ETag")

    async def abort_multipart_upload(self, key: str, upload_id: str) -> None:
        await self._request(
            method="DELETE",
            key=key,
            params={"uploadId": upload_id},
        )
