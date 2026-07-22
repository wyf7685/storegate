"""Protocol-level tests for AsyncS3Client response handling."""

import contextlib
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator, Callable

import pytest
from pydantic import SecretStr

from storegate.storage.s3.client import (
    AsyncS3Client,
    ListObjectsCommonPrefix,
    ListObjectsContents,
    S3Config,
    S3HttpStatusError,
    S3ResponseParseError,
)
from storegate.utils import httpx

pytestmark = pytest.mark.httpx


def _config() -> S3Config:
    return S3Config(
        access_key_id=SecretStr("test-access-key"),
        secret_access_key=SecretStr("test-secret-key"),
        region="us-east-1",
        bucket="test-bucket",
    )


@contextlib.asynccontextmanager
async def _mocked_client(handler: Callable[[httpx.Request], httpx.Response]) -> AsyncIterator[AsyncS3Client]:
    client = AsyncS3Client(_config())
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://test-bucket.s3.us-east-1.amazonaws.com") as raw:
        client._client = raw
        yield client


async def test_list_objects_paginates_and_parses_entries() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if "continuation-token" not in request.url.params:
            return httpx.Response(
                200,
                content=b"""
                    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                      <IsTruncated>true</IsTruncated>
                      <NextContinuationToken>token-2</NextContinuationToken>
                      <CommonPrefixes><Prefix>root/sub/</Prefix></CommonPrefixes>
                      <Contents>
                        <Key>root/first.txt</Key>
                        <LastModified>2025-01-01T00:00:00+00:00</LastModified>
                        <ETag>etag-1</ETag>
                        <Size>5</Size>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        return httpx.Response(
            200,
            content=b"""
                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <IsTruncated>false</IsTruncated>
                  <Contents>
                    <Key>root/second.txt</Key>
                    <LastModified>2025-01-02T00:00:00+00:00</LastModified>
                    <ETag>etag-2</ETag>
                    <Size>7</Size>
                  </Contents>
                </ListBucketResult>
            """,
        )

    async with _mocked_client(handler) as client:
        objects = [item async for item in client.list_objects(prefix="root")]

    assert len(objects) == 3
    assert objects[0] == ListObjectsCommonPrefix(prefix="root/sub/")
    first = objects[1]
    second = objects[2]
    assert isinstance(first, ListObjectsContents)
    assert isinstance(second, ListObjectsContents)
    assert (first.key, first.size, first.etag, first.last_modified.year) == ("root/first.txt", 5, "etag-1", 2025)
    assert (second.key, second.size, second.etag, second.last_modified.day) == ("root/second.txt", 7, "etag-2", 2)
    assert len(requests) == 2
    assert requests[0].url.params["prefix"] == "root/"
    assert requests[1].url.params["continuation-token"] == "token-2"


async def test_list_objects_rejects_malformed_xml() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<ListBucketResult>")

    async with _mocked_client(handler) as client:
        with pytest.raises(S3ResponseParseError, match="Failed to parse S3 XML response"):
            _ = [item async for item in client.list_objects()]


async def test_list_objects_rejects_invalid_size() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"""
                <ListBucketResult>
                  <IsTruncated>false</IsTruncated>
                  <Contents>
                    <Key>file.txt</Key>
                    <LastModified>2025-01-01T00:00:00+00:00</LastModified>
                    <ETag>etag</ETag>
                    <Size>invalid</Size>
                  </Contents>
                </ListBucketResult>
            """,
        )

    async with _mocked_client(handler) as client:
        with pytest.raises(S3ResponseParseError, match="Invalid Size"):
            _ = [item async for item in client.list_objects()]


@pytest.mark.parametrize(
    ("headers", "message"),
    [
        ({"ETag": "etag", "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT"}, "Missing Content-Length"),
        (
            {"Content-Length": "invalid", "ETag": "etag", "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT"},
            "Invalid Content-Length",
        ),
        ({"Content-Length": "1", "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT"}, "Missing ETag"),
        ({"Content-Length": "1", "ETag": "etag"}, "Missing Last-Modified"),
    ],
)
async def test_head_object_rejects_missing_or_invalid_headers(headers: dict[str, str], message: str) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=headers)

    async with _mocked_client(handler) as client:
        with pytest.raises(S3ResponseParseError, match=message):
            await client.head_object("file.txt")


async def test_copy_object_rejects_embedded_error_in_success_response() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=(
                b"<CopyObjectResult><Error><Code>InternalError</Code>"
                b"<Message>copy failed</Message></Error></CopyObjectResult>"
            ),
        )

    async with _mocked_client(handler) as client:
        with pytest.raises(S3ResponseParseError, match="CopyObject failed: InternalError: copy failed"):
            await client.put_object_copy("source.txt", "target.txt")


async def test_delete_objects_rejects_per_object_errors() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=(
                b"<DeleteResult><Error><Key>blocked.txt</Key><Code>AccessDenied</Code>"
                b"<Message>denied</Message></Error></DeleteResult>"
            ),
        )

    async with _mocked_client(handler) as client:
        with pytest.raises(S3ResponseParseError, match="Failed to delete objects: AccessDenied: denied"):
            await client.delete_objects(["blocked.txt"])


async def test_upload_part_requires_etag_header() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    async with _mocked_client(handler) as client:
        with pytest.raises(S3ResponseParseError, match="Missing ETag"):
            await client.upload_part("large.bin", b"part", 1, "upload-1")


async def test_complete_multipart_upload_serializes_parts() -> None:
    request_body = b""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_body
        request_body = request.content
        return httpx.Response(
            200,
            content=b"<CompleteMultipartUploadResult><ETag>complete</ETag></CompleteMultipartUploadResult>",
        )

    async with _mocked_client(handler) as client:
        await client.complete_multipart_upload(
            "large.bin",
            "upload-1",
            [
                {"PartNumber": 1, "ETag": "etag-1"},
                {"PartNumber": 2, "ETag": "etag-2"},
            ],
        )

    root = ET.fromstring(request_body)  # noqa: S314
    assert [node.findtext("PartNumber") for node in root.findall("Part")] == ["1", "2"]
    assert [node.findtext("ETag") for node in root.findall("Part")] == ["etag-1", "etag-2"]


async def test_http_error_includes_empty_body_placeholder() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"")

    async with _mocked_client(handler) as client:
        with pytest.raises(S3HttpStatusError) as exc_info:
            await client.head_bucket()

    assert exc_info.value.status_code == 503
    assert exc_info.value.body == "<empty body>"
