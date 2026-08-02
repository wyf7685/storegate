"""AWS Signature Version 4 (SigV4) implementation for S3 requests.

Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

The signing process:
1. Build a *canonical request* from the HTTP method, URI, query string,
   headers, and payload hash.
2. Build a *string to sign* from the algorithm id, timestamp, credential
   scope, and SHA-256 of the canonical request.
3. Derive a *signing key* via four HMAC-SHA256 rounds
   (secret → date → region → service → ``aws4_request``).
4. Sign the string-to-sign with the signing key and assemble the
   ``Authorization`` header.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from datetime import datetime
from urllib.parse import quote

_SAFE_CHARS = "-_.~"


def _uri_encode(value: str) -> str:
    """URI-encode a string per SigV4 rules (unreserved chars only)."""
    return quote(str(value), safe=_SAFE_CHARS)


def _encode_query_kv(data: Mapping[str, str]) -> list[tuple[str, str]]:
    """Encode and sort query parameters for the canonical query string."""
    encoded: list[tuple[str, str]] = []
    for key, value in data.items():
        encoded.append((_uri_encode(key), _uri_encode(value)))
    encoded.sort(key=lambda item: item[0])
    return encoded


def _format_query_kv(encoded: list[tuple[str, str]]) -> str:
    return "&".join(f"{key}={value}" for key, value in encoded)


def _canonical_headers(headers: Mapping[str, str]) -> tuple[str, str]:
    """Return ``(canonical_headers, signed_headers)``.

    Header names are lowercased; values are trimmed and runs of internal
    whitespace collapsed to a single space, per the SigV4 spec. Entries are
    sorted by header name.
    """
    normalized: list[tuple[str, str]] = []
    for key, value in headers.items():
        lower_key = key.lower()
        norm_value = " ".join(value.strip().split())
        normalized.append((lower_key, norm_value))
    normalized.sort(key=lambda item: item[0])
    canonical = "".join(f"{k}:{v}\n" for k, v in normalized)
    signed = ";".join(k for k, _ in normalized)
    return canonical, signed


class AWSSigV4Signer:
    """Signs S3 requests with AWS Signature Version 4."""

    _SERVICE = "s3"

    def __init__(self, access_key_id: str, secret_access_key: str, region: str) -> None:
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region

    @staticmethod
    def _hmac_sha256(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    @staticmethod
    def _sha256_hex(data: str) -> str:
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def _derive_signing_key(self, date_stamp: str) -> bytes:
        k_date = self._hmac_sha256(("AWS4" + self._secret_access_key).encode("utf-8"), date_stamp)
        k_region = self._hmac_sha256(k_date, self._region)
        k_service = self._hmac_sha256(k_region, self._SERVICE)
        return self._hmac_sha256(k_service, "aws4_request")

    def build_authorization(
        self,
        *,
        method: str,
        canonical_uri: str,
        params: Mapping[str, str],
        headers: Mapping[str, str],
        payload_hash: str,
        now: datetime,
    ) -> str:
        """Build the ``Authorization`` header value for a signed request.

        Args:
            method: HTTP method (e.g. ``"GET"``).
            canonical_uri: The already URI-encoded request path. Must match
                the path actually sent on the wire (including any bucket
                prefix for path-style addressing).
            params: Query parameters (values already as strings).
            headers: All headers to sign, including ``host``, ``x-amz-date``
                and ``x-amz-content-sha256``.
            payload_hash: The value of ``x-amz-content-sha256`` — typically
                ``"UNSIGNED-PAYLOAD"`` or a lowercase SHA-256 hex digest.
            now: Request timestamp (UTC). Drives both the date stamp and the
                ``x-amz-date`` header.
        """
        canonical_query = _format_query_kv(_encode_query_kv(params))
        canonical_headers, signed_headers = _canonical_headers(headers)
        canonical_request = (
            f"{method.upper()}\n{canonical_uri}\n{canonical_query}\n"
            f"{canonical_headers}\n{signed_headers}\n{payload_hash}"
        )

        date_stamp = now.strftime("%Y%m%d")
        time_stamp = now.strftime("%Y%m%dT%H%M%SZ")
        credential_scope = f"{date_stamp}/{self._region}/{self._SERVICE}/aws4_request"
        string_to_sign = f"AWS4-HMAC-SHA256\n{time_stamp}\n{credential_scope}\n{self._sha256_hex(canonical_request)}"

        signing_key = self._derive_signing_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        return (
            f"AWS4-HMAC-SHA256 "
            f"Credential={self._access_key_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
