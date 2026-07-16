"""SigV4 signer correctness against AWS-published test vectors.

These are pure unit tests — no network, no credentials required. The
reference vector is the Get Object example from the AWS SigV4 documentation:
https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
"""

from datetime import UTC, datetime

from app.storage.s3.client.auth import AWSSigV4Signer

# AWS documented test credentials (well-known example values, not real keys).
_AKID = "AKIAIOSFODNN7EXAMPLE"
_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
# SHA-256 of an empty body — the documented payload hash for this example.
_EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def _make_signer(region: str = "us-east-1") -> AWSSigV4Signer:
    return AWSSigV4Signer(access_key_id=_AKID, secret_access_key=_SECRET, region=region)


class TestSigV4Vectors:
    def test_get_object_with_range(self) -> None:
        """AWS docs example: GET object with a Range header.

        Reproduces the documented Authorization header byte-for-byte, which
        exercises canonical-request construction, canonical-headers sorting,
        the four-round signing-key derivation, and final HMAC.
        """
        signer = _make_signer()
        now = datetime(2013, 5, 24, 0, 0, 0, tzinfo=UTC)
        auth = signer.build_authorization(
            method="GET",
            canonical_uri="/test.txt",
            params={},
            headers={
                "Host": "examplebucket.s3.amazonaws.com",
                "Range": "bytes=0-9",
                "x-amz-content-sha256": _EMPTY_SHA256,
                "x-amz-date": "20130524T000000Z",
            },
            payload_hash=_EMPTY_SHA256,
            now=now,
        )
        assert auth == (
            "AWS4-HMAC-SHA256 "
            "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        )

    def test_header_order_independent(self) -> None:
        """Canonical-headers sorting makes header insertion order irrelevant."""
        signer = _make_signer()
        now = datetime(2013, 5, 24, 0, 0, 0, tzinfo=UTC)
        headers = {
            "x-amz-date": "20130524T000000Z",
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": _EMPTY_SHA256,
            "Range": "bytes=0-9",
        }
        auth = signer.build_authorization(
            method="GET",
            canonical_uri="/test.txt",
            params={},
            headers=headers,
            payload_hash=_EMPTY_SHA256,
            now=now,
        )
        assert "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date" in auth

    def test_region_affects_signature(self) -> None:
        """A different region produces a different signature (credential scope isolation)."""
        now = datetime(2013, 5, 24, 0, 0, 0, tzinfo=UTC)
        headers = {
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": _EMPTY_SHA256,
            "x-amz-date": "20130524T000000Z",
        }
        sig_us = _make_signer("us-east-1").build_authorization(
            method="GET",
            canonical_uri="/test.txt",
            params={},
            headers=headers,
            payload_hash=_EMPTY_SHA256,
            now=now,
        )
        sig_eu = _make_signer("eu-west-1").build_authorization(
            method="GET",
            canonical_uri="/test.txt",
            params={},
            headers=headers,
            payload_hash=_EMPTY_SHA256,
            now=now,
        )
        assert sig_us != sig_eu
        assert "us-east-1/s3/aws4_request" in sig_us
        assert "eu-west-1/s3/aws4_request" in sig_eu

    def test_unsigned_payload_changes_signature(self) -> None:
        """UNSIGNED-PAYLOAD vs a real hash yield different signatures."""
        signer = _make_signer()
        now = datetime(2013, 5, 24, 0, 0, 0, tzinfo=UTC)
        headers = {
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": _EMPTY_SHA256,
            "x-amz-date": "20130524T000000Z",
        }
        signed = signer.build_authorization(
            method="GET",
            canonical_uri="/test.txt",
            params={},
            headers=headers,
            payload_hash=_EMPTY_SHA256,
            now=now,
        )
        headers_unsigned = {
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
            "x-amz-date": "20130524T000000Z",
        }
        unsigned = signer.build_authorization(
            method="GET",
            canonical_uri="/test.txt",
            params={},
            headers=headers_unsigned,
            payload_hash="UNSIGNED-PAYLOAD",
            now=now,
        )
        assert signed != unsigned
