from __future__ import annotations

import dataclasses
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Literal, Self
from urllib.parse import urlparse

from pydantic import BaseModel, Field, SecretStr, model_validator

type AuthMode = Literal["basic", "bearer", "anonymous"]


class DavConfig(BaseModel):
    """Configuration for a WebDAV client storage backend.

    Supports any standard WebDAV server (Nextcloud, ownCloud, Apache mod_dav,
    wsgidav, ...) via ``base_url``. Authentication is pluggable via
    ``auth_mode``: HTTP Basic, Bearer token, or anonymous.
    """

    base_url: str
    auth_mode: AuthMode = "basic"
    username: str | None = None
    password: SecretStr | None = None
    token: SecretStr | None = None
    # Sub-path prefix under ``base_url`` for isolating multiple storage
    # instances on the same WebDAV account (analogous to an S3 bucket prefix).
    root_prefix: str = ""
    verify_ssl: bool = True
    # Custom CA bundle (PEM) for self-signed deployments (e.g. on-prem Nextcloud).
    ca_cert_path: str | None = None
    timeout: float = Field(default=30.0, gt=0)
    max_concurrency: int = Field(default=8, gt=0)
    http2: bool = True
    chunk_size: int = Field(default=4 * 1024 * 1024, gt=0)

    @model_validator(mode="after")
    def _validate(self) -> Self:
        parsed = urlparse(self.base_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"base_url must use http or https scheme, got {parsed.scheme!r}")
        if not parsed.netloc:
            raise ValueError("base_url must include a host")

        # Normalize: no trailing slash on base_url; root_prefix is "" or "/seg".
        self.base_url = self.base_url.rstrip("/")
        prefix = self.root_prefix.strip()
        if "\x00" in prefix:
            raise ValueError("root_prefix must not contain NUL")
        if not prefix:
            self.root_prefix = ""
        else:
            if not prefix.startswith("/"):
                prefix = "/" + prefix
            raw_path = PurePosixPath(prefix)
            if ".." in raw_path.parts:
                raise ValueError("root_prefix must not contain '..' segments")
            normalized = PurePosixPath("/", *[part for part in raw_path.parts[1:] if part != "."]).as_posix()
            self.root_prefix = "" if normalized == "/" else normalized

        match self.auth_mode:
            case "basic":
                if not self.username or self.password is None:
                    raise ValueError("basic auth requires both username and password")
            case "bearer":
                if self.token is None:
                    raise ValueError("bearer auth requires a token")
            case "anonymous":
                pass

        return self

    @classmethod
    def from_file(cls, path: str | Path) -> DavConfig:
        return cls.model_validate_json(Path(path).read_bytes())


@dataclasses.dataclass(frozen=True, slots=True)
class DavResource:
    """A single ``<D:response>`` element from a WebDAV PROPFIND multistatus body."""

    href: str
    resource_types: tuple[str, ...]
    content_length: int | None
    last_modified: datetime | None
    creation_date: datetime | None
    display_name: str | None

    @property
    def is_collection(self) -> bool:
        return "{DAV:}collection" in self.resource_types
