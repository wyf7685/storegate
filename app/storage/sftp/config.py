from pathlib import Path, PurePosixPath
from typing import Self

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


class SFTPConfig(BaseModel):
    """Configuration for the SFTP storage backend."""

    host: str
    port: int = Field(default=22, ge=1, le=65535)
    username: str

    password: SecretStr | None = None
    client_keys: tuple[Path, ...] = ()
    passphrase: SecretStr | None = None

    known_hosts: Path | None = None
    disable_host_key_check: bool = False

    root_prefix: str = "/"
    chunk_size: int = Field(default=1024 * 1024, gt=0)
    max_channels: int = Field(default=4, gt=0)

    connect_timeout: float = Field(default=30.0, gt=0)
    login_timeout: float = Field(default=30.0, gt=0)
    io_timeout: float = Field(default=30.0, gt=0)
    close_timeout: float = Field(default=30.0, gt=0)

    keepalive_interval: float = Field(default=30.0, ge=0)
    keepalive_count_max: int = Field(default=3, gt=0)

    @field_validator("client_keys", mode="before")
    @classmethod
    def _validate_client_keys(cls, value: object) -> object:
        if value is None:
            return ()
        if isinstance(value, (str, Path)):
            value = (value,)
        if isinstance(value, (tuple, list)):
            for item in value:
                if not str(item).strip():
                    raise ValueError("client key paths must not be empty")
        return value

    @model_validator(mode="after")
    def _validate(self) -> Self:
        self.host = self.host.strip().lower()
        if not self.host:
            raise ValueError("host must not be empty")
        if "\x00" in self.host:
            raise ValueError("host must not contain NUL")

        self.username = self.username.strip()
        if not self.username:
            raise ValueError("username must not be empty")
        if "\x00" in self.username:
            raise ValueError("username must not contain NUL")

        if self.password is None and not self.client_keys:
            raise ValueError("password or at least one client key is required")
        if self.passphrase is not None and not self.client_keys:
            raise ValueError("passphrase requires at least one client key")
        if self.known_hosts is not None and self.disable_host_key_check:
            raise ValueError("known_hosts and disable_host_key_check are mutually exclusive")

        prefix = self.root_prefix.strip()
        if "\x00" in prefix:
            raise ValueError("root_prefix must not contain NUL")
        if not prefix.startswith("/"):
            raise ValueError("root_prefix must be an absolute POSIX path")

        parts = prefix.split("/")
        if ".." in parts:
            raise ValueError("root_prefix must not contain '..' segments")
        normalized_parts = [part for part in parts if part not in {"", "."}]
        self.root_prefix = PurePosixPath("/", *normalized_parts).as_posix()
        return self

    @classmethod
    def from_file(cls, path: str | Path) -> SFTPConfig:
        return cls.model_validate_json(Path(path).read_bytes())
