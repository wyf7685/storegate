import codecs
from pathlib import Path, PurePosixPath
from typing import Self

from pydantic import BaseModel, Field, SecretStr, model_validator


class FTPConfig(BaseModel):
    """Configuration for a plain FTP storage backend."""

    host: str
    port: int = Field(default=21, ge=1, le=65535)
    username: str = "anonymous"
    password: SecretStr = SecretStr("anon@")
    root_prefix: str = "/"
    chunk_size: int = Field(default=1024 * 1024, gt=0)
    encoding: str = "utf-8"
    timeout: float = Field(default=30.0, gt=0)

    @model_validator(mode="after")
    def _validate(self) -> Self:
        self.host = self.host.strip().lower()
        if not self.host:
            raise ValueError("host must not be empty")

        self.username = self.username.strip()
        if not self.username:
            raise ValueError("username must not be empty")

        try:
            codecs.lookup(self.encoding)
        except LookupError as exc:
            raise ValueError(f"Unknown encoding: {self.encoding!r}") from exc

        prefix = self.root_prefix.strip()
        if "\x00" in prefix:
            raise ValueError("root_prefix must not contain NUL")

        raw_path = PurePosixPath(prefix)
        if not raw_path.is_absolute() or raw_path.anchor != "/":
            raise ValueError("root_prefix must be an absolute POSIX path")
        if ".." in raw_path.parts:
            raise ValueError("root_prefix must not contain '..' segments")

        normalized = PurePosixPath("/", *raw_path.parts[1:]).as_posix()
        self.root_prefix = normalized
        return self

    @classmethod
    def from_file(cls, path: str | Path) -> FTPConfig:
        return cls.model_validate_json(Path(path).read_bytes())
