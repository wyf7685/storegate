from pathlib import Path

from pydantic import BaseModel, SecretStr


class CosConfig(BaseModel):
    secret_id: SecretStr
    secret_key: SecretStr
    region: str
    bucket: str
    is_internal: bool = False
    max_concurrency: int = 16
    token: str | None = None
    scheme: str = "https"
    timeout: float = 30

    @classmethod
    def from_file(cls, path: str | Path) -> CosConfig:
        return cls.model_validate_json(Path(path).read_bytes())
