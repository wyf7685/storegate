import json

from pydantic import BaseModel, SecretStr

from app.const import CONFIG_FILE
from app.utils import SecretStrEncoder


class CosConfig(BaseModel):
    secret_id: SecretStr
    secret_key: SecretStr
    region: str
    bucket: str
    is_internal: bool


class Config(BaseModel):
    cos: CosConfig | None = None


def _load_config() -> Config:
    if not CONFIG_FILE.exists():
        return Config()
    return Config.model_validate_json(CONFIG_FILE.read_bytes())


_config: Config | None = None


def get_config() -> Config:
    global _config
    if _config is None:
        _config = _load_config()
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.write_text(json.dumps(_config.model_dump(), indent=2, cls=SecretStrEncoder), encoding="utf-8")
    return _config
