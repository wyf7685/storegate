from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
CONFIG_FILE = DATA_DIR / "config.json"

DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB
