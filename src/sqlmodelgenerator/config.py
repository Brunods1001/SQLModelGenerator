import os
from pathlib import Path
from typing import Any
import tomllib


ROOT_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
CONFIG_PATH = ROOT_DIR / "config.toml"

def get_env() -> dict[str, Any]:
    with open(CONFIG_PATH, "rb") as f:
        data = tomllib.load(f)
        return data


