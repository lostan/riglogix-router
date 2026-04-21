import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

load_dotenv()

_CONFIG_PATH = Path(__file__).parent / "settings.yaml"

with _CONFIG_PATH.open() as f:
    settings: dict = yaml.safe_load(f)
