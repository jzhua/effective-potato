"""Project-wide configuration helpers and directory constants."""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_INPUT_DIR = DATA_DIR / "input"
CLEAN_OUTPUT_DIR = DATA_DIR / "clean"
REJECTED_OUTPUT_DIR = DATA_DIR / "rejected"
AGGREGATIONS_DIR = DATA_DIR / "aggregations"


def ensure_directories() -> None:
    """Create the expected data directories if they do not already exist."""
    for directory in (DEFAULT_INPUT_DIR, CLEAN_OUTPUT_DIR, REJECTED_OUTPUT_DIR, AGGREGATIONS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
