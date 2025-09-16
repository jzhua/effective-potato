"""Generate a lookup of common category names from raw CSV inputs."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Iterable

import pandas as pd

from data_pipeline.settings import DATA_DIR


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan raw CSV files and write a list of common category names.",
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="CSV files to scan for category frequencies.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.01,
        help="Minimum share of total rows (0-1) required to include a category (default: 0.01 for 1%).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DATA_DIR / "lookups" / "common_categories.json",
        help="Destination JSON file for the canonical categories list.",
    )
    return parser.parse_args()


def iter_categories(paths: Iterable[Path]) -> Iterable[str]:
    for path in paths:
        frame = pd.read_csv(path, usecols=["category"], dtype=str)
        for value in frame["category"].dropna():
            yield value.strip()


def build_lookup(categories: Counter[str], threshold: float) -> list[str]:
    if not 0 <= threshold <= 1:
        raise ValueError("threshold must be between 0 and 1")

    total = sum(categories.values())
    if total == 0:
        return []

    canonicals: list[str] = []
    for raw, count in categories.most_common():
        if (count / total) < threshold:
            break
        normalised = raw.strip()
        if not normalised:
            continue
        canonicals.append(normalised.title())
    return sorted(set(canonicals))


def main() -> None:
    args = parse_args()
    counter = Counter(iter_categories(args.inputs))
    canonical_categories = build_lookup(counter, args.threshold)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(canonical_categories, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover
    main()
