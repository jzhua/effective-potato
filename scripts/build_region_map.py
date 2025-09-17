"""Generate or update a mapping from raw region values to canonical regions."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Iterable

import pandas as pd

from data_pipeline.settings import DATA_DIR

_REGION_LIST_PATH = DATA_DIR / "lookups" / "common_regions.json"
_REGION_MAP_PATH = DATA_DIR / "lookups" / "region_map.json"

# Removed _REGION_SYNONYMS - now all mappings are in region_map.json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan raw CSV files and update the region mapping table.",
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="CSV files to scan for region values.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_REGION_MAP_PATH,
        help="Destination JSON file for the raw-to-canonical mapping.",
    )
    parser.add_argument(
        "--unknown-label",
        default="UNKNOWN",
        help="Label to assign when the script cannot determine a canonical region (default: UNKNOWN).",
    )
    return parser.parse_args()


def iter_regions(paths: Iterable[Path]) -> Iterable[str]:
    for path in paths:
        frame = pd.read_csv(path, usecols=["region"], dtype=str)
        for value in frame["region"].dropna():
            yield value.strip()


def load_canonical_regions() -> list[str]:
    with _REGION_LIST_PATH.open(encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, list):
        raise ValueError("common_regions.json must contain a JSON array of canonical regions")
    return sorted({str(region).strip() for region in raw if region})


def load_existing_mapping(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("region_map.json must contain a JSON object")
    mapping: dict[str, str] = {}
    for key, value in raw.items():
        cleaned_key = str(key).strip()
        if not cleaned_key:
            continue
        mapping[cleaned_key] = str(value).strip()
    return mapping


def levenshtein(left: str, right: str, *, max_distance: int = 2) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    if abs(len(left) - len(right)) > max_distance:
        return max_distance + 1

    previous = list(range(len(right) + 1))
    for i, l_char in enumerate(left, start=1):
        current = [i]
        min_distance = current[0]
        for j, r_char in enumerate(right, start=1):
            cost = 0 if l_char == r_char else 1
            insert_cost = current[j - 1] + 1
            delete_cost = previous[j] + 1
            replace_cost = previous[j - 1] + cost
            best_cost = min(insert_cost, delete_cost, replace_cost)
            current.append(best_cost)
            if best_cost < min_distance:
                min_distance = best_cost
        if min_distance > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def guess_region(raw_value: str, canonicals: list[str]) -> str | None:
    if not raw_value:
        return None
    lowered = raw_value.casefold()
    canonical_lookup = {value.casefold(): value for value in canonicals}
    if lowered in canonical_lookup:
        return canonical_lookup[lowered]

    best_match: tuple[str, int] | None = None
    for candidate in canonicals:
        distance = levenshtein(lowered, candidate.casefold())
        if distance <= 2:
            if best_match is None or distance < best_match[1]:
                best_match = (candidate, distance)
                if distance == 0:
                    break
    if best_match is not None:
        return best_match[0]
    return None


def build_mapping(counter: Counter[str], canonicals: list[str], existing: dict[str, str], unknown_label: str) -> dict[str, str]:
    mapping = dict(existing)
    canonical_set = set(canonicals)
    canonical_lookup = {value.casefold(): value for value in canonicals}

    for raw_value, _ in counter.most_common():
        cleaned = raw_value.strip()
        if not cleaned or cleaned in mapping:
            continue

        suggestion = guess_region(cleaned, canonicals)
        if suggestion is None:
            mapping[cleaned] = unknown_label
        else:
            mapping[cleaned] = suggestion

    # Ensure canonical names map to themselves
    for canonical in canonical_set:
        mapping.setdefault(canonical, canonical)

    return dict(sorted(mapping.items(), key=lambda item: item[0].casefold()))


def main() -> None:
    args = parse_args()
    canonical_regions = load_canonical_regions()
    existing = load_existing_mapping(args.output)
    counter = Counter(iter_regions(args.inputs))
    mapping = build_mapping(counter, canonical_regions, existing, args.unknown_label)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(mapping, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover
    main()
