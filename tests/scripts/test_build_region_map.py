import importlib
import json
from pathlib import Path

import pandas as pd

build_region_map = importlib.import_module("scripts.build_region_map")


def write_csv(tmp_path: Path, values: list[str]) -> Path:
    path = tmp_path / "input.csv"
    frame = pd.DataFrame({"region": values})
    frame.to_csv(path, index=False)
    return path


def write_common_regions(tmp_path: Path, regions: list[str]) -> Path:
    path = tmp_path / "common_regions.json"
    path.write_text(json.dumps(regions), encoding="utf-8")
    return path


def write_region_map(tmp_path: Path, mapping: dict[str, str]) -> Path:
    path = tmp_path / "region_map.json"
    path.write_text(json.dumps(mapping), encoding="utf-8")
    return path


def test_build_mapping_only_targets_canonical_regions(tmp_path, monkeypatch):
    csv_path = write_csv(tmp_path, ["US", "Atlantis"])
    common_regions_path = write_common_regions(tmp_path, ["United States"])
    region_map_path = write_region_map(tmp_path, {"Existing": "United States"})

    monkeypatch.setattr(build_region_map, "_REGION_LIST_PATH", common_regions_path)
    monkeypatch.setattr(build_region_map, "_REGION_MAP_PATH", region_map_path)

    canonicals = build_region_map.load_canonical_regions()
    existing = build_region_map.load_existing_mapping(region_map_path)
    counter = build_region_map.Counter(build_region_map.iter_regions([csv_path]))
    mapping = build_region_map.build_mapping(counter, canonicals, existing, "UNKNOWN")

    for target in mapping.values():
        if target != "UNKNOWN":
            assert target in canonicals
    assert mapping.get("Atlantis") == "UNKNOWN"
