"""Regenerate lookup artefacts and optionally run validation tests."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regenerate lookup tables and run validation tests.",
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="CSV files to scan when building category and region lookups.",
    )
    parser.add_argument(
        "--category-threshold",
        type=float,
        default=0.01,
        help="Minimum share of rows required to keep a category (default: 0.01).",
    )
    parser.add_argument(
        "--region-threshold",
        type=float,
        default=0.01,
        help="Minimum share of rows required to keep a region (default: 0.01).",
    )
    parser.add_argument(
        "--unknown-label",
        default="UNKNOWN",
        help="Label for unmapped regions when generating the region map (default: UNKNOWN).",
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip running pytest after regenerating the lookups.",
    )
    parser.add_argument(
        "--pytest-args",
        nargs=argparse.REMAINDER,
        default=None,
        help="Additional arguments to pass to pytest (must come after '--').",
    )
    return parser.parse_args()


def run_command(command: list[str]) -> None:
    subprocess.run(command, check=True)


def build_category_lookup(inputs: Iterable[Path], threshold: float) -> None:
    cmd = [
        sys.executable,
        "-m",
        "scripts.build_category_lookup",
        *(str(path) for path in inputs),
        "--threshold",
        str(threshold),
    ]
    run_command(cmd)


def build_region_lookup(inputs: Iterable[Path], threshold: float) -> None:
    cmd = [
        sys.executable,
        "-m",
        "scripts.build_region_lookup",
        *(str(path) for path in inputs),
        "--threshold",
        str(threshold),
    ]
    run_command(cmd)


def build_region_map(inputs: Iterable[Path], unknown_label: str) -> None:
    cmd = [
        sys.executable,
        "-m",
        "scripts.build_region_map",
        *(str(path) for path in inputs),
        "--unknown-label",
        unknown_label,
    ]
    run_command(cmd)


def run_tests(pytest_args: list[str] | None) -> None:
    command = [sys.executable, "-m", "pytest"]
    if pytest_args:
        command.extend(pytest_args)
    run_command(command)


def main() -> None:
    args = parse_args()

    build_category_lookup(args.inputs, args.category_threshold)
    build_region_lookup(args.inputs, args.region_threshold)
    build_region_map(args.inputs, args.unknown_label)

    if not args.skip_tests:
        run_tests(args.pytest_args)


if __name__ == "__main__":  # pragma: no cover
    main()
