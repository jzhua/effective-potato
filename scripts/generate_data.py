"""CLI helper to generate synthetic ecommerce data into ``data/input``."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from data_pipeline.generation import EcommerceDataGenerator
from data_pipeline.settings import DEFAULT_INPUT_DIR, ensure_directories


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate raw ecommerce CSV data.")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_INPUT_DIR / "raw_ecommerce_data.csv",
        help="Destination CSV file",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help=(
            "If set, emit a 'clean' version of the dataset (useful for testing "
            "downstream stages)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    
    args = parse_args()
    ensure_directories()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting data generation with {args.rows} rows")
    logger.info(f"Output file: {args.output}")
    logger.info(f"Clean data mode: {args.clean}")

    generator = EcommerceDataGenerator(clean_data=args.clean)
    generator.generate_csv(str(args.output), num_rows=args.rows)
    
    logger.info("Data generation completed successfully")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
