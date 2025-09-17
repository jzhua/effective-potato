"""Compute all derived aggregations needed by the dashboard."""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from data_pipeline.aggregations import build_all_aggregations
from data_pipeline.settings import AGGREGATIONS_DIR, CLEAN_OUTPUT_DIR, ensure_directories


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build analytics aggregations from the cleaned dataset.")
    parser.add_argument(
        "--cleaned",
        type=Path,
        default=CLEAN_OUTPUT_DIR / "raw_ecommerce_data_clean.parquet",
        help="Path to the cleaned parquet file produced by the cleaning stage.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=AGGREGATIONS_DIR,
        help="Destination directory for the aggregation parquet files.",
    )
    parser.add_argument(
        "--top-products-by-category",
        type=int,
        default=5,
        help="Number of top products per category to include in rankings (default: 5).",
    )
    parser.add_argument(
        "--anomaly-limit",
        type=int,
        default=5,
        help="Number of anomaly records to identify (default: 5).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    
    # Validate input file
    if not args.cleaned.exists():
        logger.error(f"Cleaned data file does not exist: {args.cleaned}")
        return
    
    # Log configuration
    logger.info("Starting aggregation build process")
    logger.info(f"Input file: {args.cleaned}")
    logger.info(f"Output directory: {args.output}")
    logger.info(f"Top products by category limit: {args.top_products_by_category}")
    logger.info(f"Anomaly records limit: {args.anomaly_limit}")
    
    # Get input file size
    file_size_mb = args.cleaned.stat().st_size / (1024 * 1024)
    logger.info(f"Input file size: {file_size_mb:.1f} MB")
    
    ensure_directories()
    
    start_time = time.time()
    try:
        result_files = build_all_aggregations(
            args.cleaned, 
            args.output, 
            top_products_limit=args.top_products_by_category,
            anomaly_limit=args.anomaly_limit
        )
        elapsed = time.time() - start_time
        
        # Log completion stats
        total_output_size = sum(f.stat().st_size for f in result_files.values()) / (1024 * 1024)
        
        logger.info(f"Aggregations completed successfully in {elapsed:.1f} seconds")
        logger.info(f"Generated {len(result_files)} aggregation files")
        logger.info(f"Total output size: {total_output_size:.1f} MB")
        logger.info(f"Processing rate: {file_size_mb / elapsed:.1f} MB/s")
        
        # List generated files
        logger.info("Generated aggregation files:")
        for name, path in result_files.items():
            size_kb = path.stat().st_size / 1024
            logger.info(f"  {name}: {size_kb:.1f} KB -> {path}")
            
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Aggregation build failed after {elapsed:.1f} seconds: {e}")
        raise


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
