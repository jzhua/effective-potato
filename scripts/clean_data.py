"""Clean the raw CSV export into an analytics-ready parquet dataset."""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from data_pipeline.cleaning import CleanConfig, clean_csv_to_parquet
from data_pipeline.settings import CLEAN_OUTPUT_DIR, DEFAULT_INPUT_DIR, ensure_directories


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean a raw ecommerce CSV file into parquet format.")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_DIR / "raw_ecommerce_data.csv",
        help="Path to the raw CSV file produced by the generator.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=CLEAN_OUTPUT_DIR / "raw_ecommerce_data_clean.parquet",
        help="Destination parquet file for the cleaned dataset.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Number of rows to process per chunk when cleaning large files.",
    )
    parser.add_argument(
        "--keep-zero-quantity",
        action="store_true",
        help="Keep orders where the quantity is zero after cleaning.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )
    parser.add_argument(
        "--no-save-rejected",
        action="store_true",
        help="Don't save rejected rows to a separate file",
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
    if not args.input.exists():
        logger.error(f"Input file does not exist: {args.input}")
        return
    
    # Log configuration
    logger.info("Starting CSV cleaning process")
    logger.info(f"Input file: {args.input}")
    logger.info(f"Output file: {args.output}")
    logger.info(f"Chunk size: {args.chunk_size:,} rows")
    logger.info(f"Drop zero quantity: {not args.keep_zero_quantity}")
    logger.info(f"Save rejected rows: {not args.no_save_rejected}")
    
    # Get input file size for progress tracking
    file_size_mb = args.input.stat().st_size / (1024 * 1024)
    logger.info(f"Input file size: {file_size_mb:.1f} MB")
    
    ensure_directories()
    config = CleanConfig(
        chunk_size=args.chunk_size, 
        drop_zero_quantity=not args.keep_zero_quantity,
        save_rejected_rows=not args.no_save_rejected
    )
    
    start_time = time.time()
    try:
        output_path, rejected_path = clean_csv_to_parquet(args.input, args.output, config=config)
        elapsed = time.time() - start_time
        
        # Log completion stats
        if output_path.exists():
            output_size_mb = output_path.stat().st_size / (1024 * 1024)
            compression_ratio = (1 - output_size_mb / file_size_mb) * 100 if file_size_mb > 0 else 0
            
            logger.info(f"Cleaning completed successfully in {elapsed:.1f} seconds")
            logger.info(f"Clean output file: {output_path}")
            logger.info(f"Clean output size: {output_size_mb:.1f} MB")
            logger.info(f"Compression ratio: {compression_ratio:.1f}%")
            logger.info(f"Processing rate: {file_size_mb / elapsed:.1f} MB/s")
            
            # Log rejected file stats
            if rejected_path and rejected_path.exists():
                rejected_size_mb = rejected_path.stat().st_size / (1024 * 1024)
                logger.info(f"Rejected rows file: {rejected_path}")
                logger.info(f"Rejected rows size: {rejected_size_mb:.1f} MB")
            elif config.save_rejected_rows:
                logger.info("No rejected rows were generated")
        else:
            logger.warning("Output file was not created")
            
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Cleaning failed after {elapsed:.1f} seconds: {e}")
        raise


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
