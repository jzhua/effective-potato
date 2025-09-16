"""Utilities for cleaning the raw ecommerce CSV exports."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, MutableSet
import logging
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_pipeline.settings import CLEAN_OUTPUT_DIR, REJECTED_OUTPUT_DIR, ensure_directories

# Common placeholder values that indicate a missing field.
_NULLISH = {"", "null", "n/a", "na", "none", "-", "missing"}
_NULLISH_REPLACEMENTS = {value: "" for value in _NULLISH}

# Canonical category names keyed by their normalized raw form.
_CATEGORY_ALIASES: Mapping[str, str] = {
    "electronics": "Electronics",
    "consumer electronics": "Electronics",
    "tech": "Electronics",
    "clothing": "Clothing",
    "fashion": "Clothing",
    "apparel": "Clothing",
    "wearables": "Clothing",
    "sports": "Sports",
    "fitness": "Sports",
    "outdoor": "Sports",
    "athletic": "Sports",
    "home & garden": "Home & Garden",
    "home": "Home & Garden",
    "garden": "Home & Garden",
    "home improvement": "Home & Garden",
    "furniture": "Home & Garden",
    "books": "Books",
    "literature": "Books",
    "education": "Books",
    "media": "Books",
    "health": "Health & Beauty",
    "beauty": "Health & Beauty",
    "personal care": "Health & Beauty",
    "wellness": "Health & Beauty",
    "automotive": "Automotive",
    "auto": "Automotive",
    "car accessories": "Automotive",
    "vehicle": "Automotive",
    "transportation": "Automotive",
    "toys": "Toys & Games",
    "games": "Toys & Games",
    "kids": "Toys & Games",
    "entertainment": "Toys & Games",
    "kitchen": "Kitchen",
    "appliances": "Kitchen",
    "cooking": "Kitchen",
    "dining": "Kitchen",
    "office": "Office",
    "business": "Office",
    "supplies": "Office",
    "workspace": "Office",
}

_REGION_ALIASES: Mapping[str, str] = {
    "north america": "North America",
    "n america": "North America",
    "north ameica": "North America",
    "na": "North America",
    "united states": "North America",
    "us": "North America",
    "usa": "North America",
    "canada": "North America",
    "europe": "Europe",
    "eurpoe": "Europe",
    "eu": "Europe",
    "united kingdom": "Europe",
    "uk": "Europe",
    "germany": "Europe",
    "france": "Europe",
    "spain": "Europe",
    "asia": "Asia",
    "aisa": "Asia",
    "china": "Asia",
    "japan": "Asia",
    "india": "Asia",
    "south korea": "Asia",
    "southeast asia": "Asia",
    "south america": "South America",
    "s america": "South America",
    "latin america": "South America",
    "brazil": "South America",
    "argentina": "South America",
    "chile": "South America",
    "australia": "Oceania",
    "austrailia": "Oceania",
    "oceania": "Oceania",
    "new zealand": "Oceania",
    "au": "Oceania",
    "africa": "Africa",
    "middle east": "Middle East & Africa",
    "south africa": "Africa",
    "nigeria": "Africa",
    "egypt": "Africa",
    "eastern europe": "Europe",
    "western europe": "Europe",
    "central america": "Central America",
    "caribbean": "Central America",
    "scandinavia": "Europe",
    "nordic": "Europe",
}


@dataclass
class CleanConfig:
    """Configuration for the cleaning pipeline."""

    chunk_size: int = 100_000
    drop_zero_quantity: bool = True
    save_rejected_rows: bool = True


def _normalise_string(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
    )


def _map_alias(series: pd.Series, mapping: Mapping[str, str], default: str) -> pd.Series:
    normalised = _normalise_string(series).replace(_NULLISH_REPLACEMENTS)
    lowered = normalised.str.lower()
    mapped = lowered.map(mapping)
    # Fill with the original title-cased value when we do not have a mapping.
    fallback = normalised.replace({"": default})
    return mapped.fillna(fallback.str.title())


def _clean_customer_email(series: pd.Series) -> pd.Series:
    normalised = _normalise_string(series).replace(_NULLISH_REPLACEMENTS)
    emails = normalised.str.lower()
    valid_mask = emails.str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", na=False)
    return emails.where(valid_mask, other=pd.NA)


def _clean_numeric(series: pd.Series, dtype: str = "float") -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if dtype == "int":
        return numeric.fillna(0).clip(lower=0).round().astype("Int64")
    return numeric.fillna(0.0)


def _clean_discount(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return numeric.clip(lower=0.0, upper=1.0)


def _clean_chunk(frame: pd.DataFrame, seen_order_ids: MutableSet[str], config: CleanConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean a data chunk and return both cleaned data and rejected rows."""
    # Make a shallow copy so that we do not mutate the original chunk returned by pandas.
    data = frame.copy()
    rejected_rows = []
    
    # Add rejection tracking
    def _reject_rows(mask, reason):
        if config.save_rejected_rows:
            rejected = data[~mask].copy()
            rejected["rejection_reason"] = reason
            rejected_rows.append(rejected)
        return data[mask]

    # Step 1: Normalise order identifiers and drop rows without a valid ID or product.
    data["order_id"] = _normalise_string(data.get("order_id"))
    data["product_name"] = _normalise_string(data.get("product_name"))
    
    # TODO: Improve validation logic - add more sophisticated checks:
    # - Order ID format validation (e.g., must match specific pattern)
    # - Product name length limits
    # - Category whitelist validation
    # - Price range validation (e.g., $0.01 - $10,000)
    # - Quantity limits (e.g., 1-1000)
    # - Region validation against known list
    # - Email format validation improvements
    # - Date range validation (e.g., within last 10 years)
    
    valid_ids_mask = (data["order_id"] != "") & (data["product_name"] != "")
    data = _reject_rows(valid_ids_mask, "missing_order_id_or_product")

    if data.empty:
        rejected_df = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()
        return data, rejected_df

    # Step 2: Remove duplicates both within the chunk and across chunks seen so far.
    before_dedup = len(data)
    data = data.drop_duplicates(subset="order_id", keep="first")
    
    # Track cross-chunk duplicates
    duplicate_mask = ~data["order_id"].isin(seen_order_ids)
    data = _reject_rows(duplicate_mask, "duplicate_order_id")

    # Step 3: Standardise categorical features.
    data["category"] = _map_alias(data.get("category"), _CATEGORY_ALIASES, "Misc")
    data["region"] = _map_alias(data.get("region"), _REGION_ALIASES, "Other")

    # Step 4: Clean numeric fields and validate ranges.
    data["quantity"] = _clean_numeric(data.get("quantity"), dtype="int")
    data["unit_price"] = _clean_numeric(data.get("unit_price"))
    data["discount_percent"] = _clean_discount(data.get("discount_percent"))
    
    # Basic validation: negative prices or extreme values
    valid_price_mask = (data["unit_price"] > 0) & (data["unit_price"] < 50000)  # $0-$50k range
    data = _reject_rows(valid_price_mask, "invalid_unit_price")
    
    # Validate discount range (already clamped but check for suspicious values)
    valid_discount_mask = data["discount_percent"] <= 0.95  # Max 95% discount
    data = _reject_rows(valid_discount_mask, "excessive_discount")

    # Step 5: Drop zero quantity if configured
    if config.drop_zero_quantity:
        zero_qty_mask = data["quantity"] > 0
        data = _reject_rows(zero_qty_mask, "zero_quantity")

    # Step 6: Parse dates, validate, and convert to Unix timestamps.
    sale_dates = pd.to_datetime(
        _normalise_string(data.get("sale_date")).replace(_NULLISH_REPLACEMENTS),
        errors="coerce",
    )
    data["sale_date"] = sale_dates

    # Reject rows with invalid dates
    valid_date_mask = data["sale_date"].notna()
    data = _reject_rows(valid_date_mask, "invalid_sale_date")

    # Additional date validation: reasonable date range (last 20 years to 1 year in future)
    if not data.empty:
        now = pd.Timestamp.now()
        min_date = now - pd.DateOffset(years=20)
        max_date = now + pd.DateOffset(years=1)
        
        date_range_mask = (data["sale_date"] >= min_date) & (data["sale_date"] <= max_date)
        data = _reject_rows(date_range_mask, "sale_date_out_of_range")

    if not data.empty:
        unix_sale_dates = (data["sale_date"].astype("int64") // 1_000_000_000).astype("Int64")
        data.loc[:, "sale_date"] = unix_sale_dates

    if data.empty:
        rejected_df = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()
        return data, rejected_df

    # Step 7: Clean emails and recompute revenue from the cleaned figures.
    data["customer_email"] = _clean_customer_email(data.get("customer_email"))
    data["revenue"] = (data["unit_price"] * data["quantity"] * (1 - data["discount_percent"]))
    data["revenue"] = data["revenue"].round(2)
    
    # Validate calculated revenue
    valid_revenue_mask = (data["revenue"] >= 0) & (data["revenue"] < 1000000)  # $0-$1M per order
    data = _reject_rows(valid_revenue_mask, "invalid_calculated_revenue")

    # Update the tracker with order IDs that made it through the filters.
    seen_order_ids.update(data["order_id"].tolist())

    # Reorder columns for consistency before persisting.
    columns = [
        "order_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "discount_percent",
        "region",
        "sale_date",
        "customer_email",
        "revenue",
    ]
    
    cleaned_data = data[columns] if not data.empty else data
    rejected_df = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()
    
    return cleaned_data, rejected_df


def clean_csv_to_parquet(
    input_csv: Path,
    output_parquet: Path | None = None,
    *,
    config: CleanConfig | None = None,
) -> tuple[Path, Path | None]:
    """Clean a raw CSV file and emit a parquet dataset.

    Parameters
    ----------
    input_csv:
        Path to the raw CSV file to clean.
    output_parquet:
        Destination parquet file. When omitted we derive it from the input
        name and place it in ``data/clean``.
    config:
        Optional :class:`CleanConfig` with advanced settings.
        
    Returns
    -------
    tuple[Path, Path | None]:
        Tuple of (clean_output_path, rejected_output_path).
        rejected_output_path is None if save_rejected_rows is False.
    """
    logger = logging.getLogger(__name__)
    
    ensure_directories()
    cfg = config or CleanConfig()

    if output_parquet is None:
        output_parquet = CLEAN_OUTPUT_DIR / f"{input_csv.stem}_clean.parquet"
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    
    # Setup rejected rows output
    rejected_parquet = None
    if cfg.save_rejected_rows:
        rejected_parquet = REJECTED_OUTPUT_DIR / f"{input_csv.stem}_rejected.parquet"
        rejected_parquet.parent.mkdir(parents=True, exist_ok=True)

    seen_order_ids: set[str] = set()
    writer: pq.ParquetWriter | None = None
    rejected_writer: pq.ParquetWriter | None = None
    
    # Progress tracking
    total_input_rows = 0
    total_output_rows = 0
    total_rejected_rows = 0
    chunks_processed = 0
    start_time = time.time()
    
    logger.info(f"Starting to process CSV file: {input_csv}")
    logger.info(f"Chunk size: {cfg.chunk_size:,} rows")
    logger.info(f"Save rejected rows: {cfg.save_rejected_rows}")
    if rejected_parquet:
        logger.info(f"Rejected rows will be saved to: {rejected_parquet}")

    try:
        csv_reader = pd.read_csv(input_csv, chunksize=cfg.chunk_size)
        
        for chunk_num, chunk in enumerate(csv_reader, 1):
            chunk_start = time.time()
            input_rows = len(chunk)
            total_input_rows += input_rows
            
            logger.debug(f"Processing chunk {chunk_num} ({input_rows:,} rows)")
            
            cleaned, rejected = _clean_chunk(chunk, seen_order_ids, cfg)
            output_rows = len(cleaned) if not cleaned.empty else 0
            rejected_rows = len(rejected) if not rejected.empty else 0
            total_output_rows += output_rows
            total_rejected_rows += rejected_rows
            
            # Write clean data
            if not cleaned.empty:
                table = pa.Table.from_pandas(cleaned, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(output_parquet, table.schema, compression="snappy")
                    logger.info(f"Created parquet writer with schema: {len(table.schema)} columns")
                
                writer.write_table(table)
                chunks_processed += 1
            
            # Write rejected data
            if not rejected.empty and cfg.save_rejected_rows:
                rejected_table = pa.Table.from_pandas(rejected, preserve_index=False)
                if rejected_writer is None:
                    rejected_writer = pq.ParquetWriter(rejected_parquet, rejected_table.schema, compression="snappy")
                    logger.info(f"Created rejected rows writer with schema: {len(rejected_table.schema)} columns")
                
                rejected_writer.write_table(rejected_table)
            
            chunk_elapsed = time.time() - chunk_start
            retention_rate = (output_rows / input_rows) * 100 if input_rows > 0 else 0
            rejection_rate = (rejected_rows / input_rows) * 100 if input_rows > 0 else 0
            
            # Log progress every 10 chunks or if chunk takes >5 seconds
            if chunk_num % 10 == 0 or chunk_elapsed > 5.0:
                elapsed = time.time() - start_time
                rate = total_input_rows / elapsed if elapsed > 0 else 0
                logger.info(f"Processed chunk {chunk_num}: {input_rows:,} → {output_rows:,} clean, "
                           f"{rejected_rows:,} rejected ({retention_rate:.1f}% retained) in {chunk_elapsed:.1f}s")
                logger.info(f"Total progress: {total_input_rows:,} rows processed, "
                           f"{total_output_rows:,} clean, {total_rejected_rows:,} rejected ({rate:.0f} rows/sec)")
                logger.info(f"Unique order IDs seen: {len(seen_order_ids):,}")
                
    except Exception as e:
        logger.error(f"Error during CSV processing: {e}")
        raise
    finally:
        if writer is not None:
            writer.close()
        if rejected_writer is not None:
            rejected_writer.close()

    elapsed = time.time() - start_time
    
    if writer is None:
        logger.warning("No data survived the cleaning process - creating empty parquet file")
        # No data survived the cleaning stage. Emit an empty parquet file so the
        # downstream stages still have a predictable artefact to read.
        empty_df = pd.DataFrame(
            columns=[
                "order_id",
                "product_name",
                "category",
                "quantity",
                "unit_price",
                "discount_percent",
                "region",
                "sale_date",
                "customer_email",
                "revenue",
            ]
        )
        pq.write_table(pa.Table.from_pandas(empty_df, preserve_index=False), output_parquet)
    else:
        overall_retention = (total_output_rows / total_input_rows) * 100 if total_input_rows > 0 else 0
        overall_rejection = (total_rejected_rows / total_input_rows) * 100 if total_input_rows > 0 else 0
        logger.info(f"Cleaning completed: {chunks_processed} chunks processed in {elapsed:.1f}s")
        logger.info(f"Final stats: {total_input_rows:,} input rows → {total_output_rows:,} clean, {total_rejected_rows:,} rejected")
        logger.info(f"Overall retention rate: {overall_retention:.1f}%, rejection rate: {overall_rejection:.1f}%")
        logger.info(f"Unique orders processed: {len(seen_order_ids):,}")
        logger.info(f"Processing rate: {total_input_rows / elapsed:.0f} rows/second")
        
        if cfg.save_rejected_rows and total_rejected_rows > 0:
            logger.info(f"Rejected rows saved to: {rejected_parquet}")

    return output_parquet, rejected_parquet
