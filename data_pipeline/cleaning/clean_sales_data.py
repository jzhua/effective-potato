"""Utilities for cleaning the raw ecommerce CSV exports."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import MutableSet
import functools
import json
import logging
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_pipeline.settings import CLEAN_OUTPUT_DIR, DATA_DIR, REJECTED_OUTPUT_DIR, ensure_directories


_CATEGORY_LOOKUP_PATH = DATA_DIR / "lookups" / "common_categories.json"
with _CATEGORY_LOOKUP_PATH.open(encoding="utf-8") as fh:
    _COMMON_CATEGORIES = json.load(fh)

if not isinstance(_COMMON_CATEGORIES, list):
    raise ValueError("common_categories.json must contain a JSON array of category names")

_CANONICAL_CATEGORIES = sorted({str(category).strip() for category in _COMMON_CATEGORIES if category})
_CANONICAL_LOOKUP = {category.casefold(): category for category in _CANONICAL_CATEGORIES}

_REGION_LIST_PATH = DATA_DIR / "lookups" / "common_regions.json"
with _REGION_LIST_PATH.open(encoding="utf-8") as fh:
    _COMMON_REGIONS = json.load(fh)

if not isinstance(_COMMON_REGIONS, list):
    raise ValueError("common_regions.json must contain a JSON array of region names")

_CANONICAL_REGIONS = sorted({str(region).strip() for region in _COMMON_REGIONS if region})
_CANONICAL_REGION_SET = set(_CANONICAL_REGIONS)

_REGION_MAP_PATH = DATA_DIR / "lookups" / "region_map.json"
with _REGION_MAP_PATH.open(encoding="utf-8") as fh:
    _REGION_MAP_RAW = json.load(fh)

if not isinstance(_REGION_MAP_RAW, dict):
    raise ValueError("region_map.json must contain a JSON object of raw->canonical mappings")

_REGION_MAP = {}
for raw_value, mapped_to in _REGION_MAP_RAW.items():
    key = str(raw_value).strip()
    if not key:
        continue
    value = str(mapped_to).strip()
    if value != "UNKNOWN" and value not in _CANONICAL_REGION_SET:
        raise ValueError(f"Region mapping for '{raw_value}' must target a known canonical region or 'UNKNOWN'")
    _REGION_MAP[key] = value

_REGION_MAP_CASEFOLD = {key.casefold(): value for key, value in _REGION_MAP.items()}

_FUZZY_THRESHOLD = 2


def _levenshtein(left: str, right: str, *, max_distance: int) -> int:
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


@functools.lru_cache(maxsize=2048)
def _resolve_category(value: str) -> str | None:
    normalised = value.strip()
    if not normalised:
        return None
    lowered = normalised.casefold()

    if lowered in _CANONICAL_LOOKUP:
        return _CANONICAL_LOOKUP[lowered]

    best_match: str | None = None
    best_distance = _FUZZY_THRESHOLD + 1
    for candidate in _CANONICAL_CATEGORIES:
        distance = _levenshtein(lowered, candidate.casefold(), max_distance=_FUZZY_THRESHOLD)
        if distance < best_distance:
            best_distance = distance
            best_match = candidate
        if best_distance == 0:
            break

    if best_match is not None and best_distance <= _FUZZY_THRESHOLD:
        return best_match

    return None


@functools.lru_cache(maxsize=2048)
def _resolve_region(value: str) -> str | None:
    normalised = value.strip()
    if not normalised:
        return None

    mapped = _REGION_MAP.get(normalised)
    if mapped is None:
        mapped = _REGION_MAP_CASEFOLD.get(normalised.casefold())

    if mapped is None or mapped == "UNKNOWN":
        return None

    return mapped


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


def _clean_customer_email(series: pd.Series) -> pd.Series:
    normalised = _normalise_string(series)
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


# Cache for date parsing - key: date_string, value: (format_index, parsed_date)
_date_cache = {}
_date_formats = [
    "%Y-%m-%d",
    "%m/%d/%Y", 
    "%d-%m-%Y",
    "%Y/%m/%d"
]

def _parse_multiple_date_formats(date_string: str) -> pd.Timestamp | None:
    """Parse date string using multiple formats with caching and pattern recognition."""
    if not date_string or pd.isna(date_string):
        return None
    
    # Check cache first
    if date_string in _date_cache:
        format_idx, cached_date = _date_cache[date_string]
        return cached_date
    
    # Fast pattern matching before expensive parsing
    str_len = len(date_string)
    if str_len == 10:
        if date_string[4] == '-' and date_string[7] == '-':
            # Pattern: YYYY-MM-DD
            try:
                result = pd.to_datetime(date_string, format=_date_formats[0])
                _date_cache[date_string] = (0, result)
                return result
            except (ValueError, TypeError):
                pass
        elif date_string[2] == '/' and date_string[5] == '/':
            # Pattern: MM/DD/YYYY
            try:
                result = pd.to_datetime(date_string, format=_date_formats[1])
                _date_cache[date_string] = (1, result)
                return result
            except (ValueError, TypeError):
                pass
        elif date_string[2] == '-' and date_string[5] == '-':
            # Pattern: DD-MM-YYYY
            try:
                result = pd.to_datetime(date_string, format=_date_formats[2])
                _date_cache[date_string] = (2, result)
                return result
            except (ValueError, TypeError):
                pass
        elif date_string[4] == '/' and date_string[7] == '/':
            # Pattern: YYYY/MM/DD
            try:
                result = pd.to_datetime(date_string, format=_date_formats[3])
                _date_cache[date_string] = (3, result)
                return result
            except (ValueError, TypeError):
                pass
    
    # Fallback to original method for edge cases
    for i, fmt in enumerate(_date_formats):
        try:
            result = pd.to_datetime(date_string, format=fmt)
            _date_cache[date_string] = (i, result)
            return result
        except (ValueError, TypeError):
            continue
    
    # Cache failures too to avoid repeated parsing attempts
    _date_cache[date_string] = (-1, None)
    return None


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
        rejected_frames = [frame for frame in rejected_rows if not frame.empty]
        rejected_df = pd.concat(rejected_frames, ignore_index=True) if rejected_frames else pd.DataFrame()
        return data, rejected_df

    # Step 2: Remove duplicates both within the chunk and across chunks seen so far.
    before_dedup = len(data)
    
    # First handle within-chunk duplicates
    duplicate_within_chunk_mask = ~data.duplicated(subset="order_id", keep="first")
    data = _reject_rows(duplicate_within_chunk_mask, "duplicate_order_id")
    
    # Then handle cross-chunk duplicates using efficient set operations
    if seen_order_ids:
        # Use native Python set operations to preserve O(1) lookup
        chunk_order_ids = set(data["order_id"])
        duplicate_order_ids = chunk_order_ids & seen_order_ids  # Fast set intersection
        
        # Create boolean mask using vectorized operations
        if duplicate_order_ids:
            # Only compute mask if there are actual duplicates
            duplicate_mask = ~data["order_id"].apply(lambda x: x in duplicate_order_ids)
        else:
            # No duplicates found, keep all rows
            duplicate_mask = pd.Series([True] * len(data), index=data.index)
    else:
        # No previous order IDs to check against
        duplicate_mask = pd.Series([True] * len(data), index=data.index)
    
    data = _reject_rows(duplicate_mask, "duplicate_order_id")

    # Step 3: Standardise categorical features.
    raw_categories = _normalise_string(data.get("category"))
    resolved_categories = [_resolve_category(value) for value in raw_categories.tolist()]
    resolved_series = pd.Series(resolved_categories, index=data.index, dtype="object")
    valid_category_mask = resolved_series.notna()
    data = _reject_rows(valid_category_mask, "unknown_category")
    if data.empty:
        rejected_frames = [frame for frame in rejected_rows if not frame.empty]
        rejected_df = pd.concat(rejected_frames, ignore_index=True) if rejected_frames else pd.DataFrame()
        return data, rejected_df

    resolved_series = resolved_series.astype("string")
    data = data.assign(category=resolved_series.loc[data.index])

    raw_regions = _normalise_string(data.get("region"))
    resolved_regions = [_resolve_region(value) for value in raw_regions.tolist()]
    region_series = pd.Series(resolved_regions, index=data.index, dtype="object")
    valid_region_mask = region_series.notna()
    data = _reject_rows(valid_region_mask, "unknown_region")
    if data.empty:
        rejected_frames = [frame for frame in rejected_rows if not frame.empty]
        rejected_df = pd.concat(rejected_frames, ignore_index=True) if rejected_frames else pd.DataFrame()
        return data, rejected_df

    region_series = region_series.astype("string")
    data = data.assign(region=region_series.loc[data.index])

    # Step 4: Clean numeric fields and validate ranges.
    data["quantity"] = _clean_numeric(data.get("quantity"), dtype="int")
    data["unit_price"] = _clean_numeric(data.get("unit_price"))
    data["discount_percent"] = _clean_discount(data.get("discount_percent"))

    # Basic validation: negative prices or extreme values
    valid_price_mask = (data["unit_price"] > 0) & (data["unit_price"] < 50000)  # $0-$50k range
    data = _reject_rows(valid_price_mask, "invalid_unit_price")

    # Flag heavy discounts for downstream anomaly review
    heavy_discount_mask = data["discount_percent"] > 0.80
    if heavy_discount_mask.any():
        data.loc[heavy_discount_mask, "anomaly_flag"] = "heavy_discount"

    # Step 5: Drop zero quantity if configured
    if config.drop_zero_quantity:
        zero_qty_mask = data["quantity"] > 0
        data = _reject_rows(zero_qty_mask, "zero_quantity")

    # Step 6: Parse dates, validate, and convert to Unix timestamps.
    sale_date_strings = _normalise_string(data.get("sale_date"))
    sale_dates = sale_date_strings.apply(_parse_multiple_date_formats)
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
        data = data.assign(sale_date=unix_sale_dates)

    if data.empty:
        rejected_frames = [frame for frame in rejected_rows if not frame.empty]
        rejected_df = pd.concat(rejected_frames, ignore_index=True) if rejected_frames else pd.DataFrame()
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

    if "anomaly_flag" in data.columns:
        columns.append("anomaly_flag")
    cleaned_data = data[columns]
    rejected_frames = [frame for frame in rejected_rows if not frame.empty]
    rejected_df = pd.concat(rejected_frames, ignore_index=True) if rejected_frames else pd.DataFrame()
    
    return cleaned_data, rejected_df


def _to_parquet_table(frame: pd.DataFrame) -> pa.Table:
    """Normalise dtypes so pyarrow can serialise mixed data reliably."""
    if frame.empty:
        return pa.Table.from_pandas(frame, preserve_index=False)

    converted = frame.convert_dtypes()
    for column in converted.columns:
        if str(converted[column].dtype) == "object":
            converted[column] = converted[column].astype("string")
    return pa.Table.from_pandas(converted, preserve_index=False)


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
    rejected_csv = None
    if cfg.save_rejected_rows:
        rejected_csv = REJECTED_OUTPUT_DIR / f"{input_csv.stem}_rejected.csv"
        rejected_csv.parent.mkdir(parents=True, exist_ok=True)

    seen_order_ids: set[str] = set()
    writer: pq.ParquetWriter | None = None
    rejected_csv_written = False
    
    # Progress tracking
    total_input_rows = 0
    total_output_rows = 0
    total_rejected_rows = 0
    chunks_processed = 0
    start_time = time.time()
    
    logger.info(f"Starting to process CSV file: {input_csv}")
    logger.info(f"Chunk size: {cfg.chunk_size:,} rows")
    logger.info(f"Save rejected rows: {cfg.save_rejected_rows}")
    if rejected_csv:
        logger.info(f"Rejected rows will be saved to: {rejected_csv}")

    try:
        csv_reader = pd.read_csv(input_csv, chunksize=cfg.chunk_size, keep_default_na=False, na_values=[""])
        
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
                cleaned_table = _to_parquet_table(cleaned)
                if writer is None:
                    writer = pq.ParquetWriter(output_parquet, cleaned_table.schema, compression="snappy")
                    logger.info(f"Created parquet writer with schema: {len(cleaned_table.schema)} columns")

                writer.write_table(cleaned_table)
                chunks_processed += 1

            # Write rejected data to CSV
            if not rejected.empty and cfg.save_rejected_rows:
                if not rejected_csv_written:
                    # Write header on first write
                    rejected.to_csv(rejected_csv, index=False, mode='w')
                    rejected_csv_written = True
                    logger.info(f"Created rejected rows CSV with {len(rejected.columns)} columns")
                else:
                    # Append without header
                    rejected.to_csv(rejected_csv, index=False, mode='a', header=False)
            
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
        # No need to close anything for CSV writing

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
            logger.info(f"Rejected rows saved to: {rejected_csv}")

    return output_parquet, rejected_csv
