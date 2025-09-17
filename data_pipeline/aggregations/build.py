"""Aggregation helpers that derive business metrics from the cleaned dataset."""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, Mapping, Iterator
import logging
import heapq
from collections import defaultdict

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from data_pipeline.settings import AGGREGATIONS_DIR, ensure_directories

_EMPTY_SCHEMAS: Mapping[str, list[str]] = {
    "monthly_sales_summary": [
        "month",
        "total_revenue",
        "total_quantity", 
        "avg_discount_percent",
        "order_count",
    ],
    "top_products": [
        "rank",
        "product_name",
        "total_revenue",
        "total_quantity",
        "order_count",
        "metric_type",  # 'revenue' or 'units'
    ],
    "region_wise_performance": [
        "region",
        "total_revenue",
        "total_quantity",
        "order_count",
        "avg_order_value",
    ],
    "category_discount_map": [
        "category",
        "avg_discount_percent",
        "order_count",
        "total_revenue",
    ],
    "anomaly_records": [
        "rank",
        "order_id",
        "product_name",
        "category",
        "region",
        "revenue",
        "quantity",
        "unit_price",
        "discount_percent",
        "sale_date",
        "anomaly_reason",
    ],
}


def _iter_parquet_chunks(clean_path: Path, batch_size: int = 1_000_000) -> Iterator[pd.DataFrame]:
    """Iterate over parquet file in chunks to avoid loading entire dataset into memory."""
    parquet_file = pq.ParquetFile(clean_path)
    total_rows = parquet_file.metadata.num_rows
    logger = logging.getLogger(__name__)
    
    processed_rows = 0
    for batch_num, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size), 1):
        # Convert arrow batch to pandas DataFrame
        df = batch.to_pandas()
        
        if df.empty:
            continue
            
        processed_rows += len(df)
        progress_pct = (processed_rows / total_rows) * 100
        
        # Log progress every 5 batches or at key milestones
        if batch_num % 5 == 0 or progress_pct in [10, 25, 50, 75, 90]:
            logger.info(f"  Progress: {processed_rows:,}/{total_rows:,} rows ({progress_pct:.1f}%) - Batch {batch_num}")
            
        # Ensure correct dtypes for downstream calculations
        df["sale_date"] = pd.to_datetime(df["sale_date"], unit="s", errors="coerce")
        df["discount_percent"] = df["discount_percent"].astype(float)
        df["quantity"] = df["quantity"].astype(float) 
        df["revenue"] = df["revenue"].astype(float)
        
        yield df


def _load_clean_data(clean_path: Path) -> pd.DataFrame:
    """Legacy function - kept for backward compatibility but not recommended for large files."""
    frame = pd.read_parquet(clean_path)
    if frame.empty:
        return frame
    # Ensure we have the correct dtypes for downstream calculations.
    frame["sale_date"] = pd.to_datetime(frame["sale_date"], unit="s", errors="coerce")
    frame["discount_percent"] = frame["discount_percent"].astype(float)
    frame["quantity"] = frame["quantity"].astype(float)
    frame["revenue"] = frame["revenue"].astype(float)
    return frame


def _monthly_sales_summary_chunked(parquet_path: Path) -> pd.DataFrame:
    """Chunked monthly sales summary: Revenue, quantity, avg discount by month"""
    logger = logging.getLogger(__name__)
    logger.info("Building monthly sales summary (chunked)...")
    
    monthly_totals = defaultdict(lambda: {
        'total_revenue': 0.0,
        'total_quantity': 0.0,
        'discount_sum': 0.0,
        'discount_count': 0,
        'order_ids': set()
    })
    
    total_rows = 0
    for chunk_num, chunk in enumerate(_iter_parquet_chunks(parquet_path), 1):
        total_rows += len(chunk)
        
        # Extract month from sale_date
        chunk = chunk.copy()
        chunk['month'] = chunk['sale_date'].dt.strftime('%Y-%m')
        
        # Aggregate by month within this chunk
        for month, group in chunk.groupby('month'):
            monthly_totals[month]['total_revenue'] += group['revenue'].sum()
            monthly_totals[month]['total_quantity'] += group['quantity'].sum()
            monthly_totals[month]['discount_sum'] += (group['discount_percent'] * len(group)).sum()
            monthly_totals[month]['discount_count'] += len(group)
            monthly_totals[month]['order_ids'].update(group['order_id'].tolist())
        
        # Progress logging is now handled by _iter_parquet_chunks
    
    # Convert to final DataFrame
    result_data = []
    for month, data in monthly_totals.items():
        avg_discount = data['discount_sum'] / data['discount_count'] if data['discount_count'] > 0 else 0
        result_data.append({
            'month': month,
            'total_revenue': round(data['total_revenue'], 2),
            'total_quantity': int(data['total_quantity']),
            'avg_discount_percent': round(avg_discount, 4),
            'order_count': len(data['order_ids'])
        })
    
    result = pd.DataFrame(result_data).sort_values('month')
    logger.info(f"Generated monthly summary for {len(result)} months from {total_rows:,} rows")
    return result


def _monthly_sales_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Monthly sales summary: Revenue, quantity, avg discount by month (legacy)"""
    logger = logging.getLogger(__name__)
    logger.info("Building monthly sales summary...")
    
    # Extract month from sale_date
    df = df.copy()
    df['month'] = df['sale_date'].dt.strftime('%Y-%m')
    
    grouped = (
        df.groupby("month")
        .agg(
            total_revenue=("revenue", "sum"),
            total_quantity=("quantity", "sum"),
            avg_discount_percent=("discount_percent", "mean"),
            order_count=("order_id", "nunique"),
        )
        .reset_index()
    )
    
    # Round values for readability
    grouped["total_revenue"] = grouped["total_revenue"].round(2)
    grouped["total_quantity"] = grouped["total_quantity"].astype(int)
    grouped["avg_discount_percent"] = grouped["avg_discount_percent"].round(4)
    
    result = grouped.sort_values("month")
    logger.info(f"Generated monthly summary for {len(result)} months")
    return result


def _top_products_chunked(parquet_path: Path, limit: int = 10) -> pd.DataFrame:
    """Chunked top products by revenue and units using heaps for efficiency"""
    logger = logging.getLogger(__name__)
    logger.info("Building top products by revenue and units (chunked)...")
    
    product_totals = defaultdict(lambda: {
        'total_revenue': 0.0,
        'total_quantity': 0.0,
        'order_ids': set()
    })
    
    total_rows = 0
    for chunk_num, chunk in enumerate(_iter_parquet_chunks(parquet_path), 1):
        total_rows += len(chunk)
        
        # Aggregate by product within this chunk
        for product, group in chunk.groupby('product_name'):
            product_totals[product]['total_revenue'] += group['revenue'].sum()
            product_totals[product]['total_quantity'] += group['quantity'].sum()
            product_totals[product]['order_ids'].update(group['order_id'].tolist())
        
        # Progress logging is now handled by _iter_parquet_chunks
    
    # Convert to list and get top products
    products_list = []
    for product, data in product_totals.items():
        products_list.append({
            'product_name': product,
            'total_revenue': data['total_revenue'],
            'total_quantity': data['total_quantity'],
            'order_count': len(data['order_ids'])
        })
    
    products_df = pd.DataFrame(products_list)
    
    # Top N by revenue
    top_by_revenue = products_df.nlargest(limit, 'total_revenue').copy()
    top_by_revenue["rank"] = range(1, len(top_by_revenue) + 1)
    top_by_revenue["metric_type"] = "revenue"
    
    # Top N by units
    top_by_units = products_df.nlargest(limit, 'total_quantity').copy()
    top_by_units["rank"] = range(1, len(top_by_units) + 1)
    top_by_units["metric_type"] = "units"
    
    # Combine both rankings
    result = pd.concat([top_by_revenue, top_by_units], ignore_index=True)
    
    # Round values
    result["total_revenue"] = result["total_revenue"].round(2)
    result["total_quantity"] = result["total_quantity"].astype(int)
    
    logger.info(f"Generated top {limit} products by revenue and {limit} by units from {total_rows:,} rows")
    return result


def _top_products(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    """Top 10 products by revenue and units (legacy)"""
    logger = logging.getLogger(__name__)
    logger.info("Building top products by revenue and units...")
    
    # Group by product
    grouped = (
        df.groupby("product_name")
        .agg(
            total_revenue=("revenue", "sum"),
            total_quantity=("quantity", "sum"),
            order_count=("order_id", "nunique"),
        )
        .reset_index()
    )
    
    # Top 10 by revenue
    top_by_revenue = grouped.sort_values("total_revenue", ascending=False).head(limit).copy()
    top_by_revenue["rank"] = range(1, len(top_by_revenue) + 1)
    top_by_revenue["metric_type"] = "revenue"
    
    # Top 10 by units
    top_by_units = grouped.sort_values("total_quantity", ascending=False).head(limit).copy()
    top_by_units["rank"] = range(1, len(top_by_units) + 1)
    top_by_units["metric_type"] = "units"
    
    # Combine both rankings
    result = pd.concat([top_by_revenue, top_by_units], ignore_index=True)
    
    # Round values
    result["total_revenue"] = result["total_revenue"].round(2)
    result["total_quantity"] = result["total_quantity"].astype(int)
    
    logger.info(f"Generated top {limit} products by revenue and {limit} by units")
    return result


def _region_wise_performance_chunked(parquet_path: Path) -> pd.DataFrame:
    """Chunked sales performance by region"""
    logger = logging.getLogger(__name__)
    logger.info("Building region-wise performance (chunked)...")
    
    region_totals = defaultdict(lambda: {
        'total_revenue': 0.0,
        'total_quantity': 0.0,
        'order_ids': set()
    })
    
    total_rows = 0
    for chunk_num, chunk in enumerate(_iter_parquet_chunks(parquet_path), 1):
        total_rows += len(chunk)
        
        # Aggregate by region within this chunk
        for region, group in chunk.groupby('region'):
            region_totals[region]['total_revenue'] += group['revenue'].sum()
            region_totals[region]['total_quantity'] += group['quantity'].sum()
            region_totals[region]['order_ids'].update(group['order_id'].tolist())
        
        # Progress logging is now handled by _iter_parquet_chunks
    
    # Convert to final DataFrame
    result_data = []
    for region, data in region_totals.items():
        order_count = len(data['order_ids'])
        avg_order_value = data['total_revenue'] / order_count if order_count > 0 else 0
        result_data.append({
            'region': region,
            'total_revenue': round(data['total_revenue'], 2),
            'total_quantity': int(data['total_quantity']),
            'order_count': order_count,
            'avg_order_value': round(avg_order_value, 2)
        })
    
    result = pd.DataFrame(result_data).sort_values('total_revenue', ascending=False)
    logger.info(f"Generated performance metrics for {len(result)} regions from {total_rows:,} rows")
    return result


def _region_wise_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Sales performance by region (legacy)"""
    logger = logging.getLogger(__name__)
    logger.info("Building region-wise performance...")
    
    grouped = (
        df.groupby("region")
        .agg(
            total_revenue=("revenue", "sum"),
            total_quantity=("quantity", "sum"),
            order_count=("order_id", "nunique"),
        )
        .reset_index()
    )
    
    # Calculate average order value
    grouped["avg_order_value"] = (grouped["total_revenue"] / grouped["order_count"]).round(2)
    
    # Round values
    grouped["total_revenue"] = grouped["total_revenue"].round(2)
    grouped["total_quantity"] = grouped["total_quantity"].astype(int)
    
    result = grouped.sort_values("total_revenue", ascending=False)
    logger.info(f"Generated performance metrics for {len(result)} regions")
    return result


def _category_discount_map_chunked(parquet_path: Path) -> pd.DataFrame:
    """Chunked average discount by category"""
    logger = logging.getLogger(__name__)
    logger.info("Building category discount mapping (chunked)...")
    
    category_totals = defaultdict(lambda: {
        'discount_sum': 0.0,
        'discount_count': 0,
        'total_revenue': 0.0,
        'order_ids': set()
    })
    
    total_rows = 0
    for chunk_num, chunk in enumerate(_iter_parquet_chunks(parquet_path), 1):
        total_rows += len(chunk)
        
        # Aggregate by category within this chunk
        for category, group in chunk.groupby('category'):
            category_totals[category]['discount_sum'] += (group['discount_percent'] * len(group)).sum()
            category_totals[category]['discount_count'] += len(group)
            category_totals[category]['total_revenue'] += group['revenue'].sum()
            category_totals[category]['order_ids'].update(group['order_id'].tolist())
        
        # Progress logging is now handled by _iter_parquet_chunks
    
    # Convert to final DataFrame
    result_data = []
    for category, data in category_totals.items():
        avg_discount = data['discount_sum'] / data['discount_count'] if data['discount_count'] > 0 else 0
        result_data.append({
            'category': category,
            'avg_discount_percent': round(avg_discount, 4),
            'order_count': len(data['order_ids']),
            'total_revenue': round(data['total_revenue'], 2)
        })
    
    result = pd.DataFrame(result_data).sort_values('avg_discount_percent', ascending=False)
    logger.info(f"Generated discount mapping for {len(result)} categories from {total_rows:,} rows")
    return result


def _anomaly_records_chunked(parquet_path: Path, limit: int = 5) -> pd.DataFrame:
    """Chunked anomaly records with extremely high revenue or extremely high discounts"""
    logger = logging.getLogger(__name__)
    logger.info("Identifying anomaly records (chunked)...")
    
    # Use heaps to track top records efficiently
    top_revenue_heap = []  # min heap for top revenue records
    top_discount_heap = []  # min heap for top discount records
    
    total_rows = 0
    for chunk_num, chunk in enumerate(_iter_parquet_chunks(parquet_path), 1):
        total_rows += len(chunk)
        
        # Process top revenue records in this chunk
        chunk_top_revenue = chunk.nlargest(limit, 'revenue')
        for idx, row in chunk_top_revenue.iterrows():
            if len(top_revenue_heap) < limit:
                heapq.heappush(top_revenue_heap, (row['revenue'], idx, row.to_dict()))
            elif row['revenue'] > top_revenue_heap[0][0]:
                heapq.heapreplace(top_revenue_heap, (row['revenue'], idx, row.to_dict()))
        
        # Process top discount records in this chunk
        chunk_top_discount = chunk.nlargest(limit, 'discount_percent')
        for idx, row in chunk_top_discount.iterrows():
            if len(top_discount_heap) < limit:
                heapq.heappush(top_discount_heap, (row['discount_percent'], idx, row.to_dict()))
            elif row['discount_percent'] > top_discount_heap[0][0]:
                heapq.heapreplace(top_discount_heap, (row['discount_percent'], idx, row.to_dict()))
        
        # Progress logging is now handled by _iter_parquet_chunks
    
    # Convert heaps to DataFrames
    top_revenue_records = [record for _, _, record in top_revenue_heap]
    top_discount_records = [record for _, _, record in top_discount_heap]
    
    top_revenue_df = pd.DataFrame(top_revenue_records)
    top_revenue_df["anomaly_reason"] = "high_revenue"
    
    top_discount_df = pd.DataFrame(top_discount_records)
    top_discount_df["anomaly_reason"] = "high_discount"
    
    # Combine and deduplicate
    all_anomalies = pd.concat([top_revenue_df, top_discount_df], ignore_index=True)
    all_anomalies = all_anomalies.drop_duplicates(subset=['order_id'], keep='first')
    all_anomalies = all_anomalies.head(limit)
    
    # Add rank and format
    all_anomalies["rank"] = range(1, len(all_anomalies) + 1)
    
    columns = [
        "rank", "order_id", "product_name", "category", "region",
        "revenue", "quantity", "unit_price", "discount_percent", "sale_date", "anomaly_reason"
    ]
    
    result = all_anomalies[columns].copy()
    result["revenue"] = result["revenue"].round(2)
    result["unit_price"] = result["unit_price"].round(2)
    result["discount_percent"] = result["discount_percent"].round(4)
    result["quantity"] = result["quantity"].astype(int)
    
    revenue_count = len(result[result["anomaly_reason"] == "high_revenue"])
    discount_count = len(result[result["anomaly_reason"] == "high_discount"])
    logger.info(f"Identified {len(result)} anomaly records: {revenue_count} high revenue, {discount_count} high discount from {total_rows:,} rows")
    
    return result


def _category_discount_map(df: pd.DataFrame) -> pd.DataFrame:
    """Average discount by category (legacy)"""
    logger = logging.getLogger(__name__)
    logger.info("Building category discount mapping...")
    
    grouped = (
        df.groupby("category")
        .agg(
            avg_discount_percent=("discount_percent", "mean"),
            order_count=("order_id", "nunique"),
            total_revenue=("revenue", "sum"),
        )
        .reset_index()
    )
    
    # Round values
    grouped["avg_discount_percent"] = grouped["avg_discount_percent"].round(4)
    grouped["total_revenue"] = grouped["total_revenue"].round(2)
    
    result = grouped.sort_values("avg_discount_percent", ascending=False)
    logger.info(f"Generated discount mapping for {len(result)} categories")
    return result


def _anomaly_records(df: pd.DataFrame, limit: int = 5) -> pd.DataFrame:
    """Anomaly records with extremely high revenue or extremely high discounts (legacy)"""
    logger = logging.getLogger(__name__)
    logger.info("Identifying anomaly records...")
    
    # Get top records by revenue
    top_revenue = df.nlargest(limit, 'revenue').copy()
    top_revenue["anomaly_reason"] = "high_revenue"
    
    # Get top records by discount percentage
    top_discount = df.nlargest(limit, 'discount_percent').copy()
    top_discount["anomaly_reason"] = "high_discount"
    
    # Combine both types of anomalies
    all_anomalies = pd.concat([top_revenue, top_discount], ignore_index=True)
    
    # Remove duplicates (records that appear in both categories)
    all_anomalies = all_anomalies.drop_duplicates(subset=['order_id'], keep='first')
    
    # Take top records by combining both types, up to the limit
    all_anomalies = all_anomalies.head(limit)
    
    # Add rank
    all_anomalies["rank"] = range(1, len(all_anomalies) + 1)
    
    # Select relevant columns
    columns = [
        "rank", "order_id", "product_name", "category", "region",
        "revenue", "quantity", "unit_price", "discount_percent", "sale_date", "anomaly_reason"
    ]
    
    result = all_anomalies[columns].copy()
    
    # Round values
    result["revenue"] = result["revenue"].round(2)
    result["unit_price"] = result["unit_price"].round(2)
    result["discount_percent"] = result["discount_percent"].round(4)
    result["quantity"] = result["quantity"].astype(int)
    
    revenue_count = len(result[result["anomaly_reason"] == "high_revenue"])
    discount_count = len(result[result["anomaly_reason"] == "high_discount"])
    logger.info(f"Identified {len(result)} anomaly records: {revenue_count} high revenue, {discount_count} high discount")
    
    return result


# Chunked aggregation builders for large datasets (uses PyArrow streaming)
_CHUNKED_AGGREGATION_BUILDERS: Mapping[str, Callable[..., pd.DataFrame]] = {
    "monthly_sales_summary": _monthly_sales_summary_chunked,
    "top_products": _top_products_chunked,
    "region_wise_performance": _region_wise_performance_chunked,
    "category_discount_map": _category_discount_map_chunked,
    "anomaly_records": _anomaly_records_chunked,
}

# Legacy aggregation builders for smaller datasets (loads all data into memory)
_AGGREGATION_BUILDERS: Mapping[str, Callable[..., pd.DataFrame]] = {
    "monthly_sales_summary": _monthly_sales_summary,
    "top_products": _top_products,
    "region_wise_performance": _region_wise_performance,
    "category_discount_map": _category_discount_map,
    "anomaly_records": _anomaly_records,
}


def build_all_aggregations(
    clean_parquet: Path,
    output_dir: Path | None = None,
    *,
    top_products_limit: int = 10,
    anomaly_limit: int = 5,
    force_chunked: bool = False,
    chunk_threshold_gb: float = 1.0,
) -> Dict[str, Path]:
    """Compute all supported aggregations from the cleaned dataset.
    
    Automatically uses chunked processing for large files to avoid memory issues.
    
    Args:
        clean_parquet: Path to cleaned parquet file
        output_dir: Output directory for aggregation files
        top_products_limit: Number of top products to include
        anomaly_limit: Number of anomaly records to identify
        force_chunked: Force chunked processing regardless of file size
        chunk_threshold_gb: File size threshold (GB) to trigger chunked processing
    
    Generates the following aggregation files:
    - monthly_sales_summary.parquet: Revenue, quantity, avg discount by month
    - top_products.parquet: Top N by revenue and units  
    - region_wise_performance.parquet: Sales by region
    - category_discount_map.parquet: Avg discount by category
    - anomaly_records.parquet: Top N records with extremely high revenue
    """
    logger = logging.getLogger(__name__)
    
    ensure_directories()
    output = output_dir or AGGREGATIONS_DIR
    output.mkdir(parents=True, exist_ok=True)
    
    # Check file size to determine processing strategy
    file_size_gb = clean_parquet.stat().st_size / (1024**3)
    use_chunked = force_chunked or file_size_gb > chunk_threshold_gb
    
    logger.info(f"Input file: {clean_parquet}")
    logger.info(f"File size: {file_size_gb:.2f} GB")
    logger.info(f"Processing strategy: {'Chunked (PyArrow)' if use_chunked else 'In-memory (pandas)'}")
    
    # Check if file is empty
    if file_size_gb < 0.001:  # Less than 1MB - likely empty
        logger.warning("Input data appears empty, generating empty aggregation files")
        generated: Dict[str, Path] = {}
        for name in _AGGREGATION_BUILDERS:
            artefact = output / f"{name}.parquet"
            pd.DataFrame(columns=_EMPTY_SCHEMAS[name]).to_parquet(artefact, index=False)
            generated[name] = artefact
            logger.info(f"Generated empty aggregation: {artefact}")
        return generated
    
    # Choose appropriate builders based on file size
    builders = _CHUNKED_AGGREGATION_BUILDERS if use_chunked else _AGGREGATION_BUILDERS
    
    if use_chunked:
        logger.info("Using chunked processing for large dataset")
        results: Dict[str, Path] = {}
        total_aggregations = len(builders)
        
        for agg_num, (name, builder) in enumerate(builders.items(), 1):
            logger.info(f"[{agg_num}/{total_aggregations}] Building aggregation: {name}")
            
            import time
            start_time = time.time()
            
            # Pass appropriate arguments to chunked functions
            if name == "top_products":
                data = builder(clean_parquet, limit=top_products_limit)
            elif name == "anomaly_records":
                data = builder(clean_parquet, limit=anomaly_limit)
            else:
                data = builder(clean_parquet)
            
            elapsed = time.time() - start_time
            
            artefact = output / f"{name}.parquet"
            data.to_parquet(artefact, index=False)
            results[name] = artefact
            
            # Log aggregation stats
            file_size_kb = artefact.stat().st_size / 1024
            logger.info(f"✅ Completed {name}: {len(data):,} rows, {file_size_kb:.1f} KB in {elapsed:.1f}s -> {artefact}")
            
            if agg_num < total_aggregations:
                logger.info(f"📊 Overall progress: {agg_num}/{total_aggregations} aggregations complete")
    
    else:
        logger.info("Using in-memory processing for smaller dataset")
        frame = _load_clean_data(clean_parquet)
        
        if frame.empty:
            logger.warning("Input data is empty, generating empty aggregation files")
            generated: Dict[str, Path] = {}
            for name in _AGGREGATION_BUILDERS:
                artefact = output / f"{name}.parquet"
                pd.DataFrame(columns=_EMPTY_SCHEMAS[name]).to_parquet(artefact, index=False)
                generated[name] = artefact
                logger.info(f"Generated empty aggregation: {artefact}")
            return generated

        logger.info(f"Processing {len(frame):,} records from cleaned data")
        logger.info(f"Data date range: {frame['sale_date'].min()} to {frame['sale_date'].max()}")
        
        results: Dict[str, Path] = {}
        for name, builder in builders.items():
            logger.info(f"Building aggregation: {name}")
            
            # Pass appropriate limits to functions that need them
            if name == "top_products":
                data = builder(frame, limit=top_products_limit)
            elif name == "anomaly_records":
                data = builder(frame, limit=anomaly_limit)
            else:
                data = builder(frame)
            
            artefact = output / f"{name}.parquet"
            data.to_parquet(artefact, index=False)
            results[name] = artefact
            
            # Log aggregation stats
            file_size_kb = artefact.stat().st_size / 1024
            logger.info(f"Saved {name}: {len(data):,} rows, {file_size_kb:.1f} KB -> {artefact}")

    logger.info(f"Successfully generated {len(results)} aggregation files in {output}")
    return results
