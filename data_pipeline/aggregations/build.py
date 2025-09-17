"""Aggregation helpers that derive business metrics from the cleaned dataset."""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, Mapping
import logging

import pandas as pd

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


def _load_clean_data(clean_path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(clean_path)
    if frame.empty:
        return frame
    # Ensure we have the correct dtypes for downstream calculations.
    frame["sale_date"] = pd.to_datetime(frame["sale_date"], unit="s", errors="coerce")
    frame["discount_percent"] = frame["discount_percent"].astype(float)
    frame["quantity"] = frame["quantity"].astype(float)
    frame["revenue"] = frame["revenue"].astype(float)
    return frame


def _monthly_sales_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Monthly sales summary: Revenue, quantity, avg discount by month"""
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


def _top_products(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    """Top 10 products by revenue and units"""
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


def _region_wise_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Sales performance by region"""
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


def _category_discount_map(df: pd.DataFrame) -> pd.DataFrame:
    """Average discount by category"""
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
    """Anomaly records with extremely high revenue or extremely high discounts"""
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
) -> Dict[str, Path]:
    """Compute all supported aggregations from the cleaned dataset.
    
    Generates the following aggregation files:
    - monthly_sales_summary.parquet: Revenue, quantity, avg discount by month
    - top_products.parquet: Top 10 by revenue and units  
    - region_wise_performance.parquet: Sales by region
    - category_discount_map.parquet: Avg discount by category
    - anomaly_records.parquet: Top 5 records with extremely high revenue
    """
    logger = logging.getLogger(__name__)
    
    ensure_directories()
    output = output_dir or AGGREGATIONS_DIR
    output.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Loading cleaned data from: {clean_parquet}")
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
    for name, builder in _AGGREGATION_BUILDERS.items():
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
