import pandas as pd

from data_pipeline.aggregations import build_all_aggregations


EXPECTED_FILES = {
    "monthly_sales_summary",
    "top_products_by_category",
    "region_wise_performance",
    "top_categories",
    "anomaly_records",
}


def test_build_all_aggregations_produces_expected_outputs(tmp_path):
    unix_time = int(pd.Timestamp("2023-01-15").timestamp())
    
    # Create enough data to exceed 1MB threshold (duplicate rows to create a larger file)
    num_rows = 100000  # Should be enough to exceed 1MB
    base_data = {
        "order_id": [f"ORD-{i}" for i in range(num_rows)],
        "product_name": ["Widget"] * num_rows,
        "category": ["Electronics"] * num_rows,
        "quantity": [3] * num_rows,
        "unit_price": [100.0] * num_rows,
        "discount_percent": [0.0] * num_rows,
        "region": ["Mumbai"] * num_rows,
        "sale_date": [unix_time] * num_rows,
        "customer_email": [f"customer{i}@example.com" for i in range(num_rows)],
        "revenue": [300.0] * num_rows,
    }
    cleaned_frame = pd.DataFrame(base_data)

    cleaned_path = tmp_path / "cleaned.parquet"
    cleaned_frame.to_parquet(cleaned_path, index=False)

    output_dir = tmp_path / "aggregations"
    results = build_all_aggregations(cleaned_path, output_dir=output_dir, top_products_limit=5, anomaly_limit=2)

    assert EXPECTED_FILES == set(results.keys())

    monthly_summary = pd.read_parquet(results["monthly_sales_summary"])
    assert list(monthly_summary["month"]) == ["2023-01"]
    assert float(monthly_summary.loc[0, "total_revenue"]) == 30000000.0  # 100000 * 300.0

    category_best = pd.read_parquet(results["top_products_by_category"])
    assert {"revenue", "units"} == set(category_best["metric_type"])
    assert {"Electronics"} == set(category_best["category"])

    anomalies = pd.read_parquet(results["anomaly_records"])
    assert len(anomalies) >= 1  # Should have at least 1 anomaly
    assert pd.Timestamp("2023-01-15") == anomalies.loc[0, "sale_date"]
