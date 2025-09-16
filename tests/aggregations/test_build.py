import pandas as pd

from data_pipeline.aggregations import build_all_aggregations


EXPECTED_FILES = {
    "monthly_sales_summary",
    "top_products",
    "region_wise_performance",
    "category_discount_map",
    "anomaly_records",
}


def test_build_all_aggregations_produces_expected_outputs(tmp_path):
    unix_time = int(pd.Timestamp("2023-01-15").timestamp())
    cleaned_frame = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "product_name": ["Widget"],
            "category": ["Electronics"],
            "quantity": [3],
            "unit_price": [100.0],
            "discount_percent": [0.0],
            "region": ["North America"],
            "sale_date": [unix_time],
            "customer_email": ["customer@example.com"],
            "revenue": [300.0],
        }
    )

    cleaned_path = tmp_path / "cleaned.parquet"
    cleaned_frame.to_parquet(cleaned_path, index=False)

    output_dir = tmp_path / "aggregations"
    results = build_all_aggregations(cleaned_path, output_dir=output_dir, top_products_limit=5, anomaly_limit=2)

    assert EXPECTED_FILES == set(results.keys())

    monthly_summary = pd.read_parquet(results["monthly_sales_summary"])
    assert list(monthly_summary["month"]) == ["2023-01"]
    assert float(monthly_summary.loc[0, "total_revenue"]) == 300.0

    top_products = pd.read_parquet(results["top_products"])
    assert {"revenue", "units"} == set(top_products["metric_type"])  # both rankings present

    anomalies = pd.read_parquet(results["anomaly_records"])
    assert len(anomalies) == 1
    assert pd.Timestamp("2023-01-15") == anomalies.loc[0, "sale_date"]
