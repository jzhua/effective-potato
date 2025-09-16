import pandas as pd

from data_pipeline.cleaning import CleanConfig, clean_csv_to_parquet
from data_pipeline.cleaning import clean_sales_data


def test_clean_csv_to_parquet_converts_sale_date_to_unix(tmp_path):
    raw_frame = pd.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-2"],
            "product_name": ["Widget", "Widget"],
            "category": ["Electronics", "Electronics"],
            "quantity": [2, 2],
            "unit_price": [9.99, 9.99],
            "discount_percent": [0.1, 0.9],
            "region": ["North America", "North America"],
            "sale_date": ["2023-01-01", "not-a-date"],
            "customer_email": ["customer@example.com", "customer@example.com"],
        }
    )
    csv_path = tmp_path / "raw.csv"
    raw_frame.to_csv(csv_path, index=False)

    output_path = tmp_path / "clean.parquet"
    clean_csv_to_parquet(
        csv_path,
        output_path,
        config=CleanConfig(chunk_size=10, save_rejected_rows=False),
    )

    cleaned = pd.read_parquet(output_path)

    assert len(cleaned) == 1
    assert cleaned.loc[0, "order_id"] == "ORD-1"
    expected_timestamp = int(pd.Timestamp("2023-01-01").timestamp())
    assert int(cleaned.loc[0, "sale_date"]) == expected_timestamp


def test_clean_csv_rejects_unknown_category(tmp_path):
    raw_frame = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "product_name": ["Widget"],
            "category": ["Totally Unknown"],
            "quantity": [1],
            "unit_price": [10.0],
            "discount_percent": [0.0],
            "region": ["North America"],
            "sale_date": ["2023-01-01"],
            "customer_email": ["customer@example.com"],
        }
    )
    csv_path = tmp_path / "raw.csv"
    raw_frame.to_csv(csv_path, index=False)

    output_path = tmp_path / "clean.parquet"
    clean_csv_to_parquet(
        csv_path,
        output_path,
        config=CleanConfig(chunk_size=10, save_rejected_rows=False),
    )

    cleaned = pd.read_parquet(output_path)
    assert cleaned.empty


def test_category_fuzzy_lookup(tmp_path, monkeypatch):
    monkeypatch.setattr(
        clean_sales_data,
        "_CANONICAL_CATEGORIES",
        ["Home Office"],
        raising=False,
    )
    monkeypatch.setattr(
        clean_sales_data,
        "_CANONICAL_LOOKUP",
        {"home office": "Home Office"},
        raising=False,
    )
    clean_sales_data._resolve_category.cache_clear()

    raw_frame = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "product_name": ["Desk"],
            "category": ["home ofice"],
            "quantity": [1],
            "unit_price": [150.0],
            "discount_percent": [0.0],
            "region": ["North America"],
            "sale_date": ["2023-02-01"],
            "customer_email": ["customer@example.com"],
        }
    )
    csv_path = tmp_path / "raw.csv"
    raw_frame.to_csv(csv_path, index=False)

    output_path = tmp_path / "clean.parquet"
    clean_csv_to_parquet(
        csv_path,
        output_path,
        config=CleanConfig(chunk_size=10, save_rejected_rows=False),
    )

    cleaned = pd.read_parquet(output_path)
    assert cleaned.loc[0, "category"] == "Home Office"
    assert "anomaly_flag" not in cleaned.columns or cleaned["anomaly_flag"].isna().all()


def test_heavy_discount_flagged_as_anomaly(tmp_path):
    raw_frame = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "product_name": ["Widget"],
            "category": ["Electronics"],
            "quantity": [1],
            "unit_price": [100.0],
            "discount_percent": [0.85],
            "region": ["North America"],
            "sale_date": ["2023-03-01"],
            "customer_email": ["customer@example.com"],
        }
    )
    csv_path = tmp_path / "raw.csv"
    raw_frame.to_csv(csv_path, index=False)

    output_path = tmp_path / "clean.parquet"
    clean_csv_to_parquet(
        csv_path,
        output_path,
        config=CleanConfig(chunk_size=10, save_rejected_rows=False),
    )

    cleaned = pd.read_parquet(output_path)
    assert cleaned.loc[0, "anomaly_flag"] == "heavy_discount"


def test_clean_csv_rejects_unknown_region(tmp_path):
    raw_frame = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "product_name": ["Widget"],
            "category": ["Electronics"],
            "quantity": [1],
            "unit_price": [10.0],
            "discount_percent": [0.0],
            "region": ["Atlantis"],
            "sale_date": ["2023-04-01"],
            "customer_email": ["customer@example.com"],
        }
    )
    csv_path = tmp_path / "raw.csv"
    raw_frame.to_csv(csv_path, index=False)

    output_path = tmp_path / "clean.parquet"
    clean_csv_to_parquet(
        csv_path,
        output_path,
        config=CleanConfig(chunk_size=10, save_rejected_rows=False),
    )

    cleaned = pd.read_parquet(output_path)
    assert cleaned.empty


def test_region_fuzzy_lookup(tmp_path, monkeypatch):
    monkeypatch.setattr(
        clean_sales_data,
        "_CANONICAL_REGIONS",
        ["North America"],
        raising=False,
    )
    monkeypatch.setattr(
        clean_sales_data,
        "_REGION_LOOKUP",
        {"north america": "North America"},
        raising=False,
    )
    clean_sales_data._resolve_region.cache_clear()

    raw_frame = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "product_name": ["Widget"],
            "category": ["Electronics"],
            "quantity": [1],
            "unit_price": [10.0],
            "discount_percent": [0.0],
            "region": ["north ameria"],
            "sale_date": ["2023-04-01"],
            "customer_email": ["customer@example.com"],
        }
    )
    csv_path = tmp_path / "raw.csv"
    raw_frame.to_csv(csv_path, index=False)

    output_path = tmp_path / "clean.parquet"
    clean_csv_to_parquet(
        csv_path,
        output_path,
        config=CleanConfig(chunk_size=10, save_rejected_rows=False),
    )

    cleaned = pd.read_parquet(output_path)
    assert cleaned.loc[0, "region"] == "North America"
