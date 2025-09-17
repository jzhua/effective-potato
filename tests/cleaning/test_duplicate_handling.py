#!/usr/bin/env python3
"""Test duplicate order ID handling in the cleaning pipeline."""

import pytest
import pandas as pd
from pathlib import Path
from data_pipeline.cleaning import clean_csv_to_parquet, CleanConfig


class TestDuplicateHandling:
    """Test that duplicate order IDs are handled correctly."""
    
    def test_duplicate_order_ids_within_chunk(self, tmp_path):
        """Test duplicate handling when duplicates appear in the same chunk."""
        
        # Create test data with duplicate order IDs but different other data
        raw_frame = pd.DataFrame({
            "order_id": ["ORD-123", "ORD-123"],  # Same order ID
            "product_name": ["Widget", "Gadget"],  # Different products
            "category": ["Electronics", "Electronics"],
            "quantity": [1, 2],
            "unit_price": [10.0, 20.0],
            "discount_percent": [0.1, 0.2],
            "region": ["Mumbai", "Thailand"],  # Different regions
            "sale_date": ["2023-01-01", "2023-01-02"],
            "customer_email": ["user1@example.com", "user2@example.com"],
        })
        
        csv_path = tmp_path / "test_duplicates.csv"
        raw_frame.to_csv(csv_path, index=False)
        
        clean_path, rejected_path = clean_csv_to_parquet(
            csv_path,
            config=CleanConfig(chunk_size=10, save_rejected_rows=True)
        )
        
        # Should have exactly 1 clean record (first one kept)
        clean_df = pd.read_parquet(clean_path)
        assert len(clean_df) == 1
        assert clean_df.iloc[0]["order_id"] == "ORD-123"
        # Should keep the first record (Widget, Mumbai)
        assert clean_df.iloc[0]["product_name"] == "Widget"
        assert clean_df.iloc[0]["region"] == "Mumbai"
        
        # Should have exactly 1 rejected record (second one rejected)
        rejected_df = pd.read_csv(rejected_path, keep_default_na=False, na_values=[""])
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]["order_id"] == "ORD-123"
        assert rejected_df.iloc[0]["rejection_reason"] == "duplicate_order_id"
        # Should preserve original data of rejected record
        assert rejected_df.iloc[0]["product_name"] == "Gadget"
        assert rejected_df.iloc[0]["region"] == "Thailand"
    
    def test_duplicate_order_ids_across_chunks(self, tmp_path):
        """Test duplicate handling when duplicates span multiple chunks."""
        
        # Create test data where duplicates appear in different chunks
        # First chunk
        chunk1 = pd.DataFrame({
            "order_id": ["ORD-100", "ORD-101"],
            "product_name": ["Widget1", "Widget2"],
            "category": ["Electronics", "Electronics"],
            "quantity": [1, 1],
            "unit_price": [10.0, 10.0],
            "discount_percent": [0.0, 0.0],
            "region": ["Mumbai", "Thailand"],
            "sale_date": ["2023-01-01", "2023-01-01"],
            "customer_email": ["user1@example.com", "user2@example.com"],
        })
        
        # Second chunk with duplicate ORD-100
        chunk2 = pd.DataFrame({
            "order_id": ["ORD-100", "ORD-102"],  # ORD-100 is duplicate
            "product_name": ["DuplicateWidget", "Widget3"],
            "category": ["Electronics", "Electronics"],
            "quantity": [5, 1],
            "unit_price": [50.0, 10.0],
            "discount_percent": [0.1, 0.0],
            "region": ["Singapore", "Philippines"],
            "sale_date": ["2023-01-02", "2023-01-01"],
            "customer_email": ["duplicate@example.com", "user3@example.com"],
        })
        
        # Combine and save
        combined = pd.concat([chunk1, chunk2], ignore_index=True)
        csv_path = tmp_path / "test_cross_chunk_duplicates.csv"
        combined.to_csv(csv_path, index=False)
        
        clean_path, rejected_path = clean_csv_to_parquet(
            csv_path,
            config=CleanConfig(chunk_size=2, save_rejected_rows=True)  # Force 2 chunks
        )
        
        # Should have 3 clean records (ORD-100 first, ORD-101, ORD-102)
        clean_df = pd.read_parquet(clean_path)
        assert len(clean_df) == 3
        clean_order_ids = set(clean_df["order_id"])
        assert clean_order_ids == {"ORD-100", "ORD-101", "ORD-102"}
        
        # The first ORD-100 should be kept
        ord_100_clean = clean_df[clean_df["order_id"] == "ORD-100"]
        assert len(ord_100_clean) == 1
        assert ord_100_clean.iloc[0]["product_name"] == "Widget1"
        assert ord_100_clean.iloc[0]["region"] == "Mumbai"
        
        # Should have 1 rejected record (duplicate ORD-100)
        rejected_df = pd.read_csv(rejected_path, keep_default_na=False, na_values=[""])
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]["order_id"] == "ORD-100"
        assert rejected_df.iloc[0]["rejection_reason"] == "duplicate_order_id"
        # Should preserve original data of the duplicate
        assert rejected_df.iloc[0]["product_name"] == "DuplicateWidget"
        assert rejected_df.iloc[0]["region"] == "Singapore"
    
    def test_na_region_with_duplicates(self, tmp_path):
        """Test that NA regions work correctly even with duplicate order IDs."""
        
        raw_frame = pd.DataFrame({
            "order_id": ["ORD-999", "ORD-999"],
            "product_name": ["NAWidget1", "NAWidget2"],
            "category": ["Electronics", "Electronics"],
            "quantity": [1, 1],
            "unit_price": [10.0, 10.0],
            "discount_percent": [0.0, 0.0],
            "region": ["Mumbay", "Mumbay"],  # Both have misspelled region
            "sale_date": ["2023-01-01", "2023-01-01"],
            "customer_email": ["na1@example.com", "na2@example.com"],
        })
        
        csv_path = tmp_path / "test_na_duplicates.csv"
        raw_frame.to_csv(csv_path, index=False)
        
        clean_path, rejected_path = clean_csv_to_parquet(
            csv_path,
            config=CleanConfig(chunk_size=10, save_rejected_rows=True)
        )
        
        # Should have 1 clean record with properly resolved Mumbay -> Mumbai
        clean_df = pd.read_parquet(clean_path)
        assert len(clean_df) == 1
        assert clean_df.iloc[0]["order_id"] == "ORD-999"
        assert clean_df.iloc[0]["region"] == "Mumbai"  # Should be resolved
        assert clean_df.iloc[0]["product_name"] == "NAWidget1"  # First one kept
        
        # Should have 1 rejected record for duplicate_order_id (not unknown_region)
        rejected_df = pd.read_csv(rejected_path, keep_default_na=False, na_values=[""])
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]["order_id"] == "ORD-999"
        assert rejected_df.iloc[0]["rejection_reason"] == "duplicate_order_id"
        assert rejected_df.iloc[0]["region"] == "Mumbay"  # Original value preserved
        assert rejected_df.iloc[0]["product_name"] == "NAWidget2"  # Second one rejected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])