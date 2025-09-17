# Product-Category Canonicalization Plan

## Overview

This document outlines the plan to implement product-category canonicalization to fix cases where products have incorrect category assignments in the CSV data. The solution follows the existing canonicalization pattern used for regions.

## Problem Statement

Products in the generated CSV data sometimes have incorrect category assignments due to the intentional data quality issues introduced during generation:
- 10% of products get completely wrong categories (e.g., "iPhone 15" → "Sports")
- 20% of correct categories get typos (e.g., "Electronics" → "Electroncs")
- This causes downstream issues like KeyError in discount calculations and poor data quality

## Solution Architecture

### 1. Single Lookup File
- **`data/lookups/product_category_map.json`**: Maps specific product names to their canonical categories
- Follows the same pattern as existing `region_map.json`

### 2. Lookup Generation Script
- **`scripts/build_product_category_map.py`**: Analyzes CSV input to extract product-category relationships from actual data

### 3. Integration in Cleaning Pipeline
- Add product-category canonicalization step in `clean_sales_data.py` similar to existing region canonicalization
- **Option B**: Fix at cleaning time (preserves generation logic, corrects during data processing)

## Implementation Details

### Phase 1: CSV Analysis Strategy

**`scripts/build_product_category_map.py`** will:

1. **Data Extraction**: Read CSV files and extract `product_name` + `category` pairs
2. **Frequency Analysis**: Count category occurrences for each unique product name
3. **Confidence Calculation**: Determine the "correct" category based on majority voting
4. **Threshold Filtering**: Only include mappings with high confidence (e.g., >70% agreement)

**Analysis Logic:**
```python
# For each unique product_name:
# 1. Count category occurrences: {"iPhone 12": {"Electronics": 847, "Sports": 23, "Clothng": 5}}
# 2. Pick most frequent category as canonical (Electronics in this case)
# 3. Only include if confidence > threshold (847/875 = 96.8% > 70%)
# 4. Handle case variations and normalization
```

### Phase 2: Lookup File Generation

**Output Format:**
```json
{
  "iPhone 12": "Electronics",
  "iphone 12": "Electronics", 
  "IPHONE 12": "Electronics",
  "MacBook Air": "Electronics",
  "Nike Air Max": "Clothing",
  "nike air max": "Clothing",
  "Coffee Maker": "Kitchen",
  "Yoga Mat": "Sports"
}
```

**Features:**
- Case-insensitive mappings (multiple case variations for each product)
- Only products with high confidence mappings included
- Canonical categories must exist in `common_categories.json`

### Phase 3: Cleaning Integration

**In `clean_sales_data.py`:**

1. **Startup Loading**: Load product category map similar to region map loading
2. **New Function**: Add `_resolve_product_category()` with case-insensitive lookup
3. **Pipeline Integration**: Add correction step after existing category normalization
4. **Fallback Behavior**: Keep original category if product not found in map

**Integration Point:**
```python
# After existing category cleaning in _clean_chunk()
raw_products = _normalise_string(data.get("product_name"))
corrected_categories = []
for product, current_category in zip(raw_products, data["category"]):
    corrected = _resolve_product_category(product, current_category)
    corrected_categories.append(corrected or current_category)

# Update category column with corrected values
data = data.assign(category=pd.Series(corrected_categories, index=data.index))
```

## CSV Analysis Approach

The lookup generation script will use the following methodology:

### 1. Data Aggregation
- Scan all provided CSV files
- Extract unique `(product_name, category)` pairs
- Count occurrences of each category per product

### 2. Confidence Filtering
- Calculate confidence as: `max_category_count / total_occurrences_for_product`
- Only include products where confidence > threshold (default: 70%)
- This filters out ambiguous or genuinely miscategorized products

### 3. Normalization
- Handle case variations (iPhone, iphone, IPHONE)
- Generate common case variations for lookup efficiency
- Validate mapped categories against canonical category list

### 4. Quality Validation
- Ensure all mapped categories exist in `common_categories.json`
- Log statistics about confidence levels and rejection rates
- Provide summary of products that couldn't be confidently mapped

## Expected Benefits

✅ **Consistent with existing patterns**: Uses same JSON lookup approach as regions  
✅ **Data-driven**: Built from actual CSV patterns, not assumptions  
✅ **Maintainable**: Easy to regenerate as new data becomes available  
✅ **Conservative**: Only corrects products with high confidence  
✅ **Backwards compatible**: Doesn't break existing generation logic  
✅ **Cleaning-time fix**: Preserves intentional data quality issues for testing while fixing real problems

## File Structure

```
data/lookups/
├── common_categories.json      # (existing)
├── common_regions.json         # (existing) 
├── region_map.json            # (existing)
└── product_category_map.json  # (new) product→category mappings

scripts/
├── build_product_category_map.py  # (new) CSV analysis script
└── update_lookups.py              # (existing, extend to include products)
```

## Usage Workflow

1. **Generate Data**: Create CSV with intentional category errors
2. **Build Lookup**: `uv run build-product-category-map data/input/sample.csv`
3. **Clean Data**: Existing cleaning pipeline automatically applies corrections
4. **Iterate**: Regenerate lookup as new data patterns emerge

This approach ensures systematic correction of obvious product-category mismatches while maintaining the flexibility to handle real-world data inconsistencies.