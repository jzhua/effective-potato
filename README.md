# Data Pipeline + Dashboard

This repository hosts a lightweight data engineering pipeline that prepares a
synthetic e-commerce sales dataset for dashboarding, as well as a dashboard
application.

## Project Structure

- `data_dashboard/` – Python dashboard app
- `data_pipeline/` – core Python package
  - `generation/` – synthetic data builders
  - `cleaning/` – CSV->parquet conversion, data normalisation+validation
  - `aggregations/` – metric builders that feed downstream dashboards
- `data/` – workspace for inputs (`input/`), cleaned parquet (`clean/`), lookups (`lookups/`), and derived outputs (`aggregations/`, `rejected/`)
- `scripts/` – CLI entry points (`generate_data`, `clean_data`, lookup builders)
- `tests/` – pytest coverage for cleaning and aggregation behaviour

## Quick Start

The end-to-end flow has not been automated. The individual steps are documented below.
The commands below use `uv run` so the project virtual environment is activated automatically.

```bash
# 1. Generate dirty or clean synthetic input data
uv run generate-data --rows 100000

# 2. Clean the raw CSV into a parquet dataset
uv run clean-data --input data/input/dirty_100m.csv

# 3. Build aggregated parquet artefacts for the dashboard
uv run build-aggregations --cleaned data/clean/raw_ecommerce_data_clean.parquet

# 4. Launch the interactive dashboard
uv run run-dashboard --debug
```

All scripts accept additional flags (run with `--help`) for customising paths
or tuning chunk sizes.

## Data pipeline

1. **Generate raw data** (`scripts/generate_data.py`)
     Produces either clean data or intentionally messy rows (typos, duplicate IDs, malformed fields) to exercise the pipeline.

     Outputs: a csv in `data/input/`
2. **Clean & normalise** (`scripts/clean_data.py`)
     Input: a csv from data/input/

     Output: 
        a cleaned parquet dataset in `data/clean/`,
        a rejected-rows CSV in `data/rejected/`

     Streams the CSV in chunks and tries to clean up the data as much as possible. This is documented further in the [Data Cleaning](#data-cleaning) section.

3. **Build aggregations** (`scripts/build_aggregations.py`)
    input: parquet in `data/clean/`
    output: multiple parquet datasets in `data/aggregations/`

   - Reads the cleaned parquet and materialises monthly sales trends, top-product rankings, regional
     performance, category/discount summaries, and anomaly snapshots as parquet files inside
     `data/aggregations/`.

## Data lookup files

The cleanup logic requires a list of canonical categories and regions. Since those are unavailable, it attempts to
create one from a dirty dataset - in other words, a CSV of sales orders - either relying on heuristics or human
intervention to canonicalize the data. See [Data Cleaning](#data-cleaning).

The canonical lists are persisted and checked into the repo, but they can be regenerated with some human intervention.
Example invocations would be:
```bash
uv run build-category-lookup data/input/dirty_1m.csv
uv run build-region-lookup data/input/dirty_1m.csv
uv run build-region-map data/input/dirty_1m.csv
# Or run everything (with tests) in one pass
uv run update-lookups data/input/dirty_1m.csv
```

## Synthetic data generation

The generator in `scripts/generate_data.py` builds each row using a product catalogue and
category-specific settings. Passing `--clean` keeps values consistent; omitting it introduces
intentional quality issues for testing the cleaners. Field behaviour:

- `order_id`: sequential `ORD-#########` strings; in dirty mode ~1% re-use a recently generated ID to create duplicates.
- `product_name`: sampled from curated variations per base product; dirty runs add typos to ~5% of names.
- `category`: follows the product’s canonical category. Dirty data stays correct ~90% of the time, sprinkles typos on 20% of those rows, and mislabels the rest using random categories.
- `quantity`: drawn from product-specific distributions. Clean data sticks to positive integers; dirty data mixes in zero, negative, oversized, and non-numeric tokens (e.g. `"two"`, `"many"`).
- `unit_price`: derived from the category’s price band (see `CATEGORY_PRICE_RANGES`) with a bias toward cheaper brackets; values are rounded to cents.
- `discount_percent`: uses `CATEGORY_DISCOUNT_RANGES` to decide both the odds of a discount and its range. Dirty runs occasionally emit extreme values (85–99% off) and about 1% outright bad numbers (negative or >1).
- `region`: random choice from `REGIONS`, which intentionally mixes canonical names with common typos (`"north america"`, `"Aisa"`, etc.).
- `sale_date`: spans the last ~2 years with recent dates favoured and seasonal nudges; dirty mode mixes in blanks and multiple formats, while clean mode sticks to ISO `YYYY-MM-DD`.
- `customer_email`: built from cached first/last names plus common domains. Dirty data leaves ~15% blank and introduces malformed addresses (~5%).
- `revenue`: recomputed as `unit_price * quantity * (1 - discount)` after normalising quantity to a non-negative integer and clamping the discount between 0 and 1.


## <a name="data-cleaning">Data cleaning rules</a>

The cleaner aggressively normalises fields and drops any rows that cannot be reconciled
with our canonical category/region lists or basic business sanity checks. Expect to tune
the lookup JSONs and re-run cleaning when source feeds drift.

### Order identity & deduplication

- Blank `order_id` or `product_name` rows are rejected up front.
- Duplicates are removed both within a chunk and across chunks by tracking seen order IDs.
- A configurable `CleanConfig.drop_zero_quantity` flag (defaults to `True`) removes rows where the cleaned quantity is zero.

### Category normalisation

- Known categories come from `data/lookups/common_categories.json`; the lookup builder script populates this list from frequent values in sample CSVs.
- Values are lowercased and matched against the canonical list. A Levenshtein-distance fuzzy match (threshold of 2) rescues minor misspellings; anything outside that range is rejected as `unknown_category`.

### Region reconciliation

- `data/lookups/common_regions.json` provides the accepted region values and `data/lookups/region_map.json` maps raw inputs to those canonical names.
- Lookups are case-insensitive, but mappings to `UNKNOWN` or unmapped values are rejected as `unknown_region`.
- Because region naming drifts more often, review rejected rows, update the mapping JSONs, and re-run cleaning when necessary.

### Lookup stewardship

- Keep `common_categories.json`, `common_regions.json`, and `region_map.json` in sync with production feeds; when rejection counts spike, rerun `uv run update-lookups data/input/dirty_1m.csv` (or the individual lookup builders) and hand-tune any edge cases before re-running `clean-data`.
- When generating new lookup artifacts, commit the curated JSON changes so teammates pick them up on their next run.
- Rejected rows enumerate the exact `rejection_reason`; use those CSVs to prioritise new mappings or cleaning rules before the next pipeline run.

### Numeric fields & discounts

- Quantities are coerced to non-negative integers (`Int64`), prices to non-negative floats, and discounts are clamped between 0.0 and 1.0.
- Orders with a cleaned unit price outside the `$0–50k` range are dropped with `invalid_unit_price`.
- Rows with discounts over 80% survive but are tagged with an `anomaly_flag` of `heavy_discount` for downstream auditing.

### Date parsing

- Sale dates accept multiple formats (`%Y-%m-%d`, `%m/%d/%Y`, `%d-%m-%Y`, `%Y/%m/%d`). Anything else is rejected as `invalid_sale_date`.
- Valid dates must fall within the last 20 years and at most one year into the future; out-of-range rows are labelled `sale_date_out_of_range`.
- Accepted timestamps are serialised as Unix epoch seconds to avoid timezone drift.

### Email, revenue, and final checks

- Customer emails are lowercased and validated via a conservative regex; non-matching addresses are set to null.
- Revenue is recalculated from the cleaned quantity, unit price, and discount, rounded to two decimals, and must fall within `$0–1M`. Failures are rejected with `invalid_calculated_revenue`.
- Rejected rows (with their `rejection_reason`) are written to `data/rejected/` when `CleanConfig.save_rejected_rows` remains enabled, making it easier to inspect edge cases and extend lookup logic.



## Roadmap

- Flesh out automated orchestration (Makefile or Taskfile)
- Add validation/tests for each pipeline stage
- Implement the dashboard application that reads the aggregation outputs
- Document data schemas and downstream contract expectations

## Development Notes

- Python version: 3.10+
- Key dependencies: `pandas`, `pyarrow`, `psutil`
- Data files are ignored by version control; generate them locally per run

## Data pipeline stats

These were run on a 2024 macbook air. Note that all of the scripts are (deliberately) performed using a single CPU thread; all
possible parallelism was disabled.

- Generate 100M row csv: ~20 mins
- Data cleanup/filtering: ~8 mins
- Create aggregations: ~9 mins
