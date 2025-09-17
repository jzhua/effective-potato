# Data Pipeline Dashboard

This repository hosts a lightweight data engineering pipeline that prepares a
synthetic e-commerce sales dataset for dashboarding. The implementation avoids
distributed compute engines (e.g. Spark) in favour of Python-only components
that can scale via chunked processing.

## Project Structure

- `data_pipeline/` – core Python package
  - `generation/` – synthetic data builders
  - `cleaning/` – CSV->parquet normalisation, validation, and anomaly flags
  - `aggregations/` – metric builders that feed downstream dashboards
- `scripts/` – CLI entry points (`generate_data`, `clean_data`, lookup builders)
- `data/` – workspace for inputs (`input/`), cleaned parquet (`clean/`), lookups (`lookups/`), and derived outputs (`aggregations/`, `rejected/`)
- `tests/` – pytest coverage for cleaning and aggregation behaviour

## Quick Start

The end-to-end flow will eventually be automated, but while things are in flux
you can run each step manually. The commands below use `uv run` so the project
virtual environment is activated automatically.

```bash
# 1. Generate dirty or clean synthetic input data
uv run generate-data --rows 100000

# 2. Clean the raw CSV into a parquet dataset
uv run clean-data --input data/input/dirty_100m.csv

# 3. Build aggregated parquet artefacts for the dashboard
uv run build-aggregations --cleaned data/clean/raw_ecommerce_data_clean.parquet
```

All scripts accept additional flags (run with `--help`) for customising paths
or tuning chunk sizes.

If the source data drifts, regenerate canonical category and region lists with:

```bash
uv run build-category-lookup data/input/dirty_1m.csv
uv run build-region-lookup data/input/dirty_1m.csv
uv run build-region-map data/input/dirty_1m.csv
# Or run everything (with tests) in one pass
uv run update-lookups data/input/dirty_1m.csv
```

Each command writes a JSON file under `data/lookups/`, which doubles as a
human-editable list—feel free to hand-fix or expand entries before the next
cleaning run. The repository ships with a starter `region_map.json` that
captures the most common raw-to-canonical mappings we've encountered. When you
run the generator against new data, any unmatched values are appended with the
`UNKNOWN` label—review those rows, decide the correct canonical region (or leave
them as `UNKNOWN` to force rejection), and rerun the cleaner.

(The motivation here is that I'm operating under an assumption that canonical names
are not available anywhere, so they have to be generated via the raw data via different ways)


## Data cleaning rules

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




## Dashboard

Launch the exploratory dashboard once aggregations exist to review trends and track records that need human intervention.

```bash
uv run run-dashboard
```

The `data-dashboard` app (source in `data_dashboard/`) reads parquet files directly from `data/aggregations/` on every refresh and surfaces rejected CSVs from `data/rejected/`. As new aggregation artefacts are added, they appear automatically in the “Aggregations Explorer” tab, while anomaly records and rejected orders are reviewed separately in the “Anomalies & Rejections” tab.

## Roadmap

- Flesh out automated orchestration (Makefile or Taskfile)
- Add validation/tests for each pipeline stage
- Implement the dashboard application that reads the aggregation outputs
- Document data schemas and downstream contract expectations

## Development Notes

- Python version: 3.10+
- Key dependencies: `pandas`, `pyarrow`, `psutil`
- Data files are ignored by version control; generate them locally per run

Further documentation will be added as the pipeline and dashboard mature.
