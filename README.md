# Data Pipeline Dashboard

This repository hosts a lightweight data engineering pipeline that prepares a
synthetic e-commerce sales dataset for dashboarding. The implementation avoids
distributed compute engines (e.g. Spark) in favour of Python-only components
that can scale via chunked processing.

## Project Structure

- `data_pipeline_dashboard/` – core Python package
  - `generation/` – utilities for creating raw CSV files
  - `cleaning/` – routines that transform messy CSV exports into clean parquet
  - `aggregations/` – business metric builders consumed by the dashboard layer
- `scripts/` – command-line helpers that orchestrate each stage
- `data/` – storage for generated inputs, cleaned parquet, and derived metrics

## Quick Start

The end-to-end flow will eventually be automated, but while things are in flux
you can run each step manually. The commands below assume an environment where
`python` invokes your project interpreter; adapt as needed.

```bash
# 1. Generate dirty or clean synthetic input data
python -m scripts.generate_data --rows 100000

# 2. Clean the raw CSV into a parquet dataset
python -m scripts.clean_data --input data/input/raw_ecommerce_data.csv

# 3. Build aggregated parquet artefacts for the dashboard
python -m scripts.build_aggregations --cleaned data/clean/raw_ecommerce_data_clean.parquet
```

All scripts accept additional flags (run with `--help`) for customising paths
or tuning chunk sizes.

If the source data drifts, regenerate canonical category names with:

```bash
python -m scripts.build_category_lookup data/input/dirty_1m.csv
```

The command writes `data/lookups/common_categories.json`, which doubles as a
human-editable list—feel free to hand-fix or expand entries before the next
cleaning run.

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
