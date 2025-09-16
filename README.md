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
uv run clean-data --input data/input/raw_ecommerce_data.csv

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
and adjust those rows before re-running the cleaner.

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
