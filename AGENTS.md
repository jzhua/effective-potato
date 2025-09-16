# Repository Guidelines

## Project Structure & Module Organization
Core logic lives in `data_pipeline/`, split into `generation/` (synthetic data builders), `cleaning/` (CSV-to-parquet transforms), and `aggregations/` (metric calculations). `scripts/` exposes orchestration entry points mirroring those stages. Keep scratch artefacts local—never depend on `/tmp` or tracked helpers for persistence. All datasets, whether raw CSV or derived parquet, belong under `data/` in the matching `input/`, `clean/`, and `aggregations/` folders.

## Build, Test, and Development Commands
Set up Python 3.10+ deps with `uv sync` or `pip install -e .[dev]`. Run pipeline stages via `uv run python -m scripts.generate_data --rows 100000`, `uv run python -m scripts.clean_data --input data/input/raw_ecommerce_data.csv`, and `uv run python -m scripts.build_aggregations --cleaned data/clean/raw_ecommerce_data_clean.parquet`. Execute the test suite with `uv run pytest`. Lint before each commit using `uv run ruff check .` to catch formatting and import issues early.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation and snake_case for modules, functions, and variables. Keep public functions pure where possible so they can be exercised with small in-memory DataFrames. When adding configuration knobs, surface them through `data_pipeline/settings.py` to keep defaults centralized. Accept CSV and parquet paths as Path objects rather than raw strings for clarity and testability.

## Testing Guidelines
Add `pytest` cases alongside the code in a mirrored `tests/` directory (e.g., `tests/cleaning/test_clean_sales_data.py`). Name tests `test_<behavior>()` and use fixtures that build minimal in-memory DataFrames rather than writing to `/tmp`. Cover parsing edge cases (currency, timestamps, null handling) and aggregation math. Run `uv run pytest --maxfail=1` before opening a pull request.

## Commit & Pull Request Guidelines
Use short, imperative commit titles in the form `type: summary` (e.g., `fix: guard empty discount values`) followed by a concise body when context is needed. Squash noisy work-in-progress commits before publishing. Pull requests should describe the change set, reference any issue IDs, note affected data artifacts, and include screenshots or sample output when altering aggregation schemas. Confirm that lint and tests pass and list the exact commands run in the PR description.

## Data & Configuration Notes
Never commit generated CSV or parquet outputs; they are ignored intentionally. `/tmp` is reserved for the operating system, so keep repository-specific scratch data within `data/` or local tooling. Document new environment variables or settings in `data_pipeline/settings.py` and surface safe defaults. When sharing sample datasets for review, downsample and place them under `data/input/` with a descriptive filename so teammates can reproduce the scenario.
