# Housing Data Lakehouse

A portfolio-ready data engineering project that turns reproducible housing records into analytics-ready datasets using a medallion architecture.

## Architecture

```text
Housing source -> Bronze (raw) -> Silver (cleaned) -> Gold (analytics)
                                      |
                               Data quality checks
```

- **Bronze:** immutable JSONL source snapshots with ingestion metadata
- **Silver:** typed, standardized, deduplicated housing records
- **Gold:** market-level metrics and reporting datasets
- **Quality:** schema, null, uniqueness, coordinate, and business-rule validation

## Current capabilities

- Generates realistic housing sales across five US markets with a deterministic seed
- Records lineage fields including source and UTC ingestion time
- Writes atomic Bronze snapshots so partial files are never published
- Rejects duplicate IDs, missing fields, invalid prices, sizes, counts, and coordinates
- Runs locally through a CLI with no API keys or downloaded datasets
- Validates commits and pull requests with Ruff and pytest in GitHub Actions

## Stack

Python 3.11, PySpark, JSONL/Parquet, pytest, Ruff, and GitHub Actions. The project runs locally first and is designed to remain portable to Databricks and cloud object storage.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
housing-lakehouse --rows 100 --seed 42
pytest
```

The generated snapshot is written under `data/bronze/`. Local data is ignored by Git.

## Repository layout

```text
src/housing_lakehouse/
  ingestion/             Reproducible source ingestion
  quality/               Data contract and business-rule checks
  cli.py                 Local pipeline orchestration
tests/                   Unit and pipeline tests
.github/workflows/       Continuous integration
data/                    Generated medallion layers (ignored)
```

## Delivery roadmap

- [x] Establish project structure and packaging
- [x] Add reproducible synthetic housing ingestion
- [x] Add Bronze boundary data-quality validation
- [x] Add a runnable CLI and continuous integration
- [ ] Implement PySpark Bronze-to-Silver standardization
- [ ] Build Gold market metrics and dimensional outputs
- [ ] Add Spark integration tests and audit metrics
- [ ] Add sample outputs and detailed architecture documentation

## Design principles

The pipeline is deterministic, idempotent, configuration-driven, and safe to demonstrate without committing credentials or large datasets.
