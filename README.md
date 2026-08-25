# Housing Data Lakehouse

A portfolio-ready data engineering project that converts reproducible housing sales into analytics-ready datasets with PySpark and a Bronze/Silver/Gold architecture.

## Architecture

```text
Synthetic housing source
        |
        v
Bronze JSONL snapshots
        |
        v
Silver partitioned Parquet  -- typed, normalized, deduplicated, validated
        |
        v
Gold partitioned Parquet    -- annual city and state market KPIs
```

See [docs/architecture.md](docs/architecture.md) for layer contracts and processing details.

## Current capabilities

- Generates deterministic housing sales across five US markets
- Writes atomic Bronze snapshots with source and UTC ingestion metadata
- Applies an explicit Spark schema and normalizes city, state, and property type
- Filters invalid records and keeps the latest record for each property ID
- Derives price per square foot, property age, sale year, and sale month
- Produces Gold sales volume, median/average price, price-per-square-foot, and size KPIs
- Writes partitioned Parquet datasets for efficient analytical reads
- Reports Bronze, Silver, and Gold row counts for every run
- Validates commits with Ruff, pytest, and local Spark integration tests

## Stack

Python 3.11, PySpark 3.5, JSONL, Parquet, pytest, Ruff, and GitHub Actions. The design runs locally and remains portable to Databricks and cloud object storage.

## Quick start

Java 17 and Python 3.11 are recommended.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Generate only a Bronze snapshot
housing-lakehouse --rows 100 --seed 42

# Run Bronze, Silver, and Gold end to end
housing-lakehouse --rows 100 --seed 42 --full

pytest
```

Generated data is written under `data/` and ignored by Git.

## Layer outputs

| Layer | Format | Purpose |
|---|---|---|
| Bronze | JSONL | Immutable source-shaped snapshots |
| Silver | Parquet, partitioned by year/state | Clean property-level sales |
| Gold | Parquet, partitioned by year/state | City-level annual market KPIs |

## Repository layout

```text
src/housing_lakehouse/
  ingestion/             Reproducible source ingestion
  quality/               Source-boundary quality checks
  transformations/       Silver and Gold Spark logic
  pipeline.py            Medallion orchestration and audit counts
  cli.py                 Local command-line interface
tests/                   Unit and Spark integration tests
docs/                    Architecture documentation
.github/workflows/       Continuous integration
```

## Delivery roadmap

- [x] Establish project structure and packaging
- [x] Add reproducible synthetic housing ingestion
- [x] Add Bronze boundary data-quality validation
- [x] Implement typed Bronze-to-Silver transformations
- [x] Build Gold market metrics
- [x] Add an end-to-end local Spark pipeline
- [ ] Persist detailed audit and rejected-record metrics
- [ ] Add incremental processing and idempotency tests
- [ ] Add sample output previews and an architecture image
- [ ] Add Databricks job configuration and cloud deployment guidance

## Design principles

The pipeline is deterministic, idempotent for a given input snapshot, configuration-driven, testable, and safe to demonstrate without credentials or committed datasets.
