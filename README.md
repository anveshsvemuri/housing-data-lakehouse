# Housing Data Lakehouse

A portfolio-ready data engineering project that turns reproducible housing records into analytics-ready datasets using a medallion architecture.

## Architecture

```text
Housing source -> Bronze (raw) -> Silver (cleaned) -> Gold (analytics)
                                      |
                               Data quality checks
```

- **Bronze:** immutable source snapshots with ingestion metadata
- **Silver:** typed, standardized, deduplicated housing records
- **Gold:** market-level metrics and reporting datasets
- **Quality:** schema, null, uniqueness, and business-rule validation

## Planned stack

Python, PySpark, Delta-style Parquet tables, pytest, and GitHub Actions. The project is designed to run locally first and remain portable to Databricks and cloud object storage.

## Repository layout

```text
src/housing_lakehouse/   Application package
tests/                   Unit and integration tests
config/                  Environment-safe pipeline configuration
data/                    Local generated data (ignored by Git)
.github/workflows/       Continuous integration
```

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Delivery roadmap

- [x] Establish project structure and packaging
- [ ] Add reproducible housing data generator and ingestion
- [ ] Implement Bronze, Silver, and Gold transformations
- [ ] Add data-quality validation and audit metrics
- [ ] Add unit/integration tests and CI
- [ ] Add runnable CLI, sample outputs, and architecture documentation

## Design principles

The pipeline will be deterministic, idempotent, configuration-driven, and safe to demonstrate without committing credentials or large datasets.
