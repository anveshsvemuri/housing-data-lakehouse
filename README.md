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
        |                         +--> Rejected Parquet -- reasons + superseded rows
        v
Gold partitioned Parquet    -- annual city and state market KPIs
                                  + Audit JSON + processing checkpoint
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
- Quarantines invalid and superseded records with machine-readable reasons
- Persists reconciled audit manifests and atomic input checkpoints
- Processes only unseen Bronze snapshots on incremental runs and safely skips no-op reruns
- Defines encrypted, versioned AWS lakehouse storage with Terraform
- Validates commits with Ruff, pytest, Spark integration tests, and Terraform

## Stack

Python 3.11, PySpark 3.5, JSONL, Parquet, Terraform, AWS S3, pytest, Ruff, and
GitHub Actions. The design runs locally and includes a safe infrastructure foundation for Databricks
and cloud object storage.

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

# On later runs, process only the new snapshot and merge it into Silver
housing-lakehouse --rows 100 --seed 43 --incremental

pytest
```

Generated data is written under `data/` and ignored by Git.

## Layer outputs

| Layer | Format | Purpose |
|---|---|---|
| Bronze | JSONL | Immutable source-shaped snapshots |
| Silver | Parquet, partitioned by year/state | Clean property-level sales |
| Gold | Parquet, partitioned by year/state | City-level annual market KPIs |
| Rejected | Parquet | Invalid and superseded rows with rejection reasons |
| Audit | JSON | Run identity, inputs, outputs, counts, mode, and reconciliation |

See a real 25-row run in [docs/sample-output.md](docs/sample-output.md).

## Repository layout

```text
src/housing_lakehouse/
  ingestion/             Reproducible source ingestion
  quality/               Source-boundary quality checks
  transformations/       Silver and Gold Spark logic
  pipeline.py            Full-refresh/incremental orchestration and audit counts
  cli.py                 Local command-line interface
tests/                   Unit and Spark integration tests
docs/                    Architecture documentation
deployment/              Databricks job template
infrastructure/terraform/ AWS lakehouse storage infrastructure
.github/workflows/       Continuous integration
```

## Delivery roadmap

- [x] Establish project structure and packaging
- [x] Add reproducible synthetic housing ingestion
- [x] Add Bronze boundary data-quality validation
- [x] Implement typed Bronze-to-Silver transformations
- [x] Build Gold market metrics
- [x] Add an end-to-end local Spark pipeline
- [x] Persist detailed audit and rejected-record metrics
- [x] Add incremental processing and idempotency tests
- [x] Add sample output previews and an architecture diagram
- [x] Add a Databricks job template and cloud deployment guidance
- [x] Add validated Terraform for secure AWS object storage
- [ ] Add object-storage adapters and table-format support for production-scale deployment

## Design principles

The pipeline is deterministic, idempotent for a given input snapshot, configuration-driven, testable, and safe to demonstrate without credentials or committed datasets.

Terraform is intentionally separate from application execution. It creates the storage and access
policy, while [docs/terraform.md](docs/terraform.md) explains planning, applying, remote state, and
the remaining adapter work required before the pipeline writes directly to S3.
