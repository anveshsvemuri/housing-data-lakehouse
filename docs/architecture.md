# Architecture and data contracts

## Processing flow

```mermaid
flowchart TD
    A["Synthetic source"] --> B["Bronze JSONL"]
    B --> G{"Checkpoint: unseen snapshot?"}
    G -- "yes" --> C["Silver PySpark transformation"]
    G -- "no" --> H["No-op: preserve outputs"]
    C --> D["Silver Parquet"]
    C --> R["Rejected Parquet"]
    D --> E["Gold KPI aggregation"]
    E --> F["Gold Parquet"]
    D --> I["Audit manifest + checkpoint"]
    R --> I
    F --> I
```

## Bronze contract

Bronze preserves the generated source record and adds `source` and `ingested_at` lineage fields. Snapshots are first written to a temporary file and atomically renamed, preventing partial input publication.

## Silver contract

Silver applies an explicit Spark schema, parses dates and timestamps, standardizes categorical values, and enforces core business rules. Duplicate property IDs are resolved by retaining the record with the latest ingestion timestamp. Derived fields include:

- `price_per_sqft`
- `property_age_at_sale`
- `sale_year`
- `sale_month`

Silver is stored as Parquet partitioned by `sale_year` and `state`.

Rejected records preserve the normalized source fields and a `rejection_reasons` array. The
quarantine contains both business-rule failures and valid older versions displaced during
property-level deduplication.

## Gold contract

Gold contains annual city/state metrics:

- distinct properties sold
- total sales volume
- average and median sale price
- average price per square foot
- average property size
- minimum and maximum sale price

Gold uses the same year/state partition strategy, supporting efficient time- and geography-scoped reads.

## Reliability

- Deterministic seeded input supports reproducible tests.
- Explicit schemas prevent accidental type drift.
- Latest-record deduplication makes repeated source records predictable.
- Content hashes enforce immutable Bronze inputs after processing.
- An atomic checkpoint records processed relative paths, hashes, and materialized row counts.
- Incremental runs read only unseen snapshots, merge accepted records into Silver, quarantine
  displaced versions, and recompute Gold from the current Silver view.
- Unchanged incremental reruns are no-ops; checkpoint publication happens only after output and
  audit writes succeed.
- Audit manifests reconcile cumulative Bronze rows with current Silver plus rejected rows.
- Unit and local Spark integration tests run in CI.

The local implementation uses overwrite-mode Parquet for compact, dependency-light demos. A
production deployment would normally replace this with Delta Lake or Apache Iceberg transactions
and object-storage-backed checkpoints.
