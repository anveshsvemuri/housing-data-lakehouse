"""End-to-end Spark orchestration for medallion lakehouse layers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from housing_lakehouse.audit import (
    build_input_inventory,
    discover_input_files,
    fingerprint_files,
    read_processing_state,
    write_audit_manifest,
    write_processing_state,
)
from housing_lakehouse.transformations import (
    BRONZE_SCHEMA,
    build_market_metrics,
    build_silver_with_rejects,
)


@dataclass(frozen=True, slots=True)
class PipelineRunSummary:
    """Auditable row counts and output locations from a pipeline run."""

    bronze_rows: int
    silver_rows: int
    gold_rows: int
    rejected_rows: int
    silver_path: Path
    gold_path: Path
    rejected_path: Path
    run_id: str
    audit_file: Path
    processed_bronze_rows: int = 0
    processed_files: int = 0
    skipped: bool = False


def create_spark_session(app_name: str = "housing-data-lakehouse") -> SparkSession:
    """Create a small local Spark session suitable for development."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _merge_incremental_silver(
    existing: DataFrame,
    incoming: DataFrame,
    rejected_columns: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Merge new accepted rows and return records superseded across runs."""
    latest_record = Window.partitionBy("property_id").orderBy(
        F.col("ingested_at").desc_nulls_last(), F.col("sale_date").desc()
    )
    ranked = existing.unionByName(incoming).withColumn(
        "_record_rank", F.row_number().over(latest_record)
    )
    accepted = ranked.filter(F.col("_record_rank") == 1).drop("_record_rank")
    base_columns = [column for column in rejected_columns if column != "rejection_reasons"]
    superseded = (
        ranked.filter(F.col("_record_rank") > 1)
        .select(*base_columns)
        .withColumn("rejection_reasons", F.array(F.lit("duplicate_superseded")))
    )
    return accepted, superseded


def run_medallion_pipeline(
    spark: SparkSession,
    *,
    bronze_path: Path,
    silver_path: Path,
    gold_path: Path,
    rejected_path: Path | None = None,
    audit_path: Path | None = None,
    incremental: bool = False,
    state_file: Path | None = None,
) -> PipelineRunSummary:
    """Transform Bronze into Silver and Gold, optionally processing only new snapshots."""
    rejected_path = rejected_path or silver_path.parent / "rejected"
    audit_path = audit_path or silver_path.parent / "audit"
    state_file = state_file or audit_path / "processing-state.json"
    input_files = discover_input_files(bronze_path)
    if not input_files:
        raise ValueError(f"no Bronze JSONL inputs found under {bronze_path}")
    inventory = build_input_inventory(bronze_path)
    previous_state = read_processing_state(state_file)
    previous_inventory = previous_state["processed_files"]
    changed_files = sorted(
        name
        for name, fingerprint in inventory.items()
        if name in previous_inventory and previous_inventory[name] != fingerprint
    )
    if incremental and changed_files:
        names = ", ".join(changed_files)
        raise ValueError(f"immutable Bronze inputs changed after processing: {names}")

    root = bronze_path if bronze_path.is_dir() else bronze_path.parent
    selected_names = (
        sorted(set(inventory) - set(previous_inventory)) if incremental else sorted(inventory)
    )
    if incremental and not selected_names:
        counts = previous_state.get("row_counts", {})
        audit_file = Path(previous_state["audit_file"])
        return PipelineRunSummary(
            bronze_rows=counts.get("bronze", 0),
            silver_rows=counts.get("silver", 0),
            gold_rows=counts.get("gold", 0),
            rejected_rows=counts.get("rejected", 0),
            silver_path=silver_path,
            gold_path=gold_path,
            rejected_path=rejected_path,
            run_id=previous_state["last_run_id"],
            audit_file=audit_file,
            processed_bronze_rows=0,
            processed_files=0,
            skipped=True,
        )

    selected_files = tuple(root / name for name in selected_names)
    bronze = spark.read.schema(BRONZE_SCHEMA).json([str(path) for path in selected_files])
    run_id = fingerprint_files(selected_files)[:16]
    transformed = build_silver_with_rejects(bronze)
    silver = transformed.accepted
    rejected = transformed.rejected

    if incremental and silver_path.exists():
        existing_silver = spark.read.parquet(str(silver_path))
        silver, superseded = _merge_incremental_silver(
            existing_silver, silver, rejected.columns
        )
        rejected = rejected.unionByName(superseded)
        if rejected_path.exists():
            rejected = spark.read.parquet(str(rejected_path)).unionByName(rejected)

    silver = silver.cache()
    rejected = rejected.cache()
    gold = build_market_metrics(silver).cache()

    processed_bronze_rows = bronze.count()
    previous_bronze_rows = previous_state.get("row_counts", {}).get("bronze", 0)
    bronze_rows = (
        previous_bronze_rows + processed_bronze_rows if incremental else processed_bronze_rows
    )
    silver_rows = silver.count()
    gold_rows = gold.count()
    rejected_rows = rejected.count()

    silver.write.mode("overwrite").partitionBy("sale_year", "state").parquet(str(silver_path))
    gold.write.mode("overwrite").partitionBy("sale_year", "state").parquet(str(gold_path))
    rejected.write.mode("overwrite").parquet(str(rejected_path))

    silver.unpersist()
    gold.unpersist()
    rejected.unpersist()

    audit_file = write_audit_manifest(
        audit_path,
        run_id=run_id,
        payload={
            "run_id": run_id,
            "completed_at": datetime.now(UTC).isoformat(),
            "status": "succeeded",
            "mode": "incremental" if incremental else "full_refresh",
            "input_files": selected_names,
            "processed_bronze_rows": processed_bronze_rows,
            "row_counts": {
                "bronze": bronze_rows,
                "silver": silver_rows,
                "rejected": rejected_rows,
                "gold": gold_rows,
            },
            "reconciled": bronze_rows == silver_rows + rejected_rows,
            "outputs": {
                "silver": str(silver_path),
                "rejected": str(rejected_path),
                "gold": str(gold_path),
            },
        },
    )
    write_processing_state(
        state_file,
        {
            "version": 1,
            "processed_files": inventory,
            "last_run_id": run_id,
            "audit_file": str(audit_file),
            "row_counts": {
                "bronze": bronze_rows,
                "silver": silver_rows,
                "rejected": rejected_rows,
                "gold": gold_rows,
            },
        },
    )

    return PipelineRunSummary(
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        gold_rows=gold_rows,
        rejected_rows=rejected_rows,
        silver_path=silver_path,
        gold_path=gold_path,
        rejected_path=rejected_path,
        run_id=run_id,
        audit_file=audit_file,
        processed_bronze_rows=processed_bronze_rows,
        processed_files=len(selected_files),
    )
