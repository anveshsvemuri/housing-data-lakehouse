"""End-to-end Spark orchestration for medallion lakehouse layers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

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


def create_spark_session(app_name: str = "housing-data-lakehouse") -> SparkSession:
    """Create a small local Spark session suitable for development."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run_medallion_pipeline(
    spark: SparkSession,
    *,
    bronze_path: Path,
    silver_path: Path,
    gold_path: Path,
    rejected_path: Path | None = None,
) -> PipelineRunSummary:
    """Transform Bronze JSONL into partitioned Silver and Gold Parquet datasets."""
    bronze = spark.read.schema(BRONZE_SCHEMA).json(str(bronze_path))
    rejected_path = rejected_path or silver_path.parent / "rejected"
    transformed = build_silver_with_rejects(bronze)
    silver = transformed.accepted.cache()
    rejected = transformed.rejected.cache()
    gold = build_market_metrics(silver).cache()

    bronze_rows = bronze.count()
    silver_rows = silver.count()
    gold_rows = gold.count()
    rejected_rows = rejected.count()

    silver.write.mode("overwrite").partitionBy("sale_year", "state").parquet(str(silver_path))
    gold.write.mode("overwrite").partitionBy("sale_year", "state").parquet(str(gold_path))
    rejected.write.mode("overwrite").parquet(str(rejected_path))

    silver.unpersist()
    gold.unpersist()
    rejected.unpersist()

    return PipelineRunSummary(
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        gold_rows=gold_rows,
        rejected_rows=rejected_rows,
        silver_path=silver_path,
        gold_path=gold_path,
        rejected_path=rejected_path,
    )
