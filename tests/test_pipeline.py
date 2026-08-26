from datetime import UTC, datetime

from housing_lakehouse.ingestion import generate_housing_records, write_jsonl
from housing_lakehouse.pipeline import run_medallion_pipeline


def test_run_medallion_pipeline_writes_queryable_layers(spark, tmp_path):
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    gold_path = tmp_path / "gold"
    records = generate_housing_records(
        12,
        seed=18,
        generated_at=datetime(2026, 1, 15, tzinfo=UTC),
    )
    write_jsonl(records, bronze_path / "housing.jsonl")

    summary = run_medallion_pipeline(
        spark,
        bronze_path=bronze_path,
        silver_path=silver_path,
        gold_path=gold_path,
    )

    assert summary.bronze_rows == 12
    assert summary.silver_rows == 12
    assert summary.gold_rows > 0
    assert summary.rejected_rows == 0
    assert spark.read.parquet(str(silver_path)).count() == 12
    assert spark.read.parquet(str(gold_path)).count() == summary.gold_rows
    assert spark.read.parquet(str(summary.rejected_path)).count() == 0
