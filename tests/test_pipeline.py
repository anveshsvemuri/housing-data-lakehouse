import json
from datetime import UTC, datetime

from housing_lakehouse.ingestion import generate_housing_records, write_jsonl
from housing_lakehouse.pipeline import create_spark_session, run_medallion_pipeline


def test_local_spark_session_uses_portable_loopback_binding(spark):
    session = create_spark_session("housing-lakehouse-test-session")

    assert session.conf.get("spark.driver.host") == "127.0.0.1"
    assert session.conf.get("spark.driver.bindAddress") == "127.0.0.1"
    assert session.conf.get("spark.ui.enabled") == "false"


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
    manifest = json.loads(summary.audit_file.read_text())
    assert manifest["run_id"] == summary.run_id
    assert manifest["row_counts"] == {"bronze": 12, "silver": 12, "rejected": 0, "gold": summary.gold_rows}
    assert manifest["reconciled"] is True


def test_repeated_input_reuses_run_identity_and_manifest(spark, tmp_path):
    bronze_path = tmp_path / "bronze"
    records = generate_housing_records(
        4,
        seed=9,
        generated_at=datetime(2026, 1, 15, tzinfo=UTC),
    )
    write_jsonl(records, bronze_path / "housing.jsonl")
    arguments = {
        "bronze_path": bronze_path,
        "silver_path": tmp_path / "silver",
        "gold_path": tmp_path / "gold",
    }

    first = run_medallion_pipeline(spark, **arguments)
    second = run_medallion_pipeline(spark, **arguments)

    assert first.run_id == second.run_id
    assert first.audit_file == second.audit_file
    manifests = [
        path for path in (tmp_path / "audit").glob("*.json") if path.name != "processing-state.json"
    ]
    assert len(manifests) == 1


def test_incremental_run_merges_only_new_snapshots_and_then_skips(spark, tmp_path):
    bronze_path = tmp_path / "bronze"
    first_records = generate_housing_records(
        4,
        seed=9,
        generated_at=datetime(2026, 1, 15, tzinfo=UTC),
    )
    write_jsonl(first_records, bronze_path / "first.jsonl")
    arguments = {
        "bronze_path": bronze_path,
        "silver_path": tmp_path / "silver",
        "gold_path": tmp_path / "gold",
    }
    run_medallion_pipeline(spark, **arguments)

    newer_records = generate_housing_records(
        2,
        seed=9,
        generated_at=datetime(2026, 1, 16, tzinfo=UTC),
    )
    write_jsonl(newer_records, bronze_path / "second.jsonl")
    incremental = run_medallion_pipeline(spark, incremental=True, **arguments)
    unchanged = run_medallion_pipeline(spark, incremental=True, **arguments)

    assert incremental.processed_files == 1
    assert incremental.processed_bronze_rows == 2
    assert incremental.bronze_rows == 6
    assert incremental.silver_rows == 4
    assert incremental.rejected_rows == 2
    assert incremental.skipped is False
    assert unchanged.skipped is True
    assert unchanged.processed_files == 0
    assert unchanged.run_id == incremental.run_id
    assert spark.read.parquet(str(tmp_path / "silver")).count() == 4
    assert spark.read.parquet(str(tmp_path / "rejected")).count() == 2


def test_incremental_run_rejects_mutated_bronze_snapshot(spark, tmp_path):
    bronze_path = tmp_path / "bronze"
    source = bronze_path / "housing.jsonl"
    records = generate_housing_records(
        2,
        seed=9,
        generated_at=datetime(2026, 1, 15, tzinfo=UTC),
    )
    write_jsonl(records, source)
    arguments = {
        "bronze_path": bronze_path,
        "silver_path": tmp_path / "silver",
        "gold_path": tmp_path / "gold",
    }
    run_medallion_pipeline(spark, **arguments)
    source.write_text(source.read_text().replace('"sale_price":', '"sale_price": 1, "old_price":'))

    try:
        run_medallion_pipeline(spark, incremental=True, **arguments)
    except ValueError as error:
        assert "immutable Bronze inputs changed" in str(error)
    else:
        raise AssertionError("mutated Bronze input was accepted")
