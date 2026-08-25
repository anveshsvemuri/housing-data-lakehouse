import json

from housing_lakehouse.cli import main, run_pipeline


def test_run_pipeline_writes_valid_bronze_snapshot(tmp_path):
    destination = run_pipeline(rows=4, seed=12, data_root=tmp_path)

    assert destination.parent == tmp_path / "bronze"
    rows = [json.loads(line) for line in destination.read_text().splitlines()]
    assert len(rows) == 4
    assert {row["source"] for row in rows} == {"synthetic"}


def test_main_accepts_command_line_arguments(tmp_path):
    assert main(["--rows", "2", "--seed", "5", "--data-root", str(tmp_path)]) == 0
    assert len(list((tmp_path / "bronze").glob("housing_*.jsonl"))) == 1
