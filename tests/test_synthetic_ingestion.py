import json
from datetime import UTC, datetime

import pytest

from housing_lakehouse.ingestion.synthetic import (
    generate_housing_records,
    write_jsonl,
)


FIXED_TIME = datetime(2026, 1, 15, 12, tzinfo=UTC)


def test_generation_is_reproducible():
    first = generate_housing_records(3, seed=7, generated_at=FIXED_TIME)
    second = generate_housing_records(3, seed=7, generated_at=FIXED_TIME)

    assert first == second
    assert len({row["property_id"] for row in first}) == 3
    assert all(row["sale_price"] > 0 for row in first)


def test_generation_rejects_negative_count():
    with pytest.raises(ValueError, match="non-negative"):
        generate_housing_records(-1)


def test_write_jsonl_creates_an_atomic_snapshot(tmp_path):
    records = generate_housing_records(2, generated_at=FIXED_TIME)
    destination = tmp_path / "bronze" / "housing.jsonl"

    assert write_jsonl(records, destination) == 2
    assert [json.loads(line) for line in destination.read_text().splitlines()] == records
    assert not destination.with_suffix(".jsonl.tmp").exists()
