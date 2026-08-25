from datetime import UTC, datetime

import pytest

from housing_lakehouse.ingestion.synthetic import generate_housing_records
from housing_lakehouse.quality import validate_housing_records


def valid_records(count=2):
    return generate_housing_records(
        count,
        seed=9,
        generated_at=datetime(2026, 1, 15, tzinfo=UTC),
    )


def test_generated_records_pass_quality_checks():
    result = validate_housing_records(valid_records())

    assert result.is_valid
    assert result.row_count == 2
    assert result.errors == ()


def test_validation_reports_duplicate_and_business_rule_errors():
    records = valid_records()
    records[1]["property_id"] = records[0]["property_id"]
    records[1]["sale_price"] = 0

    result = validate_housing_records(records)

    assert not result.is_valid
    assert any("duplicate property_id" in error for error in result.errors)
    assert any("sale_price must be positive" in error for error in result.errors)


def test_raise_for_errors_provides_actionable_message():
    record = valid_records(1)[0]
    del record["state"]

    with pytest.raises(ValueError, match="missing fields state"):
        validate_housing_records([record]).raise_for_errors()
