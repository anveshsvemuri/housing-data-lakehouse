"""Dependency-free checks that protect the Bronze-to-Silver boundary."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

REQUIRED_FIELDS = frozenset(
    {
        "property_id",
        "sale_date",
        "city",
        "state",
        "property_type",
        "bedrooms",
        "bathrooms",
        "square_feet",
        "year_built",
        "sale_price",
        "latitude",
        "longitude",
        "source",
        "ingested_at",
    }
)


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Summary returned by a validation run."""

    row_count: int
    errors: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        return not self.errors

    def raise_for_errors(self) -> None:
        if self.errors:
            raise ValueError("Housing data failed validation:\n- " + "\n- ".join(self.errors))


def validate_housing_records(records: Iterable[Mapping[str, Any]]) -> ValidationResult:
    """Validate required fields, identifiers, and core housing business rules."""
    rows = list(records)
    errors: list[str] = []
    seen_ids: set[str] = set()

    for index, row in enumerate(rows):
        missing = sorted(REQUIRED_FIELDS.difference(row))
        if missing:
            errors.append(f"row {index}: missing fields {', '.join(missing)}")
            continue

        property_id = str(row["property_id"]).strip()
        if not property_id:
            errors.append(f"row {index}: property_id is blank")
        elif property_id in seen_ids:
            errors.append(f"row {index}: duplicate property_id {property_id}")
        seen_ids.add(property_id)

        if row["sale_price"] <= 0:
            errors.append(f"row {index}: sale_price must be positive")
        if row["square_feet"] <= 0:
            errors.append(f"row {index}: square_feet must be positive")
        if row["bedrooms"] < 0 or row["bathrooms"] <= 0:
            errors.append(f"row {index}: bedroom/bathroom counts are invalid")
        if len(str(row["state"])) != 2:
            errors.append(f"row {index}: state must be a two-letter code")
        if not (-90 <= row["latitude"] <= 90 and -180 <= row["longitude"] <= 180):
            errors.append(f"row {index}: coordinates are out of range")

    return ValidationResult(row_count=len(rows), errors=tuple(errors))
