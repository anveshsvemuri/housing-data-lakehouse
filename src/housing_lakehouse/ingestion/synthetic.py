"""Deterministic synthetic housing data for local and CI pipeline runs."""

from __future__ import annotations

import json
import random
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

_MARKETS = (
    ("Austin", "TX", 30.2672, -97.7431),
    ("Charlotte", "NC", 35.2271, -80.8431),
    ("Denver", "CO", 39.7392, -104.9903),
    ("Jersey City", "NJ", 40.7178, -74.0431),
    ("Seattle", "WA", 47.6062, -122.3321),
)
_PROPERTY_TYPES = ("condo", "single_family", "townhouse")


def generate_housing_records(
    count: int,
    *,
    seed: int = 42,
    generated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return realistic, deterministic records without external credentials."""
    if count < 0:
        raise ValueError("count must be non-negative")

    rng = random.Random(seed)
    ingestion_time = (generated_at or datetime.now(UTC)).astimezone(UTC)
    base_sale_date = ingestion_time.date() - timedelta(days=365)
    records: list[dict[str, Any]] = []

    for index in range(count):
        city, state, latitude, longitude = rng.choice(_MARKETS)
        bedrooms = rng.randint(1, 5)
        bathrooms = rng.choice((1.0, 1.5, 2.0, 2.5, 3.0, 3.5))
        square_feet = rng.randint(650, 3_800)
        year_built = rng.randint(1940, ingestion_time.year)
        price_per_sqft = rng.randint(180, 650)
        records.append(
            {
                "property_id": f"H-{seed:04d}-{index:06d}",
                "sale_date": (base_sale_date + timedelta(days=rng.randint(0, 364))).isoformat(),
                "city": city,
                "state": state,
                "property_type": rng.choice(_PROPERTY_TYPES),
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "square_feet": square_feet,
                "year_built": year_built,
                "sale_price": square_feet * price_per_sqft,
                "latitude": round(latitude + rng.uniform(-0.18, 0.18), 6),
                "longitude": round(longitude + rng.uniform(-0.18, 0.18), 6),
                "source": "synthetic",
                "ingested_at": ingestion_time.isoformat(),
            }
        )
    return records


def write_jsonl(records: Iterable[dict[str, Any]], destination: Path) -> int:
    """Write records atomically as newline-delimited JSON and return row count."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_suffix(f"{destination.suffix}.tmp")
    count = 0
    with temporary_path.open("w", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record, sort_keys=True) + "\n")
            count += 1
    temporary_path.replace(destination)
    return count
