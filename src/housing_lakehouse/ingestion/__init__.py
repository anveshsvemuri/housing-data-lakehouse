"""Data ingestion utilities."""

from .synthetic import generate_housing_records, write_jsonl

__all__ = ["generate_housing_records", "write_jsonl"]
