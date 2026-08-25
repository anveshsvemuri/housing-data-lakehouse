"""Data-quality checks for housing records."""

from .validation import ValidationResult, validate_housing_records

__all__ = ["ValidationResult", "validate_housing_records"]
