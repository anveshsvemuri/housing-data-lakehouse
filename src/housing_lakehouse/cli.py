"""Command-line entry point for reproducible local pipeline runs."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from housing_lakehouse.ingestion import generate_housing_records, write_jsonl
from housing_lakehouse.quality import validate_housing_records
from housing_lakehouse.settings import PipelineSettings

LOGGER = logging.getLogger("housing_lakehouse")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local housing lakehouse pipeline")
    parser.add_argument("--rows", type=int, default=100, help="number of synthetic rows")
    parser.add_argument("--seed", type=int, default=42, help="deterministic random seed")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    return parser


def run_pipeline(*, rows: int, seed: int, data_root: Path) -> Path:
    settings = PipelineSettings(data_root=data_root)
    settings.create_data_directories()
    generated_at = datetime.now(UTC)
    records = generate_housing_records(rows, seed=seed, generated_at=generated_at)
    validation = validate_housing_records(records)
    validation.raise_for_errors()

    snapshot = generated_at.strftime("housing_%Y%m%dT%H%M%SZ.jsonl")
    destination = settings.bronze_path / snapshot
    written = write_jsonl(records, destination)
    LOGGER.info("wrote %s validated rows to %s", written, destination)
    return destination


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    run_pipeline(rows=args.rows, seed=args.seed, data_root=args.data_root)
    return 0
