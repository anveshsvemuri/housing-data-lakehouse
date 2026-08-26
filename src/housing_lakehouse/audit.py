"""Deterministic run identity and atomic audit-manifest persistence."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


def discover_input_files(bronze_path: Path) -> tuple[Path, ...]:
    """Return JSONL inputs in stable order for a file or Bronze directory."""
    if bronze_path.is_file():
        return (bronze_path,)
    return tuple(sorted(path for path in bronze_path.rglob("*.jsonl") if path.is_file()))


def fingerprint_files(paths: Iterable[Path]) -> str:
    """Create a content-based identifier that is stable across reruns."""
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode())
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def write_audit_manifest(
    audit_path: Path,
    *,
    run_id: str,
    payload: Mapping[str, Any],
) -> Path:
    """Atomically publish one manifest per deterministic run ID."""
    audit_path.mkdir(parents=True, exist_ok=True)
    destination = audit_path / f"{run_id}.json"
    temporary = destination.with_suffix(".json.tmp")
    with temporary.open("w", encoding="utf-8") as target:
        json.dump(payload, target, indent=2, sort_keys=True)
        target.write("\n")
    os.replace(temporary, destination)
    return destination
