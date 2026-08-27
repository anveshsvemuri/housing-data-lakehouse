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


def fingerprint_file(path: Path) -> str:
    """Return a SHA-256 content fingerprint for one input file."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_input_inventory(bronze_path: Path) -> dict[str, str]:
    """Map stable Bronze-relative paths to content fingerprints."""
    files = discover_input_files(bronze_path)
    root = bronze_path if bronze_path.is_dir() else bronze_path.parent
    return {path.relative_to(root).as_posix(): fingerprint_file(path) for path in files}


def read_processing_state(state_file: Path) -> dict[str, Any]:
    """Read a processing checkpoint, returning an empty state when absent."""
    if not state_file.exists():
        return {"version": 1, "processed_files": {}, "row_counts": {}}
    with state_file.open(encoding="utf-8") as source:
        state = json.load(source)
    if state.get("version") != 1 or not isinstance(state.get("processed_files"), dict):
        raise ValueError(f"unsupported or invalid processing state: {state_file}")
    return state


def write_processing_state(state_file: Path, payload: Mapping[str, Any]) -> Path:
    """Atomically publish the checkpoint only after a successful pipeline run."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_file.with_suffix(f"{state_file.suffix}.tmp")
    with temporary.open("w", encoding="utf-8") as target:
        json.dump(payload, target, indent=2, sort_keys=True)
        target.write("\n")
    os.replace(temporary, state_file)
    return state_file


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
