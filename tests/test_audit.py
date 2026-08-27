import json

from housing_lakehouse.audit import (
    build_input_inventory,
    discover_input_files,
    fingerprint_file,
    fingerprint_files,
    read_processing_state,
    write_audit_manifest,
    write_processing_state,
)


def test_fingerprint_is_content_based_and_ordered(tmp_path):
    first = tmp_path / "a.jsonl"
    second = tmp_path / "b.jsonl"
    first.write_text('{"id": 1}\n')
    second.write_text('{"id": 2}\n')

    inputs = discover_input_files(tmp_path)
    original = fingerprint_files(inputs)
    assert inputs == (first, second)
    assert original == fingerprint_files(inputs)

    second.write_text('{"id": 3}\n')
    assert fingerprint_files(discover_input_files(tmp_path)) != original


def test_audit_manifest_replaces_same_run_atomically(tmp_path):
    destination = write_audit_manifest(tmp_path, run_id="abc123", payload={"rows": 10})
    write_audit_manifest(tmp_path, run_id="abc123", payload={"rows": 11})

    assert json.loads(destination.read_text()) == {"rows": 11}
    assert not list(tmp_path.glob("*.tmp"))


def test_input_inventory_uses_relative_paths_and_content_hashes(tmp_path):
    bronze = tmp_path / "bronze"
    nested = bronze / "year=2026"
    nested.mkdir(parents=True)
    first = bronze / "a.jsonl"
    second = nested / "b.jsonl"
    first.write_text('{"id": 1}\n')
    second.write_text('{"id": 2}\n')

    inventory = build_input_inventory(bronze)

    assert inventory == {
        "a.jsonl": fingerprint_file(first),
        "year=2026/b.jsonl": fingerprint_file(second),
    }


def test_processing_state_round_trip_is_atomic(tmp_path):
    state_file = tmp_path / "checkpoints" / "processing-state.json"
    assert read_processing_state(state_file) == {
        "version": 1,
        "processed_files": {},
        "row_counts": {},
    }

    payload = {
        "version": 1,
        "processed_files": {"housing.jsonl": "abc123"},
        "row_counts": {"bronze": 10, "silver": 9, "rejected": 1, "gold": 3},
    }
    write_processing_state(state_file, payload)

    assert read_processing_state(state_file) == payload
    assert not list(state_file.parent.glob("*.tmp"))


def test_processing_state_rejects_unknown_versions(tmp_path):
    state_file = tmp_path / "processing-state.json"
    state_file.write_text('{"version": 2, "processed_files": {}}')

    try:
        read_processing_state(state_file)
    except ValueError as error:
        assert "unsupported or invalid" in str(error)
    else:
        raise AssertionError("invalid checkpoint version was accepted")
