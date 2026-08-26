import json

from housing_lakehouse.audit import discover_input_files, fingerprint_files, write_audit_manifest


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
