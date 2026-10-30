import json

import jsonschema
import pytest

from hearing_evidence.contracts import (
    load_contract,
    stage_contracts,
    unwrap_docket,
    validate_file,
    validate_record,
)

SCHEMA = {
    "type": "object",
    "required": ["id", "name"],
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
}


def test_unwrap_docket_returns_first_result():
    data = {"count": 1, "results": [{"id": 1}]}
    assert unwrap_docket(data) == {"id": 1}


def test_validate_record_passes_for_matching_record():
    validate_record({"id": 1, "name": "a"}, SCHEMA)


def test_validate_record_raises_for_missing_required_field():
    with pytest.raises(jsonschema.ValidationError):
        validate_record({"id": 1}, SCHEMA)


def test_validate_file_unwraps_before_validating(tmp_path):
    path = tmp_path / "record.json"
    path.write_text(json.dumps({"results": [{"id": 1, "name": "a"}]}), encoding="utf-8")
    validate_file(path, SCHEMA, unwrap=unwrap_docket)


def test_validate_file_raises_for_invalid_record(tmp_path):
    path = tmp_path / "record.json"
    path.write_text(json.dumps({"id": "not-an-int", "name": "a"}), encoding="utf-8")
    with pytest.raises(jsonschema.ValidationError):
        validate_file(path, SCHEMA)


def test_load_contract_reads_schema_block(tmp_path):
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text("schema:\n  docket:\n    type: object\n", encoding="utf-8")
    contract = load_contract(contract_path)
    assert contract["schema"]["docket"]["type"] == "object"


def test_stage_contracts_copies_every_yaml(tmp_path):
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    (contracts_dir / "a.yaml").write_text("source_id: a\n", encoding="utf-8")
    (contracts_dir / "b.yaml").write_text("source_id: b\n", encoding="utf-8")
    (contracts_dir / "notes.txt").write_text("ignore me\n", encoding="utf-8")

    stage_dir = tmp_path / "stage" / "_contracts"
    copied = stage_contracts(contracts_dir, stage_dir)

    assert {p.name for p in copied} == {"a.yaml", "b.yaml"}
    assert (stage_dir / "a.yaml").read_text(encoding="utf-8") == "source_id: a\n"


def test_stage_contracts_overwrites_existing_copy(tmp_path):
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    (contracts_dir / "a.yaml").write_text("source_id: a\n", encoding="utf-8")

    stage_dir = tmp_path / "stage" / "_contracts"
    stage_dir.mkdir(parents=True)
    (stage_dir / "a.yaml").write_text("stale\n", encoding="utf-8")

    stage_contracts(contracts_dir, stage_dir)

    assert (stage_dir / "a.yaml").read_text(encoding="utf-8") == "source_id: a\n"
