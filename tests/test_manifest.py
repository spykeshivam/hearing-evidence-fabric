from pathlib import Path

from hearing_evidence.contracts import load_contract
from hearing_evidence.manifest import manifest_columns

CONTRACT = load_contract(Path(__file__).resolve().parents[1] / "contracts" / "object_manifest.yaml")


def test_manifest_columns_reads_names_in_contract_order():
    contract = {"columns": [{"name": "a"}, {"name": "b"}, {"name": "c"}]}
    assert manifest_columns(contract) == ("a", "b", "c")


def test_object_manifest_contract_has_ten_columns():
    assert len(CONTRACT["columns"]) == 10


def test_every_contract_column_has_a_description():
    for col in CONTRACT["columns"]:
        assert col.get("description", "").strip(), f"{col['name']} has no description"
