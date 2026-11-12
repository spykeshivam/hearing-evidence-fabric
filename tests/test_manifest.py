from pathlib import Path

from hearing_evidence.contracts import load_contract
from hearing_evidence.manifest import MANIFEST_COLUMNS

CONTRACT = load_contract(Path(__file__).resolve().parents[1] / "contracts" / "object_manifest.yaml")


def test_manifest_columns_match_contract():
    contract_columns = tuple(col["name"] for col in CONTRACT["columns"])
    assert contract_columns == MANIFEST_COLUMNS


def test_every_contract_column_has_a_description():
    for col in CONTRACT["columns"]:
        assert col.get("description", "").strip(), f"{col['name']} has no description"
