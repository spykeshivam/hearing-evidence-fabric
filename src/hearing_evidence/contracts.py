import json
from pathlib import Path
from typing import Any

import jsonschema
import yaml


def load_contract(path: Path | str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def unwrap_docket(data: dict[str, Any]) -> dict[str, Any]:
    """CourtListener's docket search endpoint wraps the single match in a pagination envelope."""
    return data["results"][0]


def validate_record(record: dict[str, Any], schema: dict[str, Any]) -> None:
    jsonschema.validate(instance=record, schema=schema)


def validate_file(path: Path | str, schema: dict[str, Any], unwrap=None) -> None:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    record = unwrap(data) if unwrap else data
    validate_record(record, schema)
