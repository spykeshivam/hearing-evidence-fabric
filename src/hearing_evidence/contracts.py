import json
import shutil
from pathlib import Path
from typing import Any

import jsonschema
import yaml


def load_contract(path: Path | str) -> dict[str, Any]:
    """Read a YAML data contract file into a dict."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def unwrap_docket(data: dict[str, Any]) -> dict[str, Any]:
    """CourtListener's docket search endpoint wraps the single match in a pagination envelope."""
    return data["results"][0]


def validate_record(record: dict[str, Any], schema: dict[str, Any]) -> None:
    """Validate `record` against a JSON Schema `schema`, raising if it doesn't match."""
    jsonschema.validate(instance=record, schema=schema)


def validate_file(path: Path | str, schema: dict[str, Any], unwrap=None) -> None:
    """Load the JSON file at `path` and validate it against `schema`.

    `unwrap` is applied to the parsed JSON first, for sources (like
    unwrap_docket) whose top-level shape needs to be peeled back before it
    matches the schema.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    record = unwrap(data) if unwrap else data
    validate_record(record, schema)


def stage_contracts(contracts_dir: Path | str, stage_dir: Path | str) -> list[Path]:
    """Copy every `*.yaml` in `contracts_dir` to `stage_dir`, overwriting existing files.

    Lets the contracts ride along the same stage -> S3 -> OneLake path as staged
    evidence, so Fabric can read them without a second, hand-kept copy. Check
    docs/decisions.md, 2026-10-29 for more details.
    """
    stage_dir = Path(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for src in sorted(Path(contracts_dir).glob("*.yaml")):
        dest = stage_dir / src.name
        shutil.copy2(src, dest)
        copied.append(dest)
    return copied
