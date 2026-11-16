"""Derive the S3 object manifest's (objects.csv) column order from its data contract."""

from typing import Any


def manifest_columns(contract: dict[str, Any]) -> tuple[str, ...]:
    """Return column names, in order, from a loaded object_manifest.yaml contract."""
    return tuple(col["name"] for col in contract["columns"])
