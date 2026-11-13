from pathlib import Path
from typing import Any

import yaml


def load_hearings(path: Path | str) -> list[dict[str, Any]]:
    """Read the hearing list from a YAML file and return its entries in file order.

    The file holds a top-level `hearings` list; each entry needs an int `term` and a
    string `docket`, and dockets must be unique. Raises ValueError naming the
    offending entry when any of that does not hold.
    """
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    hearings = data.get("hearings")
    if not isinstance(hearings, list) or not hearings:
        raise ValueError(f"{path}: expected a non-empty 'hearings' list")
    seen = set()
    for i, h in enumerate(hearings):
        if not isinstance(h, dict):
            raise ValueError(f"{path}: hearing #{i} is not a mapping")
        if not isinstance(h.get("term"), int) or isinstance(h.get("term"), bool):
            raise ValueError(f"{path}: hearing #{i} needs an integer 'term'")
        if not isinstance(h.get("docket"), str) or not h["docket"]:
            raise ValueError(f"{path}: hearing #{i} needs a string 'docket'")
        if h["docket"] in seen:
            raise ValueError(f"{path}: docket {h['docket']} is listed twice")
        seen.add(h["docket"])
    return hearings
