from pathlib import Path

import pytest

from hearing_evidence.hearings import load_hearings

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config" / "hearings.yaml"


def write(tmp_path, text):
    """Write `text` to a hearings.yaml under tmp_path and return its path."""
    path = tmp_path / "hearings.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_load_hearings_returns_entries_in_order(tmp_path):
    path = write(
        tmp_path,
        'hearings:\n  - {term: 2024, docket: "23-970"}\n  - {term: 2022, docket: "21-1333"}\n',
    )
    assert [h["docket"] for h in load_hearings(path)] == ["23-970", "21-1333"]


def test_load_hearings_keeps_docket_as_string(tmp_path):
    path = write(tmp_path, 'hearings:\n  - {term: 2017, docket: "17-2"}\n')
    assert load_hearings(path)[0]["docket"] == "17-2"


def test_load_hearings_rejects_missing_term(tmp_path):
    path = write(tmp_path, 'hearings:\n  - {docket: "23-970"}\n')
    with pytest.raises(ValueError, match="term"):
        load_hearings(path)


def test_load_hearings_rejects_non_string_docket(tmp_path):
    path = write(tmp_path, "hearings:\n  - {term: 2024, docket: 23970}\n")
    with pytest.raises(ValueError, match="docket"):
        load_hearings(path)


def test_load_hearings_rejects_duplicate_docket(tmp_path):
    path = write(
        tmp_path,
        'hearings:\n  - {term: 2024, docket: "23-970"}\n  - {term: 2024, docket: "23-970"}\n',
    )
    with pytest.raises(ValueError, match="listed twice"):
        load_hearings(path)


def test_load_hearings_rejects_empty_list(tmp_path):
    path = write(tmp_path, "hearings: []\n")
    with pytest.raises(ValueError, match="non-empty"):
        load_hearings(path)


def test_repo_hearing_list_loads():
    assert load_hearings(REPO_CONFIG)
