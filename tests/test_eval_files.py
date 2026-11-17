"""Checks that the data-agent eval files in eval/ are well formed.

questions.yaml holds the questions and their ground truth; results.csv logs each run
of the agent against them. These tests catch typos and broken references before
they reach a run, so every result row points at a real question.
"""

import csv
from pathlib import Path

import pytest
import yaml

EVAL_DIR = Path(__file__).resolve().parents[1] / "eval"
QUESTIONS = EVAL_DIR / "questions.yaml"
RESULTS = EVAL_DIR / "results.csv"

REQUIRED_FIELDS = {
    "id",
    "version",
    "text",
    "expected_behaviour",
    "ground_truth_dax",
    "ground_truth",
    "notes",
}
BEHAVIOURS = {"answer", "refuse"}
VERDICTS = {"correct", "partial", "wrong", "refused_ok"}
RESULTS_HEADER = [
    "run_id",
    "run_date",
    "agent_version",
    "question_id",
    "question_version",
    "agent_answer",
    "agent_dax",
    "verdict",
    "rationale",
    "data_fingerprint",
]


@pytest.fixture(scope="module")
def questions():
    """Return the list of question mappings parsed from questions.yaml."""
    # safe_load builds plain Python dicts and lists and refuses arbitrary objects.
    with open(QUESTIONS, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data["questions"]


def read_results():
    """Return results.csv as a header list and a list of row dicts."""
    # newline="" lets the csv module handle line endings itself, including
    # newlines inside quoted fields.
    with open(RESULTS, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)


def test_questions_parse_to_non_empty_list(questions):
    """questions.yaml parses and holds at least one question."""
    assert isinstance(questions, list)
    assert questions


def test_question_ids_are_unique(questions):
    """No two questions share an id."""
    ids = [q["id"] for q in questions]
    assert len(ids) == len(set(ids))


def test_questions_have_required_fields(questions):
    """Every question has every required field (ground_truth may be null)."""
    for q in questions:
        # Set difference: the required fields absent from this question's keys.
        missing = REQUIRED_FIELDS - q.keys()
        assert not missing, f"{q.get('id')}: missing {sorted(missing)}"


def test_expected_behaviour_is_allowed(questions):
    """expected_behaviour is either answer or refuse."""
    for q in questions:
        assert q["expected_behaviour"] in BEHAVIOURS, q["id"]


def test_ground_truth_dax_is_non_empty(questions):
    """ground_truth_dax holds a non-empty string."""
    for q in questions:
        dax = q["ground_truth_dax"]
        assert isinstance(dax, str) and dax.strip(), q["id"]


def test_results_header_matches():
    """results.csv starts with the exact column list, in order."""
    header, _ = read_results()
    assert header == RESULTS_HEADER


def test_result_rows_reference_questions_and_valid_verdicts(questions):
    """Every result row has an allowed verdict and points at an existing question version."""
    # CSV values are always strings, so the question versions are compared as strings.
    known = {(q["id"], str(q["version"])) for q in questions}
    _, rows = read_results()
    # Zero rows passes: the loop body simply never runs.
    for i, row in enumerate(rows, start=2):
        assert row["verdict"] in VERDICTS, f"line {i}: verdict {row['verdict']!r}"
        key = (row["question_id"], row["question_version"])
        assert key in known, f"line {i}: unknown question {key}"
