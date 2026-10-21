from pathlib import Path

import pytest

from hearing_evidence.contracts import load_contract, unwrap_docket, validate_file

ROOT = Path(__file__).resolve().parents[1]
STAGE = ROOT / "data" / "stage"
CONTRACTS = ROOT / "contracts"

pytestmark = pytest.mark.skipif(
    not STAGE.exists(),
    reason="data/stage not present (gitignored); run scripts/fetch_and_stage.py locally first",
)

if STAGE.exists():
    courtlistener_contract = load_contract(CONTRACTS / "courtlistener_audio.yaml")
    oyez_contract = load_contract(CONTRACTS / "oyez_transcripts.yaml")
    DOCKET_FILES = sorted((STAGE / "courtlistener" / "dockets").glob("*.json"))
    AUDIO_META_FILES = sorted((STAGE / "courtlistener" / "audio_meta").glob("*.json"))
    OYEZ_CASE_FILES = sorted((STAGE / "oyez" / "cases").glob("*.json"))
    OYEZ_TRANSCRIPT_FILES = sorted((STAGE / "oyez" / "transcripts").glob("*.json"))
else:
    DOCKET_FILES = AUDIO_META_FILES = OYEZ_CASE_FILES = OYEZ_TRANSCRIPT_FILES = []


@pytest.mark.parametrize("path", DOCKET_FILES, ids=lambda p: p.stem)
def test_docket_matches_contract(path):
    validate_file(path, courtlistener_contract["schema"]["docket"], unwrap=unwrap_docket)


@pytest.mark.parametrize("path", AUDIO_META_FILES, ids=lambda p: p.stem)
def test_audio_meta_matches_contract(path):
    validate_file(path, courtlistener_contract["schema"]["audio_meta"])


@pytest.mark.parametrize("path", OYEZ_CASE_FILES, ids=lambda p: p.stem)
def test_oyez_case_matches_contract(path):
    validate_file(path, oyez_contract["schema"]["case"])


@pytest.mark.parametrize("path", OYEZ_TRANSCRIPT_FILES, ids=lambda p: p.stem)
def test_oyez_transcript_matches_contract(path):
    validate_file(path, oyez_contract["schema"]["transcript"])
