import os

from hearing_evidence.env import load_dotenv_file


def test_load_dotenv_file_sets_missing_vars(tmp_path, monkeypatch):
    monkeypatch.delenv("CL_TOKEN", raising=False)
    env_path = tmp_path / ".env"
    env_path.write_text("CL_TOKEN=abc123\n", encoding="utf-8")

    load_dotenv_file(env_path)

    assert os.environ["CL_TOKEN"] == "abc123"


def test_load_dotenv_file_skips_blank_and_comment_lines(tmp_path, monkeypatch):
    monkeypatch.delenv("HE_BUCKET", raising=False)
    env_path = tmp_path / ".env"
    env_path.write_text("# a comment\n\nHE_BUCKET=my-bucket\n", encoding="utf-8")

    load_dotenv_file(env_path)

    assert os.environ["HE_BUCKET"] == "my-bucket"


def test_load_dotenv_file_does_not_override_existing_env(tmp_path, monkeypatch):
    monkeypatch.setenv("CL_TOKEN", "already-set")
    env_path = tmp_path / ".env"
    env_path.write_text("CL_TOKEN=from-file\n", encoding="utf-8")

    load_dotenv_file(env_path)

    assert os.environ["CL_TOKEN"] == "already-set"


def test_load_dotenv_file_missing_file_is_a_noop(tmp_path):
    load_dotenv_file(tmp_path / "does-not-exist.env")
