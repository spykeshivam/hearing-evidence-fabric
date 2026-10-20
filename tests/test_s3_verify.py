import json
from types import SimpleNamespace

from hearing_evidence import s3_verify


def test_verify_objects_statuses(monkeypatch):
    rows = [
        {"s3_key": "a.json", "bytes": 10},
        {"s3_key": "b.json", "bytes": 10},
        {"s3_key": "c.json", "bytes": 10},
    ]

    def fake_run(cmd, **kwargs):
        key = cmd[cmd.index("--key") + 1]
        if key == "a.json":
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ContentLength": 10}))
        if key == "b.json":
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ContentLength": 5}))
        return SimpleNamespace(returncode=255, stdout="")

    monkeypatch.setattr(s3_verify.subprocess, "run", fake_run)

    statuses = s3_verify.verify_objects("bucket", "he-uploader", "eu-north-1", rows)

    assert statuses == {
        "a.json": "uploaded",
        "b.json": "size_mismatch",
        "c.json": "missing",
    }
