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
        assert cmd[2] == "list-objects-v2"
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "Contents": [
                        {"Key": "a.json", "Size": 10},
                        {"Key": "b.json", "Size": 5},
                    ]
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(s3_verify.subprocess, "run", fake_run)

    statuses = s3_verify.verify_objects("bucket", "he-uploader", "eu-north-1", rows)

    assert statuses == {
        "a.json": "uploaded",
        "b.json": "size_mismatch",
        "c.json": "missing",
    }


def test_verify_objects_list_call_fails(monkeypatch):
    rows = [{"s3_key": "a.json", "bytes": 10}]

    def fake_run(cmd, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="AccessDenied")

    monkeypatch.setattr(s3_verify.subprocess, "run", fake_run)

    statuses = s3_verify.verify_objects("bucket", "he-uploader", "eu-north-1", rows)

    assert statuses == {"a.json": "missing"}
