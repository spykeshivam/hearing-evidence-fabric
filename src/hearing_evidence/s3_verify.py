import json
import subprocess


def verify_objects(bucket, profile, region, rows):
    """Check S3 for each row's s3_key and compare its size to the manifest's bytes.

    Calls `aws s3api head-object` per row, so it reflects what's actually in the
    bucket right now rather than trusting a sync command's own exit code. Returns
    a dict mapping s3_key to "uploaded", "size_mismatch", or "missing".
    """
    statuses = {}
    for row in rows:
        key = row["s3_key"]
        result = subprocess.run(
            [
                "aws",
                "s3api",
                "head-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--profile",
                profile,
                "--region",
                region,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            statuses[key] = "missing"
            continue
        content_length = json.loads(result.stdout)["ContentLength"]
        statuses[key] = "uploaded" if content_length == row["bytes"] else "size_mismatch"
    return statuses
