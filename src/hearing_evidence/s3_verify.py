import json
import subprocess


def verify_objects(bucket, profile, region, rows):
    """Check S3 for each row's s3_key and compare its size to the manifest's bytes.

    Lists the bucket once via `aws s3api list-objects-v2`, which only needs
    s3:ListBucket, rather than calling head-object per key, which needs
    s3:GetObject and isn't granted to the uploader profile. Returns a dict
    mapping s3_key to "uploaded", "size_mismatch", or "missing".
    """
    result = subprocess.run(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--profile",
            profile,
            "--region",
            region,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    sizes = {}
    if result.returncode == 0:
        for obj in json.loads(result.stdout).get("Contents") or []:
            sizes[obj["Key"]] = obj["Size"]
    else:
        print(f"  warning: could not list bucket for verification: {result.stderr.strip()}")

    statuses = {}
    for row in rows:
        key = row["s3_key"]
        if key not in sizes:
            statuses[key] = "missing"
        elif sizes[key] != row["bytes"]:
            statuses[key] = "size_mismatch"
        else:
            statuses[key] = "uploaded"
    return statuses
