"""Column names for the per-batch S3 object manifest (objects.csv).

Mirrors contracts/object_manifest.yaml; keep both in sync.
"""

MANIFEST_COLUMNS = (
    "batch_id",
    "s3_key",
    "source",
    "kind",
    "source_url",
    "retrieved_at",
    "bytes",
    "sha256",
    "licence_id",
    "upload_status",
)
