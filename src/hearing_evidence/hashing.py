import hashlib
from pathlib import Path


def sha256_of(path: Path | str) -> str:
    """Return the sha256 hex digest of the file at `path`, read in 1 MB chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
