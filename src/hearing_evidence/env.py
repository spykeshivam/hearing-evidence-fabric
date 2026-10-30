import os
from pathlib import Path


def load_dotenv_file(path: Path | str = ".env") -> None:
    """Read `KEY=VALUE` lines from `path` into `os.environ`.

    Skips blank lines and lines starting with `#`. Never overwrites a key
    already set in `os.environ`, so a value exported in the shell always
    takes precedence over the file. Does nothing if `path` doesn't exist.
    """
    path = Path(path)
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()
