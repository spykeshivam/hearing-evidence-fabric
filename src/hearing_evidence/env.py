from pathlib import Path

from dotenv import load_dotenv


def load_dotenv_file(path: Path | str = ".env") -> None:
    """Load `KEY=VALUE` pairs from `path` into `os.environ` via python-dotenv.

    Never overwrites a key already set in `os.environ`, so a value exported
    in the shell always takes precedence over the file. Does nothing if
    `path` doesn't exist.
    """
    load_dotenv(dotenv_path=path, override=False)
