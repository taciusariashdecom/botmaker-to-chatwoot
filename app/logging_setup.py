import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Tuple


def _resolve_log_dir(preferred: str) -> Tuple[str, bool]:
    """Return a writeable log directory, falling back to /tmp when necessary."""

    try:
        Path(preferred).mkdir(parents=True, exist_ok=True)
        return preferred, False
    except OSError:
        fallback_root = Path(os.getenv("TMPDIR", "/tmp")) / "botmaker-logs"
        fallback_root.mkdir(parents=True, exist_ok=True)
        return str(fallback_root), True


def setup_logging(log_dir: str) -> str:
    resolved_dir, used_fallback = _resolve_log_dir(log_dir)
    log_path = os.path.join(resolved_dir, "app.log")

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Remove handlers we manage to avoid duplicated logs when setup is invoked twice.
    root.handlers = [
        handler
        for handler in root.handlers
        if not isinstance(handler, (logging.StreamHandler, RotatingFileHandler))
    ]

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    root.addHandler(ch)

    # File handler (rotating)
    fh = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setFormatter(formatter)
    root.addHandler(fh)

    if used_fallback:
        root.warning("Falling back to writable log directory: %s", resolved_dir)

    return resolved_dir
