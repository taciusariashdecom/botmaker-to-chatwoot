from __future__ import annotations

from datetime import datetime, timezone


def iso_now() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
