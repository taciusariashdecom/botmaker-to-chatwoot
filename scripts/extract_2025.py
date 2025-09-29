#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, List, Tuple

# Ensure we can import the local 'app' package when running from repo root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.config import get_settings  # noqa: E402


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime
    prefix: str

    def iso_bounds(self) -> Tuple[str, str]:
        return (self.start.isoformat().replace("+00:00", "Z"), self.end.isoformat().replace("+00:00", "Z"))


def month_windows(year: int, root_prefix: str) -> List[Window]:
    out: List[Window] = []
    for m in range(1, 13):
        start = datetime(year, m, 1, tzinfo=timezone.utc)
        if m == 12:
            end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end = datetime(year, m + 1, 1, tzinfo=timezone.utc)
        prefix = f"{root_prefix}/{year}-{m:02d}"
        out.append(Window(start, end, prefix))
    return out


def biweekly_from_month(win: Window) -> List[Window]:
    mid = win.start + (win.end - win.start) / 2
    w1 = Window(win.start, mid, f"{win.prefix}-a")
    w2 = Window(mid, win.end, f"{win.prefix}-b")
    return [w1, w2]


def plan_text(windows: Iterable[Window]) -> str:
    lines = ["Extraction plan:"]
    for w in windows:
        f, t = w.iso_bounds()
        lines.append(f"- {f} -> {t}  |  prefix: {w.prefix}")
    return "\n".join(lines)


def run_extract(window: Window) -> int:
    f, t = window.iso_bounds()
    cmd = [
        sys.executable,
        "-m",
        "app.extract",
        "--from",
        f,
        "--to",
        t,
        "--long-term",
        "--output-prefix",
        window.prefix,
    ]
    print(f"[RUN] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(ROOT))
    return proc.returncode


def execute_with_fallback(windows: Iterable[Window]) -> int:
    """Execute monthly windows; on failure, split month into biweekly windows and try again.
    Returns the number of failures (0 if all good)."""
    failures = 0
    for w in windows:
        rc = run_extract(w)
        if rc == 0:
            continue
        print(f"[WARN] Monthly extraction failed for {w.prefix}. Trying biweekly fallback…")
        subfails = 0
        for bw in biweekly_from_month(w):
            subrc = run_extract(bw)
            if subrc != 0:
                print(f"[ERROR] Biweekly extraction failed for {bw.prefix} (rc={subrc})")
                subfails += 1
        if subfails:
            failures += 1
    return failures


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Orchestrate Botmaker extractions for a full year with fallbacks")
    p.add_argument("--year", type=int, default=2025, help="Target year (default: 2025)")
    p.add_argument(
        "--root-prefix",
        default="botmaker/2025",
        help="Prefix under data directory to store results (default: botmaker/2025)",
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Execute extraction (default: only print plan)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()  # ensure .env is loaded

    wins = month_windows(args.year, args.root_prefix)

    print(plan_text(wins))
    print("\nNOTE: Each window runs with --long-term and no per-chat limits.")
    print("      Data will be written under data/<prefix>/… as NDJSON + summary.json")

    if not args.execute:
        print("\nPlan only (no execution). Use --execute to run.")
        return

    failures = execute_with_fallback(wins)
    if failures == 0:
        print("\nAll windows completed successfully.")
    else:
        print(f"\nCompleted with {failures} monthly windows failing even after biweekly fallback.")


if __name__ == "__main__":
    main()
