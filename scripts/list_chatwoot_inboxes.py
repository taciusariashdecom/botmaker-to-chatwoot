#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

# make repo root importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.chatwoot import ChatwootClient  # noqa: E402
from app.config import get_settings  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="List Chatwoot inboxes for an account")
    parser.add_argument("--account-id", required=True, help="Chatwoot account ID")
    args = parser.parse_args()

    settings = get_settings()
    if not settings.chatwoot_api_access_token:
        print("ERROR: CHATWOOT_API_ACCESS_TOKEN is not set in environment (.env)")
        sys.exit(1)

    client = ChatwootClient(
        base_url=settings.chatwoot_base_url,
        api_access_token=settings.chatwoot_api_access_token,
        rps=settings.rate_limit_rps,
    )
    try:
        data: Dict[str, Any] = client.list_inboxes(args.account_id)
        items: List[Dict[str, Any]] = data if isinstance(data, list) else data.get("payload") or data.get("data") or data.get("items") or []
        if not isinstance(items, list):
            print("Unexpected response structure:")
            print(data)
            sys.exit(2)
        print("id,name,channel_type,website_url")
        for inbox in items:
            print(
                f"{inbox.get('id')},{inbox.get('name')},{inbox.get('channel_type')},{inbox.get('website_url') or ''}"
            )
    finally:
        client.close()


if __name__ == "__main__":
    main()
