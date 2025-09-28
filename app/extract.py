from __future__ import annotations

import argparse
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional

from .botmaker import BotmakerClient, stream_chats, stream_messages
from .checkpoints import CheckpointStore
from .config import Settings, get_settings
from .logging_setup import setup_logging
from .storage import make_storage

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract chats, contacts and messages from Botmaker")
    parser.add_argument("--from", dest="from_iso", help="Start datetime ISO-8601")
    parser.add_argument("--to", dest="to_iso", help="End datetime ISO-8601")
    parser.add_argument("--max-chats", type=int, default=None, help="Limit number of chats for this run")
    parser.add_argument(
        "--messages-per-chat",
        type=int,
        default=None,
        help="Limit number of messages per chat (omit for all)",
    )
    parser.add_argument("--skip-messages", action="store_true", help="Skip downloading messages")
    parser.add_argument("--long-term", action="store_true", help="Enable long-term search for messages")
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="Optional prefix under data directory. Defaults to botmaker/run-<timestamp>",
    )
    parser.add_argument("--reset-checkpoints", action="store_true", help="Ignore stored checkpoints")
    return parser.parse_args()


def default_window(settings, args) -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    default_end = args.to_iso or settings.extract_end or now.isoformat()
    if args.from_iso:
        default_start = args.from_iso
    elif settings.extract_start:
        default_start = settings.extract_start
    else:
        default_start = (now - timedelta(days=1)).isoformat()
    return default_start, default_end


def build_contact_record(chat_dict: Dict) -> Dict:
    return {
        "contact_id": chat_dict.get("contact_id"),
        "channel_id": chat_dict.get("channel_id"),
        "chat_id": chat_dict.get("chat_id"),
        "first_name": chat_dict.get("first_name"),
        "last_name": chat_dict.get("last_name"),
        "email": chat_dict.get("email"),
        "country": chat_dict.get("country"),
        "external_id": chat_dict.get("external_id"),
        "variables": chat_dict.get("variables"),
        "tags": chat_dict.get("tags"),
        "inserted_at": datetime.now(timezone.utc).isoformat(),
        "exported_to_chatwoot": False,
        "exported_at": None,
    }


def run_sample_extract(
    *,
    settings: Optional[Settings] = None,
    max_chats: int = 1,
    messages_per_chat: Optional[int] = 1,
    skip_messages: bool = False,
    long_term: bool = False,
) -> Dict[str, object]:
    """Execute uma extração em memória limitada para uso em testes/web."""

    resolved_settings = settings or get_settings()
    args_like = SimpleNamespace(from_iso=None, to_iso=None)
    from_iso, to_iso = default_window(resolved_settings, args_like)

    client = BotmakerClient(
        resolved_settings.botmaker_base_url,
        resolved_settings.botmaker_api_token,
        resolved_settings.rate_limit_rps,
    )

    contacts: Dict[str, Dict] = {}
    chats_output: List[Dict] = []
    messages_output: List[Dict] = []

    try:
        for chat in stream_chats(
            client,
            from_iso=from_iso,
            to_iso=to_iso,
            limit=max_chats,
        ):
            chat_dict = asdict(chat)
            chats_output.append(chat_dict)

            contact_id = chat_dict.get("contact_id")
            if contact_id and contact_id not in contacts:
                contacts[contact_id] = build_contact_record(chat_dict)

            if skip_messages:
                continue

            fetched = 0
            for message in stream_messages(
                client,
                chat_id=chat.chat_id,
                channel_id=chat.channel_id,
                contact_id=chat.contact_id,
                limit=messages_per_chat,
                long_term_search=long_term,
            ):
                msg_dict = asdict(message)
                messages_output.append(msg_dict)
                fetched += 1
                if messages_per_chat and fetched >= messages_per_chat:
                    break

        summary = {
            "type": "extract_sample",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "window": {"from": from_iso, "to": to_iso},
            "limits": {
                "max_chats": max_chats,
                "messages_per_chat": messages_per_chat,
                "skip_messages": skip_messages,
                "long_term_search": long_term,
            },
            "counts": {
                "contacts": len(contacts),
                "chats": len(chats_output),
                "messages": len(messages_output),
            },
        }

        return {
            "summary": summary,
            "contacts": list(contacts.values()),
            "chats": chats_output,
            "messages": messages_output,
        }
    finally:
        client.close()


def main() -> None:
    args = parse_args()
    settings = get_settings()

    setup_logging(settings.log_dir)
    logger.info("Starting Botmaker extraction")

    storage = make_storage(settings.storage_backend, settings.data_dir)
    checkpoints = CheckpointStore(settings.mappings_dir)

    from_iso, to_iso = default_window(settings, args)
    if args.reset_checkpoints:
        checkpoints.delete("last_extract")

    prefix = args.output_prefix
    if not prefix:
        prefix = f"botmaker/run-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"

    export_meta = {
        "from": from_iso,
        "to": to_iso,
        "skip_messages": args.skip_messages,
        "long_term_search": args.long_term,
    }

    client = BotmakerClient(
        settings.botmaker_base_url,
        settings.botmaker_api_token,
        settings.rate_limit_rps,
    )

    contacts: Dict[str, Dict] = {}
    chats_output: List[Dict] = []
    messages_output: List[Dict] = []

    try:
        for chat in stream_chats(
            client,
            from_iso=from_iso,
            to_iso=to_iso,
            limit=args.max_chats,
        ):
            chat_dict = asdict(chat)
            chats_output.append(chat_dict)

            contact_id = chat_dict.get("contact_id")
            if contact_id and contact_id not in contacts:
                contacts[contact_id] = build_contact_record(chat_dict)

            if args.skip_messages:
                continue

            per_chat_limit = args.messages_per_chat
            count = 0
            for message in stream_messages(
                client,
                chat_id=chat.chat_id,
                channel_id=chat.channel_id,
                contact_id=chat.contact_id,
                long_term_search=args.long_term,
            ):
                msg_dict = asdict(message)
                messages_output.append(msg_dict)
                count += 1
                if per_chat_limit and count >= per_chat_limit:
                    break

        storage.write_ndjson(f"{prefix}/chats.ndjson", chats_output)
        storage.write_ndjson(f"{prefix}/contacts.ndjson", contacts.values())
        if not args.skip_messages:
            storage.write_ndjson(f"{prefix}/messages.ndjson", messages_output)

        export_meta.update(
            {
                "chats_exported": len(chats_output),
                "contacts_exported": len(contacts),
                "messages_exported": len(messages_output),
                "output_prefix": prefix,
            }
        )
        checkpoints.set("last_extract", export_meta)
        # Write extraction summary for web frontend
        summary = {
            "type": "extract",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "prefix": prefix,
            "window": {"from": from_iso, "to": to_iso},
            "counts": {
                "contacts": len(contacts),
                "chats": len(chats_output),
                "messages": len(messages_output),
            },
            "files": {
                "contacts": "contacts.ndjson",
                "chats": "chats.ndjson",
                "messages": "messages.ndjson",
            },
        }
        storage.write_json(f"{prefix}/summary.json", summary)
        logger.info(
            "Extraction finished",
            extra={
                "chats": len(chats_output),
                "contacts": len(contacts),
                "messages": len(messages_output),
                "prefix": prefix,
            },
        )

    finally:
        client.close()


if __name__ == "__main__":
    main()
