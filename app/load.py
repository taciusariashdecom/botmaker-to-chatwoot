from __future__ import annotations

import argparse
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .chatwoot import ChatwootClient
from .config import get_settings
from .logging_setup import setup_logging
from .storage import make_storage
from .mapping_store import MappingStore
from .checkpoints import CheckpointStore
from .models import BotmakerChat, BotmakerMessage

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Botmaker exports into Chatwoot")
    parser.add_argument("--input-prefix", required=True, help="Relative path under data directory")
    parser.add_argument("--dry-run", action="store_true", help="Do not call Chatwoot APIs")
    parser.add_argument("--limit-chats", type=int, default=None, help="Limit number of chats to import")
    parser.add_argument("--limit-messages", type=int, default=None, help="Limit total messages to import")
    parser.add_argument("--chunk-size", type=int, default=None, help="Override default chunk size for message batching")
    parser.add_argument("--skip-messages", action="store_true", help="Skip message replay phase")
    parser.add_argument("--skip-conversations", action="store_true", help="Skip conversation creation phase")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Remove stored loader checkpoint before running")
    return parser.parse_args()


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_list(obj: Any) -> List[Dict[str, Any]]:
    return list(obj) if obj else []


def contact_payload(contact: Dict[str, Any]) -> Dict[str, Any]:
    full_name = " ".join(filter(None, [contact.get("first_name"), contact.get("last_name")])).strip()
    payload: Dict[str, Any] = {
        "name": full_name or contact.get("first_name") or contact.get("last_name") or contact.get("contact_id"),
        "identifier": contact.get("contact_id"),
        "email": contact.get("email"),
        "avatar_url": None,
        "phone_number": contact.get("contact_id") if contact.get("contact_id", "").isdigit() else None,
        "custom_attributes": {
            "botmaker_channel_id": contact.get("channel_id"),
            "botmaker_chat_id": contact.get("chat_id"),
            "botmaker_external_id": contact.get("external_id"),
            "botmaker_tags": contact.get("tags", []),
            "botmaker_variables": contact.get("variables", {}),
            "botmaker_inserted_at": contact.get("inserted_at"),
        },
    }
    # Remove keys with None to avoid API validation errors.
    return {k: v for k, v in payload.items() if v not in (None, "")}


def additional_attributes_for_chat(chat: BotmakerChat) -> Dict[str, Any]:
    return {
        "botmaker_chat_id": chat.chat_id,
        "botmaker_contact_id": chat.contact_id,
        "botmaker_channel_id": chat.channel_id,
        "botmaker_queue_id": chat.queue_id,
        "botmaker_agent_id": chat.agent_id,
        "botmaker_tags": chat.tags,
        "botmaker_variables": chat.variables,
        "botmaker_creation_time": chat.creation_time,
        "botmaker_last_user_message": chat.last_user_message_datetime,
    }


def determine_message_content(message: BotmakerMessage) -> str:
    content = message.content or {}
    message_type = content.get("type")
    if message_type == "text" and content.get("text"):
        return content.get("text")
    if message_type == "buttons" and content.get("selectedButton"):
        return f"[Button] {content.get('selectedButton')}"
    if message_type == "image" and content.get("media"):
        return f"[Image] {content['media'].get('url', 'binary')}"
    if message_type == "audio" and content.get("media"):
        return f"[Audio] {content['media'].get('url', 'binary')}"
    if message_type == "file" and content.get("media"):
        return f"[File] {content['media'].get('url', 'binary')}"
    if content.get("originalText"):
        return content["originalText"]
    return f"[{message_type or 'unknown'} message without text]"


def message_type_for_chatwoot(message: BotmakerMessage) -> str:
    sender = (message.sender or "").lower()
    if sender == "user":
        return "incoming"
    return "outgoing"


def message_payload(message: BotmakerMessage) -> Dict[str, Any]:
    payload = {
        "content": determine_message_content(message),
        "message_type": message_type_for_chatwoot(message),
        "content_attributes": {
            "botmaker": message.content,
            "original_sent_at": message.creation_time,
            "botmaker_message_id": message.id,
            "botmaker_session_id": message.session_id,
            "botmaker_sender": message.sender,
        },
    }
    if payload["message_type"] == "outgoing" and message.sender == "agent":
        payload["private"] = False
    return payload


def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_dir)

    storage = make_storage(settings.storage_backend, settings.data_dir)
    checkpoints = CheckpointStore(settings.mappings_dir, filename="loader_checkpoint.json")
    if args.reset_checkpoint:
        checkpoints.delete("last_load")

    contact_map = MappingStore(settings.mappings_dir, "contact_map.json")
    conversation_map = MappingStore(settings.mappings_dir, "conversation_map.json")
    message_map = MappingStore(settings.mappings_dir, "message_map.json")

    chunk_size = args.chunk_size or settings.chunk_size

    input_prefix = args.input_prefix.rstrip("/")
    contacts_path = f"{input_prefix}/contacts.ndjson"
    chats_path = f"{input_prefix}/chats.ndjson"
    messages_path = f"{input_prefix}/messages.ndjson"

    contacts_records = ensure_list(storage.read_ndjson(contacts_path))
    chats_records = ensure_list(storage.read_ndjson(chats_path))
    messages_records = ensure_list(storage.read_ndjson(messages_path)) if not args.skip_messages else []

    logger.info(
        "Loaded export references",
        extra={
            "contacts": len(contacts_records),
            "chats": len(chats_records),
            "messages": len(messages_records),
        },
    )

    client = ChatwootClient(
        settings.chatwoot_base_url,
        settings.chatwoot_api_access_token,
        settings.rate_limit_rps,
    )

    contacts_updated: List[Dict[str, Any]] = []
    chats_updated: List[Dict[str, Any]] = []
    messages_updated: List[Dict[str, Any]] = []

    try:
        # Contacts
        for raw_contact in contacts_records:
            contact_id = raw_contact.get("contact_id")
            if not contact_id:
                continue

            mapping = contact_map.get(contact_id)
            if mapping:
                raw_contact["exported_to_chatwoot"] = True
                raw_contact["exported_at"] = mapping.get("exported_at")
            elif args.dry_run:
                logger.info("[DRY-RUN] Would create Chatwoot contact", extra={"contact_id": contact_id})
            else:
                payload = contact_payload(raw_contact)
                response = client.create_contact(settings.chatwoot_account_id, payload)
                exported_at = iso_now()
                contact_map.set(
                    contact_id,
                    {
                        "chatwoot_contact_id": response.get("id"),
                        "exported_at": exported_at,
                        "payload": payload,
                    },
                )
                raw_contact["exported_to_chatwoot"] = True
                raw_contact["exported_at"] = exported_at
                logger.info("Created Chatwoot contact", extra={"contact_id": contact_id, "chatwoot_id": response.get("id")})

            contacts_updated.append(raw_contact)

        if args.skip_conversations and not args.skip_messages:
            logger.warning("Skipping conversations but attempting to send messages may fail if conversations do not exist")

        # Conversations
        chat_limit_counter = 0
        for chat_record in chats_records:
            chat_limit_counter += 1
            if args.limit_chats and chat_limit_counter > args.limit_chats:
                break

            chat = BotmakerChat(**chat_record)
            mapping = conversation_map.get(chat.chat_id)
            if mapping:
                chat_record["exported_to_chatwoot"] = True
                chat_record["exported_at"] = mapping.get("exported_at")
                chats_updated.append(chat_record)
                continue

            contact_mapping = contact_map.get(chat.contact_id)
            if not contact_mapping:
                logger.warning("Contact not exported yet; skipping chat", extra={"chat_id": chat.chat_id, "contact_id": chat.contact_id})
                chats_updated.append(chat_record)
                continue

            if args.skip_conversations:
                chats_updated.append(chat_record)
                continue

            if args.dry_run:
                logger.info(
                    "[DRY-RUN] Would create Chatwoot conversation",
                    extra={"chat_id": chat.chat_id, "contact_id": chat.contact_id},
                )
                chats_updated.append(chat_record)
                continue

            payload = {
                "source_id": chat.chat_id,
                "inbox_id": settings.chatwoot_inbox_id,
                "contact_id": contact_mapping.get("chatwoot_contact_id"),
                "additional_attributes": additional_attributes_for_chat(chat),
            }
            response = client.create_conversation(settings.chatwoot_account_id, payload)
            exported_at = iso_now()
            conversation_map.set(
                chat.chat_id,
                {
                    "chatwoot_conversation_id": response.get("id"),
                    "chatwoot_contact_id": contact_mapping.get("chatwoot_contact_id"),
                    "exported_at": exported_at,
                },
            )
            chat_record["exported_to_chatwoot"] = True
            chat_record["exported_at"] = exported_at
            chats_updated.append(chat_record)
            logger.info(
                "Created Chatwoot conversation",
                extra={"chat_id": chat.chat_id, "conversation_id": response.get("id")},
            )

        # Messages
        if not args.skip_messages:
            total_messages_processed = 0
            for message_record in messages_records:
                if args.limit_messages and total_messages_processed >= args.limit_messages:
                    break

                message = BotmakerMessage(**message_record)
                mapping = message_map.get(message.id)
                if mapping:
                    message_record["exported_to_chatwoot"] = True
                    message_record["exported_at"] = mapping.get("exported_at")
                    messages_updated.append(message_record)
                    total_messages_processed += 1
                    continue

                conversation_mapping = conversation_map.get(message.chat_id)
                if not conversation_mapping:
                    logger.warning(
                        "Conversation missing for message; skipping",
                        extra={"message_id": message.id, "chat_id": message.chat_id},
                    )
                    messages_updated.append(message_record)
                    continue

                if args.dry_run:
                    logger.info(
                        "[DRY-RUN] Would create Chatwoot message",
                        extra={"message_id": message.id, "chat_id": message.chat_id},
                    )
                    messages_updated.append(message_record)
                    total_messages_processed += 1
                    continue

                payload = message_payload(message)
                response = client.create_message(
                    settings.chatwoot_account_id,
                    conversation_mapping.get("chatwoot_conversation_id"),
                    payload,
                )
                exported_at = iso_now()
                message_map.set(
                    message.id,
                    {
                        "chatwoot_message_id": response.get("id"),
                        "conversation_id": conversation_mapping.get("chatwoot_conversation_id"),
                        "exported_at": exported_at,
                    },
                )
                message_record["exported_to_chatwoot"] = True
                message_record["exported_at"] = exported_at
                messages_updated.append(message_record)
                total_messages_processed += 1

            logger.info("Messages processed", extra={"messages": total_messages_processed})

        # Persist status snapshots
        storage.write_ndjson(f"{input_prefix}/contacts_export_status.ndjson", contacts_updated)
        storage.write_ndjson(f"{input_prefix}/chats_export_status.ndjson", chats_updated)
        if messages_updated:
            storage.write_ndjson(f"{input_prefix}/messages_export_status.ndjson", messages_updated)

        checkpoints.set(
            "last_load",
            {
                "input_prefix": input_prefix,
                "contacts_processed": len(contacts_updated),
                "chats_processed": len(chats_updated),
                "messages_processed": len(messages_updated),
                "dry_run": args.dry_run,
                "timestamp": iso_now(),
            },
        )
        # Write loader summary for web frontend
        storage.write_json(
            f"{input_prefix}/load_summary.json",
            {
                "type": "load",
                "timestamp": iso_now(),
                "prefix": input_prefix,
                "counts": {
                    "contacts_updated": len(contacts_updated),
                    "chats_updated": len(chats_updated),
                    "messages_updated": len(messages_updated),
                },
                "dry_run": args.dry_run,
            },
        )

    finally:
        client.close()


if __name__ == "__main__":
    main()
