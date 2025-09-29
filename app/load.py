from __future__ import annotations

import argparse
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
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


def _extract_first_contact_id(obj: Any) -> Optional[int]:
    """Try to find a contact id from various Chatwoot search response shapes.
    Looks for integer 'id' at top-level items or nested under 'contact'.
    """
    def _iter_candidates(o: Any):
        if isinstance(o, dict):
            # direct
            yield o
            # nested typical keys
            for k in ("payload", "data", "items", "contacts", "result", "results"):
                if k in o:
                    yield from _iter_candidates(o[k])
        elif isinstance(o, list):
            for it in o:
                yield from _iter_candidates(it)

    for cand in _iter_candidates(obj):
        if not isinstance(cand, dict):
            continue
        # direct id
        cid = cand.get("id")
        if isinstance(cid, int):
            return cid
        # nested contact.id
        contact = cand.get("contact")
        if isinstance(contact, dict) and isinstance(contact.get("id"), int):
            return contact.get("id")
    return None


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        # Accept both '...Z' and with timezone offset
        if ts.endswith("Z"):
            ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _date_str(ts: Optional[str]) -> Optional[str]:
    dt = _parse_iso(ts)
    if not dt:
        return None
    return dt.date().isoformat()


def _is_whatsapp(channel_id: Optional[str]) -> bool:
    s = (channel_id or "").lower()
    return "whatsapp" in s


def compute_interactions(
    chats: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, bool], Dict[str, bool]]:
    """Compute last interaction per chat/contact and whatsapp flags.

    Returns:
      chat_last_iso: chat_id -> last ISO timestamp
      contact_last_iso: contact_id -> last ISO timestamp
      chat_is_whatsapp: chat_id -> bool
      contact_is_whatsapp: contact_id -> bool
    """
    chat_last_iso: Dict[str, str] = {}
    contact_last_iso: Dict[str, str] = {}
    chat_is_whatsapp: Dict[str, bool] = {}
    contact_is_whatsapp: Dict[str, bool] = {}

    # initialize channel flags from chats
    for rec in chats:
        cid = rec.get("chat_id")
        contact_id = rec.get("contact_id")
        ch = rec.get("channel_id")
        if cid:
            chat_is_whatsapp[cid] = _is_whatsapp(ch)
        if contact_id:
            # OR aggregation at contact level
            contact_is_whatsapp[contact_id] = contact_is_whatsapp.get(contact_id, False) or _is_whatsapp(ch)

    for m in messages:
        mid = m.get("creation_time")
        chat_id = m.get("chat_id")
        contact_id = m.get("contact_id")
        if mid:
            if chat_id:
                prev = chat_last_iso.get(chat_id)
                if not prev or (_parse_iso(mid) and _parse_iso(prev) and _parse_iso(mid) > _parse_iso(prev)):
                    chat_last_iso[chat_id] = mid
                prevc = contact_last_iso.get(contact_id)
                if not prevc or (_parse_iso(mid) and _parse_iso(prevc) and _parse_iso(mid) > _parse_iso(prevc)):
                    contact_last_iso[contact_id] = mid
    return chat_last_iso, contact_last_iso, chat_is_whatsapp, contact_is_whatsapp


def _sanitize_labels(values: Optional[List[Any]]) -> List[str]:
    out: List[str] = []
    if not isinstance(values, list):
        return out
    seen = set()
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def contact_payload(contact: Dict[str, Any]) -> Dict[str, Any]:
    full_name = " ".join(filter(None, [contact.get("first_name"), contact.get("last_name")])).strip()
    # Normalize phone: Chatwoot expects E.164 format with leading '+'.
    raw_phone = contact.get("contact_id")
    phone_e164 = None
    if isinstance(raw_phone, str) and raw_phone.isdigit():
        phone_e164 = "+" + raw_phone

    payload: Dict[str, Any] = {
        "name": full_name or contact.get("first_name") or contact.get("last_name") or contact.get("contact_id"),
        "identifier": contact.get("contact_id"),
        "email": contact.get("email"),
        "avatar_url": None,
        "phone_number": phone_e164,
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


def compute_last_agents(
    messages: List[Dict[str, Any]]
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str]]:
    """Return last agent per chat/contact and timestamps.

    Returns:
      chat_last_agent_id, chat_last_agent_iso, contact_last_agent_id, contact_last_agent_iso
    """
    chat_last_agent_id: Dict[str, str] = {}
    chat_last_agent_iso: Dict[str, str] = {}
    contact_last_agent_id: Dict[str, str] = {}
    contact_last_agent_iso: Dict[str, str] = {}
    for m in messages:
        sender = (m.get("sender") or "").lower()
        agent_id = m.get("agent_id")
        if sender == "agent" or (agent_id and sender != "user"):
            ts = m.get("creation_time")
            chat_id = m.get("chat_id")
            contact_id = m.get("contact_id")
            if chat_id and ts:
                prev = chat_last_agent_iso.get(chat_id)
                if not prev or (_parse_iso(ts) and _parse_iso(prev) and _parse_iso(ts) > _parse_iso(prev)):
                    if agent_id:
                        chat_last_agent_id[chat_id] = str(agent_id)
                    chat_last_agent_iso[chat_id] = ts
            if contact_id and ts:
                prevc = contact_last_agent_iso.get(contact_id)
                if not prevc or (_parse_iso(ts) and _parse_iso(prevc) and _parse_iso(ts) > _parse_iso(prevc)):
                    if agent_id:
                        contact_last_agent_id[contact_id] = str(agent_id)
                    contact_last_agent_iso[contact_id] = ts
    return chat_last_agent_id, chat_last_agent_iso, contact_last_agent_id, contact_last_agent_iso


def _conversation_note_for_chat(
    chat: BotmakerChat,
    last_iso: Optional[str],
    is_whatsapp: Optional[bool],
    last_agent_id: Optional[str] = None,
    last_agent_iso: Optional[str] = None,
) -> Optional[str]:
    parts: List[str] = []
    parts.append("Importado de Botmaker")
    if chat.agent_id:
        parts.append(f"Agente (inicial): {chat.agent_id}")
    if chat.queue_id:
        parts.append(f"Fila: {chat.queue_id}")
    if is_whatsapp is not None:
        parts.append(f"Canal: {'whatsapp' if is_whatsapp else 'outro'} ({chat.channel_id})")
    if last_iso:
        parts.append(f"Última interação: {last_iso}")
    if last_agent_id:
        if last_agent_iso:
            parts.append(f"Último atendente: {last_agent_id} em {last_agent_iso}")
        else:
            parts.append(f"Último atendente: {last_agent_id}")
    return "\n".join(parts) if parts else None


def _contact_note_for_contact(raw_contact: Dict[str, Any], last_agent_id: Optional[str] = None) -> Optional[str]:
    tags = raw_contact.get("tags") or []
    variables = raw_contact.get("variables") or {}
    parts: List[str] = ["Importado de Botmaker (contato)"]
    if last_agent_id:
        parts.append(f"Último atendente: {last_agent_id}")
    if tags:
        parts.append(f"Tags: {', '.join([str(t) for t in tags])}")
    if variables:
        keys = ", ".join(sorted([str(k) for k in variables.keys()]))
        parts.append(f"Variáveis: {keys}")
    return "\n".join(parts)


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
    # Compute last interaction timestamps and channel flags
    chat_last_iso, contact_last_iso, chat_is_whatsapp, contact_is_whatsapp = compute_interactions(
        chats_records, messages_records
    )
    # Compute last agents per chat/contact
    (
        chat_last_agent_id,
        chat_last_agent_iso,
        contact_last_agent_id,
        contact_last_agent_iso,
    ) = compute_last_agents(messages_records)

    logger.info(
        "Loaded export references",
        extra={
            "contacts": len(contacts_records),
            "chats": len(chats_records),
            "messages": len(messages_records),
        },
    )

    client = None
    if not args.dry_run:
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
            if mapping and not mapping.get("chatwoot_contact_id"):
                logger.warning(
                    "Contact mapping exists without Chatwoot ID; will reconcile",
                    extra={"contact_id": contact_id},
                )
                mapping = None
            if mapping:
                raw_contact["exported_to_chatwoot"] = True
                raw_contact["exported_at"] = mapping.get("exported_at")
                # Optionally update attributes (whatsapp, data_ultima_interacao) and add a note once
                if client is not None and mapping.get("chatwoot_contact_id"):
                    try:
                        cw_cid = int(mapping.get("chatwoot_contact_id"))
                        ca: Dict[str, Any] = {}
                        w = contact_is_whatsapp.get(contact_id)
                        if w is not None:
                            ca["whatsapp"] = bool(w)
                        du = _date_str(contact_last_iso.get(contact_id))
                        if du is not None:
                            ca["data_ultima_interacao"] = du
                        la = contact_last_agent_id.get(contact_id)
                        if la:
                            ca["ultimo_atendente"] = la
                        if ca:
                            client.update_contact(settings.chatwoot_account_id, cw_cid, {"custom_attributes": ca})
                    except Exception:
                        logger.exception("Failed to update contact attributes", extra={"contact_id": contact_id})
                    # Backfill a contact note if not seeded yet
                    try:
                        if not mapping.get("contact_note_seeded"):
                            note = _contact_note_for_contact(raw_contact, contact_last_agent_id.get(contact_id))
                            if note:
                                client.create_contact_note(settings.chatwoot_account_id, int(cw_cid), {"content": note})
                                mapping["contact_note_seeded"] = True
                                contact_map.set(contact_id, mapping)
                    except Exception:
                        logger.exception("Failed to backfill contact note", extra={"contact_id": contact_id})
            elif args.dry_run:
                logger.info("[DRY-RUN] Would create Chatwoot contact", extra={"contact_id": contact_id})
            else:
                payload = contact_payload(raw_contact)
                try:
                    response = client.create_contact(settings.chatwoot_account_id, payload)
                    cw_id = response.get("id") if isinstance(response, dict) else None
                    if cw_id is None:
                        cw_id = _extract_first_contact_id(response)
                    if cw_id is None:
                        # Try to find the contact we just created via search
                        queries: List[str] = []
                        for key in ("identifier", "email", "phone_number", "name"):
                            val = payload.get(key)
                            if val:
                                queries.append(str(val))
                        for q in queries:
                            search = client.search_contacts(settings.chatwoot_account_id, q)
                            cid = _extract_first_contact_id(search)
                            if cid:
                                cw_id = cid
                                break
                    if cw_id is None:
                        logger.error(
                            "Contact created but could not extract Chatwoot id",
                            extra={"contact_id": contact_id, "response": response},
                        )
                        raise RuntimeError("Missing Chatwoot contact id after creation")

                    exported_at = iso_now()
                    contact_map.set(
                        contact_id,
                        {
                            "chatwoot_contact_id": cw_id,
                            "exported_at": exported_at,
                            "payload": payload,
                        },
                    )
                    raw_contact["exported_to_chatwoot"] = True
                    raw_contact["exported_at"] = exported_at
                    logger.info(
                        "Created Chatwoot contact",
                        extra={"contact_id": contact_id, "chatwoot_id": cw_id},
                    )
                    # After creation, set whatsapp + data_ultima_interacao, and add optional note
                    try:
                        ca: Dict[str, Any] = {}
                        w = contact_is_whatsapp.get(contact_id)
                        if w is not None:
                            ca["whatsapp"] = bool(w)
                        du = _date_str(contact_last_iso.get(contact_id))
                        if du is not None:
                            ca["data_ultima_interacao"] = du
                        la = contact_last_agent_id.get(contact_id)
                        if la:
                            ca["ultimo_atendente"] = la
                        if ca:
                            client.update_contact(settings.chatwoot_account_id, int(cw_id), {"custom_attributes": ca})
                    except Exception:
                        logger.exception("Failed to set contact attributes post-create", extra={"contact_id": contact_id})
                    try:
                        note = _contact_note_for_contact(raw_contact, contact_last_agent_id.get(contact_id))
                        if note:
                            client.create_contact_note(settings.chatwoot_account_id, int(cw_id), {"content": note})
                    except Exception:
                        logger.exception("Failed to create contact note", extra={"contact_id": contact_id})
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code if exc.response is not None else None
                    if status == 422:
                        try:
                            logger.warning("Chatwoot 422 create_contact response: %s", exc.response.text if exc.response else None)
                        except Exception:
                            pass
                        # Likely identifier/email conflict. Try to find existing contact via search.
                        queries: List[str] = []
                        for key in ("identifier", "email", "phone_number", "name"):
                            val = payload.get(key)
                            if val:
                                queries.append(str(val))
                        chatwoot_contact_id = None
                        try:
                            # 1) Free-text search attempts
                            for q in queries:
                                search = client.search_contacts(settings.chatwoot_account_id, q)
                                cid = _extract_first_contact_id(search)
                                if cid:
                                    chatwoot_contact_id = cid
                                    break
                            # 2) List endpoint with direct filters (best-effort)
                            if not chatwoot_contact_id:
                                if payload.get("identifier"):
                                    res = client.list_contacts(settings.chatwoot_account_id, identifier=payload["identifier"])  # type: ignore[arg-type]
                                    cid = _extract_first_contact_id(res)
                                    if cid:
                                        chatwoot_contact_id = cid
                                if not chatwoot_contact_id and payload.get("email"):
                                    res = client.list_contacts(settings.chatwoot_account_id, email=payload["email"])  # type: ignore[arg-type]
                                    cid = _extract_first_contact_id(res)
                                    if cid:
                                        chatwoot_contact_id = cid
                                if not chatwoot_contact_id and payload.get("phone_number"):
                                    res = client.list_contacts(settings.chatwoot_account_id, phone_number=payload["phone_number"])  # type: ignore[arg-type]
                                    cid = _extract_first_contact_id(res)
                                    if cid:
                                        chatwoot_contact_id = cid
                            if chatwoot_contact_id:
                                exported_at = iso_now()
                                contact_map.set(
                                    contact_id,
                                    {
                                        "chatwoot_contact_id": chatwoot_contact_id,
                                        "exported_at": exported_at,
                                        "payload": payload,
                                        "reconciled": True,
                                    },
                                )
                                raw_contact["exported_to_chatwoot"] = True
                                raw_contact["exported_at"] = exported_at
                                logger.info(
                                    "Reused existing Chatwoot contact via search",
                                    extra={"contact_id": contact_id, "chatwoot_id": chatwoot_contact_id},
                                )
                            else:
                                # 3) Try alternative payloads (avoid conflicting field)
                                alt_payloads: List[Dict[str, Any]] = []
                                # a) drop phone_number
                                p1 = {k: v for k, v in payload.items() if k != "phone_number"}
                                alt_payloads.append(p1)
                                # b) drop identifier (keep phone/email)
                                p2 = {k: v for k, v in payload.items() if k != "identifier"}
                                alt_payloads.append(p2)

                                alt_created = False
                                for ap in alt_payloads:
                                    try:
                                        r = client.create_contact(settings.chatwoot_account_id, ap)
                                        exported_at = iso_now()
                                        contact_map.set(
                                            contact_id,
                                            {
                                                "chatwoot_contact_id": r.get("id"),
                                                "exported_at": exported_at,
                                                "payload": ap,
                                                "reconciled": True,
                                                "note": "created via alternative payload after 422",
                                            },
                                        )
                                        raw_contact["exported_to_chatwoot"] = True
                                        raw_contact["exported_at"] = exported_at
                                        logger.info(
                                            "Created Chatwoot contact via alternative payload",
                                            extra={"contact_id": contact_id, "chatwoot_id": r.get("id")},
                                        )
                                        alt_created = True
                                        break
                                    except httpx.HTTPStatusError as e2:
                                        logger.warning(
                                            "Alt payload also failed: %s",
                                            e2.response.text if e2.response else str(e2),
                                        )
                                if not alt_created:
                                    logger.error(
                                        "422 creating contact; no match found; alt payloads failed",
                                        extra={
                                            "contact_id": contact_id,
                                            "original_payload": payload,
                                            "response": exc.response.text if exc.response else None,
                                        },
                                    )
                                    raise
                        except Exception:
                            # Bubble up with context
                            logger.exception(
                                "Failed to reconcile Chatwoot contact after 422",
                                extra={"contact_id": contact_id, "payload": payload},
                            )
                            raise
                    else:
                        raise

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
                # Backfill conversation attributes and note once
                if client is not None and mapping.get("chatwoot_conversation_id"):
                    try:
                        conv_id = int(mapping.get("chatwoot_conversation_id"))
                        # enrich current attributes
                        add_attrs = additional_attributes_for_chat(chat)
                        w = chat_is_whatsapp.get(chat.chat_id)
                        if w is not None:
                            add_attrs["whatsapp"] = bool(w)
                        du = _date_str(chat_last_iso.get(chat.chat_id))
                        if du is not None:
                            add_attrs["data_ultima_interacao"] = du
                        la = chat_last_agent_id.get(chat.chat_id)
                        if la:
                            add_attrs["ultimo_atendente"] = la
                        client.update_conversation(
                            settings.chatwoot_account_id, conv_id, {"additional_attributes": add_attrs}
                        )
                    except Exception:
                        logger.exception(
                            "Failed to backfill conversation attributes",
                            extra={"chat_id": chat.chat_id},
                        )
                    # Labels (tags)
                    try:
                        labels = _sanitize_labels(chat.tags)
                        if labels:
                            client.add_conversation_labels(settings.chatwoot_account_id, conv_id, labels)
                    except Exception:
                        logger.exception(
                            "Failed to backfill conversation labels",
                            extra={"chat_id": chat.chat_id, "labels": chat.tags},
                        )
                    # Note
                    try:
                        if not mapping.get("conversation_note_seeded"):
                            note = _conversation_note_for_chat(
                                chat,
                                chat_last_iso.get(chat.chat_id),
                                chat_is_whatsapp.get(chat.chat_id),
                                chat_last_agent_id.get(chat.chat_id),
                                chat_last_agent_iso.get(chat.chat_id),
                            )
                            if note:
                                client.create_message(
                                    settings.chatwoot_account_id,
                                    int(mapping.get("chatwoot_conversation_id")),
                                    {"content": note, "private": True, "message_type": "outgoing"},
                                )
                                mapping["conversation_note_seeded"] = True
                                conversation_map.set(chat.chat_id, mapping)
                    except Exception:
                        logger.exception("Failed to backfill conversation note", extra={"chat_id": chat.chat_id})
                chats_updated.append(chat_record)
                continue

            contact_mapping = contact_map.get(chat.contact_id)
            if not contact_mapping or not contact_mapping.get("chatwoot_contact_id"):
                # Try to reconcile by searching Chatwoot (identifier/phone/email)
                try:
                    queries: List[str] = []
                    # identifier (raw contact_id)
                    if chat.contact_id:
                        queries.append(str(chat.contact_id))
                    # E.164 phone
                    if chat.contact_id and str(chat.contact_id).isdigit():
                        queries.append("+" + str(chat.contact_id))
                    cw_cid: Optional[int] = None
                    for q in queries:
                        search = client.search_contacts(settings.chatwoot_account_id, q)
                        cid = _extract_first_contact_id(search)
                        if cid:
                            cw_cid = cid
                            break
                    if not cw_cid:
                        # Try list endpoint with filters
                        for params in (
                            {"identifier": chat.contact_id},
                            {"phone_number": ("+" + str(chat.contact_id)) if str(chat.contact_id).isdigit() else None},
                        ):
                            params = {k: v for k, v in params.items() if v}
                            if not params:
                                continue
                            res = client.list_contacts(settings.chatwoot_account_id, **params)
                            cid = _extract_first_contact_id(res)
                            if cid:
                                cw_cid = cid
                                break
                    if not cw_cid:
                        # As a last resort, create the contact from chat info
                        chat_like = {
                            "contact_id": chat.contact_id,
                            "channel_id": chat.channel_id,
                            "first_name": chat.first_name,
                            "last_name": chat.last_name,
                            "email": chat.email,
                            "variables": chat.variables,
                            "tags": chat.tags,
                            "inserted_at": chat.inserted_at,
                        }
                        cp = contact_payload(chat_like)
                        resp = client.create_contact(settings.chatwoot_account_id, cp)
                        cw_cid = resp.get("id") if isinstance(resp, dict) else _extract_first_contact_id(resp)
                    if cw_cid:
                        exported_at = iso_now()
                        contact_map.set(
                            chat.contact_id,
                            {
                                "chatwoot_contact_id": cw_cid,
                                "exported_at": exported_at,
                                "payload": {"identifier": chat.contact_id},
                                "reconciled": True,
                            },
                        )
                        contact_mapping = contact_map.get(chat.contact_id)
                    else:
                        logger.warning(
                            "Contact not exported yet; skipping chat",
                            extra={"chat_id": chat.chat_id, "contact_id": chat.contact_id},
                        )
                        chats_updated.append(chat_record)
                        continue
                except Exception:
                    logger.exception(
                        "Failed to reconcile missing contact mapping via search/create",
                        extra={"chat_id": chat.chat_id, "contact_id": chat.contact_id},
                    )
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

            # Ensure numeric types where applicable
            inbox_id_val = settings.chatwoot_inbox_id
            try:
                inbox_id_val = int(inbox_id_val)  # type: ignore[assignment]
            except Exception:
                pass
            contact_id_val = contact_mapping.get("chatwoot_contact_id")
            try:
                contact_id_val = int(contact_id_val)  # type: ignore[assignment]
            except Exception:
                pass

            # Merge chat attributes with whatsapp + last interaction date
            add_attrs = additional_attributes_for_chat(chat)
            try:
                w = chat_is_whatsapp.get(chat.chat_id)
                if w is not None:
                    add_attrs["whatsapp"] = bool(w)
                du = _date_str(chat_last_iso.get(chat.chat_id))
                if du is not None:
                    add_attrs["data_ultima_interacao"] = du
                la = chat_last_agent_id.get(chat.chat_id)
                if la:
                    add_attrs["ultimo_atendente"] = la
            except Exception:
                logger.exception("Failed to enrich additional_attributes for chat", extra={"chat_id": chat.chat_id})

            payload = {
                "source_id": chat.chat_id,
                "inbox_id": inbox_id_val,
                "contact_id": contact_id_val,
                "additional_attributes": add_attrs,
            }
            # Pre-create contact_inbox mapping for API channel (idempotent)
            try:
                if isinstance(contact_id_val, int):
                    client.create_contact_inbox(
                        settings.chatwoot_account_id,
                        contact_id_val,
                        {"inbox_id": inbox_id_val, "source_id": chat.chat_id},
                    )
            except httpx.HTTPStatusError:
                # ignore errors, we'll still try to create conversation below
                pass
            try:
                response = client.create_conversation(settings.chatwoot_account_id, payload)
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response else None
                body = exc.response.text if exc.response else None
                logger.error(
                    "Failed to create Chatwoot conversation: status=%s body=%s payload=%s",
                    status,
                    body,
                    payload,
                )
                # If inbox/contact mapping is missing for API channel, create it and retry once.
                if status == 404 and isinstance(contact_id_val, int):
                    try:
                        map_payload = {"inbox_id": inbox_id_val, "source_id": chat.chat_id}
                        logger.info(
                            "Attempting to create contact_inbox mapping before retry",
                            extra={"contact_id": contact_id_val, "map_payload": map_payload},
                        )
                        client.create_contact_inbox(settings.chatwoot_account_id, contact_id_val, map_payload)  # type: ignore[arg-type]
                        # retry
                        response = client.create_conversation(settings.chatwoot_account_id, payload)
                    except httpx.HTTPStatusError as e2:
                        logger.error(
                            "Retry after contact_inbox mapping failed: status=%s body=%s",
                            e2.response.status_code if e2.response else None,
                            e2.response.text if e2.response else None,
                        )
                        raise
                else:
                    raise
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
            # Add labels (tags) to conversation
            try:
                conv_id = int(response.get("id"))
                labels = _sanitize_labels(chat.tags)
                if labels:
                    client.add_conversation_labels(settings.chatwoot_account_id, conv_id, labels)
            except Exception:
                logger.exception(
                    "Failed to apply conversation labels",
                    extra={"chat_id": chat.chat_id, "labels": chat.tags},
                )
            # Add conversation note with agent/queue/channel/last interaction (as private message)
            try:
                note = _conversation_note_for_chat(
                    chat,
                    chat_last_iso.get(chat.chat_id),
                    chat_is_whatsapp.get(chat.chat_id),
                    chat_last_agent_id.get(chat.chat_id),
                    chat_last_agent_iso.get(chat.chat_id),
                )
                if note:
                    client.create_message(
                        settings.chatwoot_account_id, int(response.get("id")), {"content": note, "private": True, "message_type": "outgoing"}
                    )
                    try:
                        created_mapping = conversation_map.get(chat.chat_id) or {}
                        created_mapping["conversation_note_seeded"] = True
                        conversation_map.set(chat.chat_id, created_mapping)
                    except Exception:
                        logger.exception("Failed to mark conversation note as seeded", extra={"chat_id": chat.chat_id})
            except Exception:
                logger.exception("Failed to create conversation note", extra={"chat_id": chat.chat_id})

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
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
