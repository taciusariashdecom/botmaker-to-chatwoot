from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class BotmakerChat:
    chat_id: str
    channel_id: str
    contact_id: str
    creation_time: Optional[str] = None
    external_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    country: Optional[str] = None
    email: Optional[str] = None
    variables: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    queue_id: Optional[str] = None
    agent_id: Optional[str] = None
    last_user_message_datetime: Optional[str] = None
    list_messages_url: Optional[str] = None
    inserted_at: Optional[str] = None
    exported_to_chatwoot: bool = False
    exported_at: Optional[str] = None

    @classmethod
    def from_api(cls, item: Dict[str, Any]) -> "BotmakerChat":
        chat_obj = item.get("chat", {})
        return cls(
            chat_id=chat_obj.get("chatId", ""),
            channel_id=chat_obj.get("channelId", ""),
            contact_id=chat_obj.get("contactId", ""),
            creation_time=item.get("creationTime"),
            external_id=item.get("externalId"),
            first_name=item.get("firstName"),
            last_name=item.get("lastName"),
            country=item.get("country"),
            email=item.get("email"),
            variables=item.get("variables", {}) or {},
            tags=item.get("tags", []) or [],
            queue_id=item.get("queueId"),
            agent_id=item.get("agentId"),
            last_user_message_datetime=item.get("lastUserMessageDatetime"),
            list_messages_url=item.get("listMessagesURL"),
            inserted_at=item.get("inserted_at"),
            exported_to_chatwoot=item.get("exported_to_chatwoot", False),
            exported_at=item.get("exported_at"),
        )


@dataclass
class BotmakerMessage:
    id: str
    creation_time: Optional[str]
    sender: str
    agent_id: Optional[str]
    queue_id: Optional[str]
    chat_id: str
    channel_id: str
    contact_id: str
    session_id: Optional[str]
    content: Dict[str, Any]
    exported_to_chatwoot: bool = False
    exported_at: Optional[str] = None

    @classmethod
    def from_api(cls, item: Dict[str, Any]) -> "BotmakerMessage":
        chat_obj = item.get("chat", {})
        return cls(
            id=item.get("id", ""),
            creation_time=item.get("creationTime"),
            sender=item.get("from", ""),
            agent_id=item.get("agentId"),
            queue_id=item.get("queueId"),
            chat_id=chat_obj.get("chatId", ""),
            channel_id=chat_obj.get("channelId", ""),
            contact_id=chat_obj.get("contactId", ""),
            session_id=item.get("sessionId"),
            content=item.get("content", {}) or {},
            exported_to_chatwoot=item.get("exported_to_chatwoot", False),
            exported_at=item.get("exported_at"),
        )
