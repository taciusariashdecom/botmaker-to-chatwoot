from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Iterator, Optional

from .http import make_botmaker_client, HttpClient
from .models import BotmakerChat, BotmakerMessage

logger = logging.getLogger(__name__)


class BotmakerClient:
    def __init__(self, base_url: str, api_token: str, rps: float) -> None:
        if not api_token:
            raise ValueError("BOTMAKER_API_TOKEN is required")
        self.http: HttpClient = make_botmaker_client(base_url, api_token, rps)

    def close(self) -> None:
        self.http.close()

    def list_chats(
        self,
        *,
        from_iso: Optional[str] = None,
        to_iso: Optional[str] = None,
        limit: Optional[int] = None,
        channel_id: Optional[str] = None,
        queue_id: Optional[str] = None,
        has_agent: Optional[bool] = None,
        next_page: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if from_iso:
            params["from"] = from_iso
        if to_iso:
            params["to"] = to_iso
        if limit:
            params["limit"] = limit
        if channel_id:
            params["channel-id"] = channel_id
        if queue_id:
            params["queue-id"] = queue_id
        if has_agent is not None:
            params["has-agent"] = str(has_agent).lower()

        url = next_page or "/chats"
        resp = self.http.request("GET", url, params=params if next_page is None else None)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected response structure for /chats")
        return data

    def list_messages(
        self,
        *,
        from_iso: Optional[str] = None,
        to_iso: Optional[str] = None,
        limit: Optional[int] = None,
        channel_id: Optional[str] = None,
        contact_id: Optional[str] = None,
        chat_id: Optional[str] = None,
        long_term_search: bool = False,
        next_page: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if from_iso:
            params["from"] = from_iso
        if to_iso:
            params["to"] = to_iso
        if limit:
            params["limit"] = limit
        if channel_id:
            params["channel-id"] = channel_id
        if contact_id:
            params["contact-id"] = contact_id
        if chat_id:
            params["chat-id"] = chat_id
        if long_term_search:
            params["long-term-search"] = str(long_term_search).lower()

        url = next_page or "/messages"
        resp = self.http.request("GET", url, params=params if next_page is None else None)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected response structure for /messages")
        return data


def stream_chats(
    client: BotmakerClient,
    *,
    from_iso: Optional[str] = None,
    to_iso: Optional[str] = None,
    limit: Optional[int] = None,
    channel_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    has_agent: Optional[bool] = None,
) -> Iterator[BotmakerChat]:
    next_page: Optional[str] = None
    total = 0
    while True:
        page = client.list_chats(
            from_iso=from_iso,
            to_iso=to_iso,
            limit=limit,
            channel_id=channel_id,
            queue_id=queue_id,
            has_agent=has_agent,
            next_page=next_page,
        )
        items = page.get("items", [])
        for item in items:
            yield BotmakerChat.from_api(item)
            total += 1
            if limit and total >= limit:
                return
        next_page = page.get("nextPage")
        if not next_page:
            break
    logger.info("Fetched %d chats", total)


def stream_messages(
    client: BotmakerClient,
    *,
    from_iso: Optional[str] = None,
    to_iso: Optional[str] = None,
    limit: Optional[int] = None,
    channel_id: Optional[str] = None,
    contact_id: Optional[str] = None,
    chat_id: Optional[str] = None,
    long_term_search: bool = False,
) -> Iterator[BotmakerMessage]:
    next_page: Optional[str] = None
    total = 0
    while True:
        page = client.list_messages(
            from_iso=from_iso,
            to_iso=to_iso,
            limit=limit,
            channel_id=channel_id,
            contact_id=contact_id,
            chat_id=chat_id,
            long_term_search=long_term_search,
            next_page=next_page,
        )
        items = page.get("items", [])
        for item in items:
            yield BotmakerMessage.from_api(item)
            total += 1
            if limit and total >= limit:
                return
        next_page = page.get("nextPage")
        if not next_page:
            break
    logger.info("Fetched %d messages", total)
