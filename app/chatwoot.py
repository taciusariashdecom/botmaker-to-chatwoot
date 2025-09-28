from __future__ import annotations

import logging
from typing import Any, Dict

from .http import HttpClient, make_chatwoot_client

logger = logging.getLogger(__name__)


class ChatwootClient:
    def __init__(self, base_url: str, api_access_token: str, rps: float) -> None:
        if not api_access_token:
            raise ValueError("CHATWOOT_API_ACCESS_TOKEN is required")
        self.http: HttpClient = make_chatwoot_client(base_url, api_access_token, rps)

    def close(self) -> None:
        self.http.close()

    def create_contact(self, account_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/contacts"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def create_conversation(self, account_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/conversations"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def create_message(self, account_id: str, conversation_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()
