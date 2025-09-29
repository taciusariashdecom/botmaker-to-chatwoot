from __future__ import annotations

import logging
from typing import Any, Dict, List

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

    def update_contact(self, account_id: str, contact_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/contacts/{contact_id}"
        resp = self.http.request("PUT", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def list_contacts(self, account_id: str, **params: Any) -> Dict[str, Any]:
        """Raw list contacts with optional filters (best-effort; Chatwoot may ignore unknown params)."""
        url = f"/api/v1/accounts/{account_id}/contacts"
        resp = self.http.request("GET", url, params=params or None)
        resp.raise_for_status()
        return resp.json()

    def create_conversation(self, account_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/conversations"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def update_conversation(self, account_id: str, conversation_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/conversations/{conversation_id}"
        resp = self.http.request("PATCH", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def add_conversation_labels(self, account_id: str, conversation_id: int, labels: List[str]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/conversations/{conversation_id}/labels"
        resp = self.http.request("POST", url, json={"labels": labels})
        resp.raise_for_status()
        return resp.json()

    def create_message(self, account_id: str, conversation_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def create_conversation_note(self, account_id: str, conversation_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/conversations/{conversation_id}/notes"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def list_inboxes(self, account_id: str) -> Dict[str, Any]:
        url = f"/api/v1/accounts/{account_id}/inboxes"
        resp = self.http.request("GET", url)
        resp.raise_for_status()
        return resp.json()

    def search_contacts(self, account_id: str, query: str) -> Dict[str, Any]:
        """Search contacts by a free-text query (identifier, email, phone, name).
        Chatwoot supports a search endpoint under contacts.
        """
        url = f"/api/v1/accounts/{account_id}/contacts/search"
        params = {"q": query}
        resp = self.http.request("GET", url, params=params)
        resp.raise_for_status()
        return resp.json()

    def create_contact_inbox(self, account_id: str, contact_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Associate a contact with an inbox and a source_id (API channel)"""
        url = f"/api/v1/accounts/{account_id}/contacts/{contact_id}/contact_inboxes"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def create_contact_note(self, account_id: str, contact_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create a note on a contact (content in payload['content'])."""
        url = f"/api/v1/accounts/{account_id}/contacts/{contact_id}/notes"
        resp = self.http.request("POST", url, json=payload)
        resp.raise_for_status()
        return resp.json()
