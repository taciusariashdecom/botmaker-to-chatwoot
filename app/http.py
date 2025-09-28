import time
import logging
from typing import Optional, Dict, Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self._last = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self._last
        sleep_for = self.min_interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last = time.time()


class HttpClient:
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, rps: float = 4.0, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.limiter = RateLimiter(rps)
        self.client = httpx.Client(base_url=self.base_url, headers=self.headers, timeout=timeout)

    def close(self):
        self.client.close()

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        self.limiter.wait()
        try:
            resp = self.client.request(method, url, **kwargs)
            if resp.status_code in (429, 503, 502, 500):
                # surface as error to trigger retry
                raise httpx.HTTPStatusError("server backoff", request=resp.request, response=resp)
            return resp
        except httpx.HTTPError as e:
            logger.warning("HTTP error on %s %s: %s", method, url, e)
            raise


def make_botmaker_client(base_url: str, token: str, rps: float) -> HttpClient:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    return HttpClient(base_url, headers=headers, rps=rps)


def make_chatwoot_client(base_url: str, api_access_token: str, rps: float) -> HttpClient:
    headers = {
        "api_access_token": api_access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    return HttpClient(base_url, headers=headers, rps=rps)
