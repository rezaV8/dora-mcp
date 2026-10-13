"""Minimal GitHub REST + GraphQL client with pagination and rate-limit handling."""

import os
import subprocess
import time
from collections.abc import Iterator
from typing import Any

import httpx

API = "https://api.github.com"


def resolve_token() -> str | None:
    """GITHUB_TOKEN env var first, then the GitHub CLI's stored login."""
    if token := os.environ.get("GITHUB_TOKEN"):
        return token
    try:
        out = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=True)
        return out.stdout.strip() or None
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None


class GitHub:
    def __init__(self, token: str | None = None):
        token = token or resolve_token()
        if not token:
            raise RuntimeError("No GitHub token. Set GITHUB_TOKEN or run `gh auth login`.")
        self.client = httpx.Client(
            base_url=API,
            timeout=60,
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "dora-mcp",
                "Authorization": f"Bearer {token}",
            },
        )

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        for attempt in range(6):
            resp = self.client.request(method, url, **kwargs)
            if resp.status_code in (403, 429) and (
                resp.headers.get("x-ratelimit-remaining") == "0" or "retry-after" in resp.headers
            ):
                reset = float(resp.headers.get("x-ratelimit-reset", time.time() + 60))
                wait = float(resp.headers.get("retry-after", max(reset - time.time(), 1)))
                if wait > 900:
                    resp.raise_for_status()
                print(f"  rate limited, sleeping {wait:.0f}s")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                time.sleep(2**attempt)
                continue
            resp.raise_for_status()
            return resp
        resp.raise_for_status()
        return resp

    def get(self, path: str, **params: Any) -> Any:
        return self._request("GET", path, params=params).json()

    def paginate(self, path: str, params: dict | None = None, items_key: str | None = None) -> Iterator[dict]:
        """Yield items across REST pages by following Link: rel="next"."""
        url: str | None = path
        query: dict | None = {**(params or {}), "per_page": 100}
        while url:
            resp = self._request("GET", url, params=query)
            query = None  # the next link already carries the query string
            body = resp.json()
            yield from (body[items_key] if items_key else body)
            url = resp.links.get("next", {}).get("url")

    def graphql(self, query: str, **variables: Any) -> dict:
        body = self._request("POST", "/graphql", json={"query": query, "variables": variables}).json()
        if body.get("errors"):
            raise RuntimeError(f"GraphQL error: {body['errors']}")
        return body["data"]
