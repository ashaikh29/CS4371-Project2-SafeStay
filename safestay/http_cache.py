"""Cached, throttled HTTP GET for every API the project calls.

Ground rules this implements:
  * every response is saved to disk together with the time it was received;
  * a saved response is reused instead of calling the service again;
  * at least one second passes between two calls to the same host.
Later tasks (hotels, Overpass, ...) can reuse get_json() as well.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

MIN_SECONDS_BETWEEN_CALLS = 1.0
USER_AGENT = "SafeStay-CS4371-student-project/1.0"

_last_call: dict[str, float] = {}


def cache_key(url: str, params: dict | None) -> str:
    """Short, stable hash of a request, used to name its cache file."""
    blob = json.dumps({"url": url, "params": params or {}}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _throttle(host: str) -> None:
    wait = MIN_SECONDS_BETWEEN_CALLS - (time.monotonic() - _last_call.get(host, 0.0))
    if wait > 0:
        time.sleep(wait)
    _last_call[host] = time.monotonic()


def get_json(url: str, cache_file: Path, params: dict | None = None,
             headers: dict | None = None, timeout: float = 180.0,
             retries: int = 4, refresh: bool = False) -> dict:
    """GET url and return {"retrieved_at", "url", "params", "from_cache", "data"}.

    The record (without "from_cache") is written to cache_file. If cache_file already
    exists and refresh is False, it is returned without any network call. Header values
    (such as API tokens) are never written to the cache.
    """
    cache_file = Path(cache_file)
    if cache_file.exists() and not refresh:
        record = json.loads(cache_file.read_text())
        record["from_cache"] = True
        return record

    host = urlparse(url).netloc
    all_headers = {"User-Agent": USER_AGENT, "Accept": "application/json", **(headers or {})}
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        _throttle(host)
        try:
            resp = requests.get(url, params=params, headers=all_headers, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            resp.raise_for_status()
            data = resp.json()
            break
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == retries:
                raise RuntimeError(f"GET {url} failed after {retries + 1} attempts: {exc}") from exc
            backoff = 5 * 2 ** attempt
            print(f"  request failed ({exc}); retrying in {backoff}s")
            time.sleep(backoff)
    else:  # pragma: no cover - loop always breaks or raises
        raise RuntimeError(str(last_error))

    record = {
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "url": url,
        "params": params or {},
        "data": data,
    }
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_file.with_suffix(cache_file.suffix + ".tmp")
    tmp.write_text(json.dumps(record))
    tmp.replace(cache_file)  # atomic: a half-written page is never mistaken for a cached one
    record["from_cache"] = False
    return record
