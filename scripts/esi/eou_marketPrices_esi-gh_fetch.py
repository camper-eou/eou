from __future__ import annotations

import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter


@dataclass(frozen=True)
class Entity:
    kind: str
    id: int
    name: str
    pages_est: int = 1


class RetryBudget:
    def __init__(self, initial: int):
        self._initial = int(initial)
        self._value = int(initial)
        self._lock = Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def used(self) -> int:
        with self._lock:
            return self._initial - self._value

    def consume(self, reason: str) -> None:
        with self._lock:
            self._value -= 1
            if self._value < 0:
                self._value = 0
            remaining = self._value
        if remaining == 0:
            raise RuntimeError(f"RETRY_BUDGET exhausted (reason: {reason})")


class TokenManager:
    def __init__(self, tokens: List[str]):
        self._tokens = [t for t in tokens if t]
        self._idx = 0
        self._lock = Lock()

    def current(self) -> str:
        with self._lock:
            if not self._tokens:
                raise RuntimeError("No ESI access tokens available for structure markets.")
            return self._tokens[self._idx]

    def rotate(self) -> None:
        with self._lock:
            self._idx += 1
            if self._idx >= len(self._tokens):
                raise RuntimeError("All ESI access tokens exhausted (cannot access structures).")


class StatsCollector:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters: Dict[str, float] = {
            "requests": 0,
            "http200": 0,
            "http401": 0,
            "http404": 0,
            "http420": 0,
            "http429": 0,
            "http5xx": 0,
            "backoff_seconds": 0.0,
            "ignored_structures": 0,
            "structure404_page1_retry": 0,
        }
        self._max_last_modified_ts: Optional[float] = None
        self._max_last_modified_iso: Optional[str] = None

    def incr(self, key: str, value: int = 1) -> None:
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def add_seconds(self, seconds: float) -> None:
        with self._lock:
            self._counters["backoff_seconds"] = self._counters.get("backoff_seconds", 0.0) + float(seconds)

    def observe_last_modified(self, header_value: Optional[str]) -> None:
        if not header_value:
            return
        try:
            dt = parsedate_to_datetime(header_value)
            if dt.tzinfo is None:
                return
            ts = dt.timestamp()
            iso = dt.astimezone().replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            return

        with self._lock:
            if self._max_last_modified_ts is None or ts > self._max_last_modified_ts:
                self._max_last_modified_ts = ts
                self._max_last_modified_iso = iso

    def snapshot(self) -> Dict[str, float | str | None]:
        with self._lock:
            out = dict(self._counters)
            out["max_last_modified"] = self._max_last_modified_iso
            return out


class EsiClient:
    def __init__(self, base: str, datasource: str, user_agent: str, timeout_connect_s: int = 10, timeout_read_s: int = 30):
        self.base = base.rstrip("/")
        self.datasource = datasource
        self.user_agent = user_agent
        self.timeout = (timeout_connect_s, timeout_read_s)
        self.session = requests.Session()

        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, url: str, headers: Dict[str, str]) -> requests.Response:
        hdrs = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": self.user_agent,
            **headers,
        }
        return self.session.get(url, headers=hdrs, timeout=self.timeout)

    def get_region_orders(self, region_id: int, page: int) -> requests.Response:
        url = f"{self.base}/markets/{region_id}/orders/"
        params = f"?datasource={self.datasource}&order_type=all&page={page}"
        return self._get(url + params, headers={})

    def get_structure_orders(self, structure_id: int, page: int, token: str) -> requests.Response:
        url = f"{self.base}/markets/structures/{structure_id}/"
        params = f"?datasource={self.datasource}&page={page}"
        return self._get(url + params, headers={"Authorization": f"Bearer {token}"})


def _sleep_from_headers(resp: requests.Response, default_s: int = 30) -> int:
    retry_after = resp.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(1, int(float(retry_after)))
        except Exception:
            pass

    err_rem = resp.headers.get("X-Esi-Error-Limit-Remain")
    err_reset = resp.headers.get("X-Esi-Error-Limit-Reset")
    try:
        if err_rem is not None and err_reset is not None and int(err_rem) <= 5:
            return max(1, int(float(err_reset)))
    except Exception:
        pass

    return default_s


def fetch_entity(
    entity: Entity,
    client: EsiClient,
    token_mgr: TokenManager,
    retry_budget: RetryBudget,
    push_orders_fn,
    stats: Optional[StatsCollector] = None,
    polite_delay_s: float = 0.30,
) -> Tuple[Optional[int], bool]:
    pages_observed: Optional[int] = None
    ignored = False

    page = 1
    last_ok_page = 0
    retried_404_page1 = False

    while True:
        if stats:
            stats.incr("requests", 1)

        if entity.kind == "region":
            resp = client.get_region_orders(entity.id, page)
        else:
            tok = token_mgr.current()
            resp = client.get_structure_orders(entity.id, page, tok)

        if stats:
            stats.observe_last_modified(resp.headers.get("Last-Modified"))

        status = resp.status_code

        if status == 200:
            if stats:
                stats.incr("http200", 1)

            xp = resp.headers.get("X-Pages")
            if xp:
                try:
                    pages_observed = int(xp)
                except Exception:
                    pass

            data = resp.json()
            out = []
            for o in data:
                try:
                    out.append(
                        {
                            "order_id": int(o["order_id"]),
                            "issued": str(o["issued"]),
                            "location_id": int(o["location_id"]),
                            "type_id": int(o["type_id"]),
                            "is_buy_order": bool(o["is_buy_order"]),
                            "price": float(o["price"]),
                            "volume_remain": int(o["volume_remain"]),
                        }
                    )
                except Exception:
                    continue

            if out:
                push_orders_fn(out)

            last_ok_page = page

            if pages_observed is not None and page >= pages_observed:
                break

            page += 1
            if polite_delay_s:
                time.sleep(polite_delay_s)
            continue

        if status == 404:
            if stats:
                stats.incr("http404", 1)

            if entity.kind == "region":
                break

            if page == 1 and not retried_404_page1:
                retried_404_page1 = True
                if stats:
                    stats.incr("structure404_page1_retry", 1)
                    stats.add_seconds(5.0)
                time.sleep(5)
                retry_budget.consume("structure_404_page1_retry")
                continue

            ignored = True
            if stats:
                stats.incr("ignored_structures", 1)
            break

        if status == 401 and entity.kind == "structure":
            if stats:
                stats.incr("http401", 1)
                stats.add_seconds(5.0)
            token_mgr.rotate()
            time.sleep(5)
            retry_budget.consume("401_rotate_token")
            continue

        if status in (420, 429) or 500 <= status <= 599:
            if stats:
                if status == 420:
                    stats.incr("http420", 1)
                elif status == 429:
                    stats.incr("http429", 1)
                else:
                    stats.incr("http5xx", 1)

            wait_s = _sleep_from_headers(resp, default_s=30)
            if stats:
                stats.add_seconds(wait_s)
            time.sleep(wait_s)
            retry_budget.consume(f"{status}_retry")
            continue

        if 400 <= status <= 499:
            if entity.kind == "structure":
                ignored = True
                if stats:
                    stats.incr("ignored_structures", 1)
                break
            raise RuntimeError(
                f"Unexpected region error {status} for region {entity.id} page {page}: {resp.text[:200]}"
            )

        wait_s = _sleep_from_headers(resp, default_s=30)
        if stats:
            stats.add_seconds(wait_s)
        time.sleep(wait_s)
        retry_budget.consume(f"unexpected_{status}")

    if pages_observed is None:
        pages_observed = last_ok_page if last_ok_page > 0 else None

    return pages_observed, ignored
