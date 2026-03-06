from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, List, Optional, Tuple

import requests


@dataclass(frozen=True)
class Entity:
    kind: str  # "region" | "structure"
    id: int
    name: str
    pages_est: int = 1


class RetryBudget:
    def __init__(self, initial: int):
        self._value = int(initial)
        self._initial = int(initial)
        self._lock = Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    @property
    def initial(self) -> int:
        return self._initial

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


@dataclass
class FetchStats:
    requests: int = 0
    http401: int = 0
    http404: int = 0
    http420: int = 0
    http429: int = 0
    http5xx: int = 0
    backoff_seconds: float = 0.0
    _lock: Lock = field(default_factory=Lock, repr=False)

    def note_status(self, status: int) -> None:
        with self._lock:
            self.requests += 1
            if status == 401:
                self.http401 += 1
            elif status == 404:
                self.http404 += 1
            elif status == 420:
                self.http420 += 1
            elif status == 429:
                self.http429 += 1
            elif 500 <= status <= 599:
                self.http5xx += 1

    def add_backoff(self, seconds: float) -> None:
        with self._lock:
            self.backoff_seconds += float(seconds)

    def snapshot(self) -> Dict[str, float | int]:
        with self._lock:
            return {
                "requests": self.requests,
                "http401": self.http401,
                "http404": self.http404,
                "http420": self.http420,
                "http429": self.http429,
                "http5xx": self.http5xx,
                "backoffSeconds": round(self.backoff_seconds, 3),
            }


class EsiClient:
    def __init__(self, base: str, datasource: str, user_agent: str, timeout_s: int = 30):
        self.base = base.rstrip("/")
        self.datasource = datasource
        self.user_agent = user_agent
        self.timeout_s = timeout_s
        self.session = requests.Session()

    def _get(self, url: str, headers: Dict[str, str]) -> requests.Response:
        hdrs = {"Accept": "application/json", "User-Agent": self.user_agent, **headers}
        return self.session.get(url, headers=hdrs, timeout=self.timeout_s)

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
    stats: FetchStats,
    polite_delay_s: float = 0.30,
) -> Tuple[Optional[int], bool]:
    """
    Fetch all pages for one entity, pushing minimal orders via push_orders_fn.
    Returns: (pages_observed, ignored_flag)
    """
    pages_observed: Optional[int] = None
    ignored = False

    page = 1
    last_ok_page = 0
    retried_404_page1 = False

    while True:
        if entity.kind == "region":
            resp = client.get_region_orders(entity.id, page)
        else:
            tok = token_mgr.current()
            resp = client.get_structure_orders(entity.id, page, tok)

        status = resp.status_code
        stats.note_status(status)

        if status == 200:
            if page == 1:
                xp = resp.headers.get("X-Pages")
                if xp:
                    try:
                        pages_observed = int(xp)
                    except Exception:
                        pages_observed = None

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
            if entity.kind == "region":
                break

            if page == 1 and not retried_404_page1:
                retried_404_page1 = True
                sleep_s = 5
                stats.add_backoff(sleep_s)
                time.sleep(sleep_s)
                retry_budget.consume("structure_404_page1_retry")
                continue

            ignored = True
            break

        if status == 401 and entity.kind == "structure":
            token_mgr.rotate()
            sleep_s = 5
            stats.add_backoff(sleep_s)
            time.sleep(sleep_s)
            retry_budget.consume("401_rotate_token")
            continue

        if status in (420, 429) or 500 <= status <= 599:
            wait_s = _sleep_from_headers(resp, default_s=30)
            stats.add_backoff(wait_s)
            time.sleep(wait_s)
            retry_budget.consume(f"{status}_retry")
            continue

        if 400 <= status <= 499:
            if entity.kind == "structure":
                ignored = True
                break
            raise RuntimeError(
                f"Unexpected region error {status} for region {entity.id} page {page}: {resp.text[:200]}"
            )

        wait_s = _sleep_from_headers(resp, default_s=30)
        stats.add_backoff(wait_s)
        time.sleep(wait_s)
        retry_budget.consume(f"unexpected_{status}")

    if pages_observed is None:
        pages_observed = last_ok_page if last_ok_page > 0 else None

    return pages_observed, ignored
