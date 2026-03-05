#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import json
import logging
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Make sibling modules importable when executed as a script:
sys.path.insert(0, os.path.dirname(__file__))

from esi_fetch import Entity, EsiClient, RetryBudget, TokenManager, fetch_entity
from metrics import compute_buy, compute_sell
from sde_loader import load_regions, load_stations_map, load_structures_map
from sheets_tokens import read_tokens_from_sheet
from sqlite_orders import OrdersWriter, connect
from types_loader import detect_types_file, load_types

LOG = logging.getLogger("market_prices_pipeline")


@dataclass
class PagesCache:
    stations: Dict[int, int]     # regionID -> pages
    structures: Dict[int, int]   # structureID -> pages


def load_pages_cache(path: Path) -> Optional[PagesCache]:
    if not path.exists():
        return None
    obj = json.loads(path.read_text(encoding="utf-8"))
    stations = {
        int(x["regionID"]): int(x["pages"])
        for x in obj.get("stations", [])
        if "regionID" in x and "pages" in x
    }
    structures = {
        int(x["stationID"]): int(x["pages"])
        for x in obj.get("structures", [])
        if "stationID" in x and "pages" in x
    }
    return PagesCache(stations=stations, structures=structures)


def write_pages_cache(
    path: Path,
    regions: List[Tuple[int, str, int]],
    structures: List[Tuple[int, str, int]],
) -> None:
    out = {
        "stations": [{"regionID": rid, "region": rname, "pages": pages} for rid, rname, pages in regions],
        "structures": [{"stationID": sid, "station": sname, "pages": pages} for sid, sname, pages in structures],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def greedy_balance(entities: List[Entity], workers: int) -> List[List[Entity]]:
    bins: List[List[Entity]] = [[] for _ in range(workers)]
    loads = [0] * workers
    entities_sorted = sorted(entities, key=lambda e: int(e.pages_est or 1), reverse=True)
    for e in entities_sorted:
        i = min(range(workers), key=lambda k: loads[k])
        bins[i].append(e)
        loads[i] += int(e.pages_est or 1)
    return bins


def compute_hubs(conn, location_names: Dict[int, str]) -> List[Tuple[int, str]]:
    total_orders = conn.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"]
    if total_orders <= 0:
        return []

    threshold = total_orders * 0.015

    rows = conn.execute(
        """
        SELECT
          location_id,
          COUNT(*) AS order_cnt,
          COUNT(DISTINCT type_id) AS type_cnt,
          SUM(price * volume_remain) AS value_sum
        FROM orders
        GROUP BY location_id
        HAVING order_cnt > ?
        """,
        (threshold,),
    ).fetchall()

    hubs = []
    for r in rows:
        loc = int(r["location_id"])
        hubs.append(
            (
                int(r["order_cnt"]),
                int(r["type_cnt"]),
                float(r["value_sum"] or 0.0),
                loc,
                location_names.get(loc, str(loc)),
            )
        )

    hubs.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]))
    return [(loc, name) for _, _, _, loc, name in hubs]


class TypeRowStream:
    def __init__(self, cursor):
        self.cur = cursor
        self.buf = None

    def _next(self):
        if self.buf is not None:
            row = self.buf
            self.buf = None
            return row
        return self.cur.fetchone()

    def take_for_type(self, type_id: int) -> List[Tuple]:
        rows = []
        while True:
            row = self._next()
            if row is None:
                break
            tid = int(row[0])
            if tid == type_id:
                rows.append(row)
                continue
            self.buf = row
            break
        return rows


def build_segment_entry(hub_id, hub_name: str, buy_entries, sell_entries) -> Dict:
    buy = compute_buy(buy_entries)
    sell = compute_sell(sell_entries)
    return {
        "hubID": hub_id,
        "hub": hub_name,
        "sellPrice": sell["price"],
        "sellUnits": sell["units"],
        "sellTotalUnits": sell["totalUnits"],
        "sellFMI": sell["ifm"],
        "sellFMIn": sell["ifmName"],
        "buyPrice": buy["price"],
        "buyUnits": buy["units"],
        "buyTotalUnits": buy["totalUnits"],
        "buyFMI": buy["ifm"],
        "buyFMIn": buy["ifmName"],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landu-root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--pages-cache", required=True)
    ap.add_argument("--sheets-id", required=True)
    ap.add_argument("--sheets-range", required=True)
    ap.add_argument("--esi-base", required=True)
    ap.add_argument("--datasource", required=True)
    ap.add_argument("--user-agent", required=True)
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--retry-budget", type=int, default=10)
    ap.add_argument("--force-refresh", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    landu_root = Path(args.landu_root)
    out_path = Path(args.out)
    pages_cache_path = Path(args.pages_cache)

    LOG.info("Loading inputs from landu repo at %s", landu_root)

    types_path = detect_types_file(landu_root)
    types = load_types(types_path)
    regions = load_regions(landu_root)

    stations_map = load_stations_map(landu_root)
    structures_map = load_structures_map(landu_root)
    location_names = {**stations_map, **structures_map}

    LOG.info("Reading structure access tokens from Google Sheets (range %s)", args.sheets_range)
    tokens = read_tokens_from_sheet(args.sheets_id, args.sheets_range)
    if not tokens:
        raise SystemExit("No tokens returned from Google Sheets.")
    token_mgr = TokenManager(tokens)

    cache = None if args.force_refresh else load_pages_cache(pages_cache_path)
    LOG.info("Pages cache: %s", "enabled" if cache else "disabled")

    entities: List[Entity] = []
    for r in regions:
        est = cache.stations.get(r.region_id, 1) if cache else 1
        entities.append(Entity(kind="region", id=r.region_id, name=r.region_name, pages_est=est))

    for sid, sname in structures_map.items():
        est = cache.structures.get(sid, 1) if cache else 1
        entities.append(Entity(kind="structure", id=sid, name=sname, pages_est=est))

    workers = max(1, int(args.max_workers))
    bins = greedy_balance(entities, workers)
    LOG.info("Planned %d entities across %d workers", len(entities), workers)

    retry_budget = RetryBudget(args.retry_budget)
    client = EsiClient(base=args.esi_base, datasource=args.datasource, user_agent=args.user_agent)

    tmpdir = Path(tempfile.mkdtemp(prefix="eou_market_prices_"))
    db_path = tmpdir / "orders.sqlite"

    writer = OrdersWriter(db_path)
    writer.start()

    observed_regions: Dict[int, int] = {}
    observed_structs: Dict[int, int] = {}
    ignored_structs: set[int] = set()

    def push_orders(batch):
        writer.push(batch)

    def worker_fn(my_entities: List[Entity]) -> None:
        for ent in my_entities:
            pages, ignored = fetch_entity(
                ent,
                client=client,
                token_mgr=token_mgr,
                retry_budget=retry_budget,
                push_orders_fn=push_orders,
                polite_delay_s=0.30,
            )
            pages = int(pages or 1)
            if ent.kind == "region":
                observed_regions[ent.id] = pages
            else:
                if ignored:
                    ignored_structs.add(ent.id)
                else:
                    observed_structs[ent.id] = pages

    LOG.info("Starting ingestion (conservative retries; obeying Retry-After)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(worker_fn, b) for b in bins if b]
        for f in concurrent.futures.as_completed(futs):
            f.result()

    writer.stop()
    LOG.info("Ingestion complete; DB at %s", db_path)

    conn = connect(db_path)

    hubs = compute_hubs(conn, location_names)
    LOG.info("Hubs passing 1.5%% threshold: %d", len(hubs))

    conn.execute("DROP TABLE IF EXISTS hubs;")
    conn.execute("CREATE TEMP TABLE hubs (location_id INTEGER PRIMARY KEY, rank INTEGER NOT NULL, name TEXT NOT NULL);")
    for rank, (loc, name) in enumerate(hubs, start=1):
        conn.execute("INSERT INTO hubs(location_id, rank, name) VALUES (?,?,?)", (int(loc), int(rank), str(name)))
    conn.commit()

    conn.execute("DROP TABLE IF EXISTS universe_buy;")
    conn.execute(
        """CREATE TEMP TABLE universe_buy AS
           SELECT type_id, price, SUM(volume_remain) AS vol
           FROM orders WHERE is_buy=1
           GROUP BY type_id, price
           ORDER BY type_id ASC, price DESC;"""
    )
    conn.execute("DROP TABLE IF EXISTS universe_sell;")
    conn.execute(
        """CREATE TEMP TABLE universe_sell AS
           SELECT type_id, price, SUM(volume_remain) AS vol
           FROM orders WHERE is_buy=0
           GROUP BY type_id, price
           ORDER BY type_id ASC, price ASC;"""
    )

    conn.execute("DROP TABLE IF EXISTS hubs_buy;")
    conn.execute(
        """CREATE TEMP TABLE hubs_buy AS
           SELECT o.type_id, o.location_id, o.price, SUM(o.volume_remain) AS vol
           FROM orders o
           JOIN hubs h ON h.location_id = o.location_id
           WHERE o.is_buy=1
           GROUP BY o.type_id, o.location_id, o.price
           ORDER BY o.type_id ASC, o.location_id ASC, o.price DESC;"""
    )
    conn.execute("DROP TABLE IF EXISTS hubs_sell;")
    conn.execute(
        """CREATE TEMP TABLE hubs_sell AS
           SELECT o.type_id, o.location_id, o.price, SUM(o.volume_remain) AS vol
           FROM orders o
           JOIN hubs h ON h.location_id = o.location_id
           WHERE o.is_buy=0
           GROUP BY o.type_id, o.location_id, o.price
           ORDER BY o.type_id ASC, o.location_id ASC, o.price ASC;"""
    )
    conn.commit()

    ub_stream = TypeRowStream(conn.execute("SELECT type_id, price, vol FROM universe_buy ORDER BY type_id, price DESC;"))
    us_stream = TypeRowStream(conn.execute("SELECT type_id, price, vol FROM universe_sell ORDER BY type_id, price ASC;"))
    hb_stream = TypeRowStream(conn.execute("SELECT type_id, location_id, price, vol FROM hubs_buy ORDER BY type_id, location_id, price DESC;"))
    hs_stream = TypeRowStream(conn.execute("SELECT type_id, location_id, price, vol FROM hubs_sell ORDER BY type_id, location_id, price ASC;"))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    LOG.info("Writing NDJSON.gz to %s (one line per type)", out_path)

    with gzip.open(out_path, "wt", encoding="utf-8") as gz:
        for t in types:
            tid = int(t.type_id)

            ub_rows = ub_stream.take_for_type(tid)
            us_rows = us_stream.take_for_type(tid)
            buy_univ = [(float(r[1]), int(r[2])) for r in ub_rows]
            sell_univ = [(float(r[1]), int(r[2])) for r in us_rows]

            hb_rows = hb_stream.take_for_type(tid)
            hs_rows = hs_stream.take_for_type(tid)

            buy_by_loc: Dict[int, List[Tuple[float, int]]] = {}
            sell_by_loc: Dict[int, List[Tuple[float, int]]] = {}

            for r in hb_rows:
                loc = int(r[1])
                buy_by_loc.setdefault(loc, []).append((float(r[2]), int(r[3])))

            for r in hs_rows:
                loc = int(r[1])
                sell_by_loc.setdefault(loc, []).append((float(r[2]), int(r[3])))

            buy_hubs_all: List[Tuple[float, int]] = []
            sell_hubs_all: List[Tuple[float, int]] = []
            for lst in buy_by_loc.values():
                buy_hubs_all.extend(lst)
            for lst in sell_by_loc.values():
                sell_hubs_all.extend(lst)

            prices = [
                build_segment_entry(0, "universe", buy_univ, sell_univ),
                build_segment_entry(None, "hubs", buy_hubs_all, sell_hubs_all),
            ]

            for loc, name in hubs:
                prices.append(
                    build_segment_entry(int(loc), str(name), buy_by_loc.get(int(loc), []), sell_by_loc.get(int(loc), []))
                )

            gz.write(json.dumps({"typeID": tid, "type": t.type_name, "prices": prices}, ensure_ascii=False, separators=(",", ":")) + "\n")

    regions_cache_rows = [(r.region_id, r.region_name, int(observed_regions.get(r.region_id, 1))) for r in regions]
    structs_cache_rows = []
    for sid, sname in structures_map.items():
        if sid in ignored_structs:
            continue
        structs_cache_rows.append((sid, sname, int(observed_structs.get(sid, 1))))

    write_pages_cache(pages_cache_path, regions_cache_rows, structs_cache_rows)
    LOG.info("Wrote pages cache to %s", pages_cache_path)

    conn.close()
    LOG.info("Done.")


if __name__ == "__main__":
    main()
