#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import importlib.util
import json
import logging
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
LOG = logging.getLogger("eou_marketPrices_esi-gh_pipeline")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_module(module_tag: str):
    filename = BASE_DIR / f"eou_marketPrices_esi-gh_{module_tag}.py"
    mod_name = f"eou_marketPrices_esi_gh_{module_tag}"
    spec = importlib.util.spec_from_file_location(mod_name, filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {filename}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


fetch_mod = _load_module("fetch")
metrics_mod = _load_module("metrics")
sde_mod = _load_module("sde")
sheets_mod = _load_module("sheets")
sqlite_mod = _load_module("sqlite")
types_mod = _load_module("types")

Entity = fetch_mod.Entity
EsiClient = fetch_mod.EsiClient
RetryBudget = fetch_mod.RetryBudget
TokenManager = fetch_mod.TokenManager
StatsCollector = fetch_mod.StatsCollector
fetch_entity = fetch_mod.fetch_entity

compute_buy = metrics_mod.compute_buy
compute_sell = metrics_mod.compute_sell
analyze_buy = metrics_mod.analyze_buy
analyze_sell = metrics_mod.analyze_sell

load_regions = sde_mod.load_regions
load_stations_map = sde_mod.load_stations_map
load_structures_map = sde_mod.load_structures_map

read_tokens_from_sheet = sheets_mod.read_tokens_from_sheet

OrdersWriter = sqlite_mod.OrdersWriter
connect = sqlite_mod.connect

detect_types_file = types_mod.detect_types_file
load_types = types_mod.load_types


def format_issued(issued: str) -> str:
    dt = datetime.fromisoformat(issued.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y/%m/%d %H:%M:%S")


def load_test_type_id(path: Path) -> int:
    obj = json.loads(path.read_text(encoding="utf-8"))
    return int(obj["type_id"])


def load_pages_cache(path: Path) -> Optional[Dict[str, Dict[int, int]]]:
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
    return {"stations": stations, "structures": structures}


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


def compute_hubs(conn, location_names: Dict[int, str]) -> Tuple[int, List[Dict]]:
    total_orders = int(conn.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"])
    if total_orders <= 0:
        return 0, []

    threshold = total_orders * 0.0175

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
            {
                "stationID": loc,
                "station": location_names.get(loc, str(loc)),
                "orders": int(r["order_cnt"]),
                "types": int(r["type_cnt"]),
                "_valueSum": float(r["value_sum"] or 0.0),
            }
        )

    hubs.sort(key=lambda x: (-x["orders"], -x["types"], -x["_valueSum"], x["stationID"]))

    for hub in hubs:
        hub["ordersShare"] = (hub["orders"] / total_orders) if total_orders else 0.0
        del hub["_valueSum"]

    return total_orders, hubs


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


def write_hubs_json(path: Path, total_orders: int, hubs: List[Dict]) -> None:
    out = {
        "hubs": len(hubs),
        "orders": total_orders,
        "hubsList": [
            {
                "stationID": int(h["stationID"]),
                "station": str(h["station"]),
                "orders": int(h["orders"]),
                "ordersShare": float(h["ordersShare"]),
                "types": int(h["types"]),
            }
            for h in hubs
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def load_tuning_state(path: Path, base_max_workers: int, base_retry_budget: int) -> Dict:
    if not path.exists():
        return {
            "version": 1,
            "status": "idle",
            "next_run": None,
            "failed": 0,
            "current": {"max_workers": base_max_workers, "retry_budget": base_retry_budget},
            "best": {"max_workers": base_max_workers, "retry_budget": base_retry_budget, "score": None, "ts": None},
            "history": [],
        }

    obj = json.loads(path.read_text(encoding="utf-8"))
    obj.setdefault("version", 1)
    obj.setdefault("status", "idle")
    obj.setdefault("next_run", None)
    obj.setdefault("failed", 0)
    obj.setdefault("current", {"max_workers": base_max_workers, "retry_budget": base_retry_budget})
    obj.setdefault("best", {"max_workers": base_max_workers, "retry_budget": base_retry_budget, "score": None, "ts": None})
    obj.setdefault("history", [])
    return obj


def write_tuning_state(path: Path, state: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))


def compute_score(ok: bool, ingest_seconds: float, http401: int, http429: int, backoff_seconds: float) -> float:
    score = float(ingest_seconds)
    score += 15.0 * float(http429)
    score += 5.0 * float(http401)
    score += 0.5 * float(backoff_seconds)
    if not ok:
        score += 300.0
    return score


def choose_tuned_parameters(state: Dict, base_workers: int, base_retry_budget: int) -> Tuple[int, int]:
    min_workers, max_workers = 4, 16
    min_retry, max_retry = 10, 50

    history = state.get("history", [])
    best = state.get("best", {})
    current = state.get("current", {})

    if not history:
        return (
            clamp(int(current.get("max_workers", base_workers)), min_workers, max_workers),
            clamp(int(current.get("retry_budget", base_retry_budget)), min_retry, max_retry),
        )

    best_score = best.get("score")
    best_workers = int(best.get("max_workers", current.get("max_workers", base_workers)))
    best_retry = int(best.get("retry_budget", current.get("retry_budget", base_retry_budget)))

    last = history[-1]
    last_selected = last.get("selected", {})
    last_result = last.get("result", {})

    last_ok = bool(last_result.get("ok", False))
    last_429 = int(last_result.get("http429", 0))
    last_backoff = float(last_result.get("backoffSeconds", 0.0))
    last_retries = int(last_result.get("retriesUsed", 0))
    last_budget = int(last_selected.get("retry_budget", best_retry))
    last_workers = int(last_selected.get("max_workers", best_workers))
    last_score = float(last_result.get("score", 1e18))

    recent_pairs = {
        (
            int(h.get("selected", {}).get("max_workers", best_workers)),
            int(h.get("selected", {}).get("retry_budget", best_retry)),
        )
        for h in history[-5:]
    }

    if not last_ok:
        return (clamp(best_workers, min_workers, max_workers), clamp(min(best_retry + 5, max_retry), min_retry, max_retry))

    if last_429 > 0 or last_backoff > 120 or (last_budget > 0 and last_retries >= int(last_budget * 0.8)):
        return (clamp(best_workers - 2, min_workers, max_workers), clamp(best_retry + 5, min_retry, max_retry))

    if best_score is not None and last_score > float(best_score):
        return (clamp(best_workers, min_workers, max_workers), clamp(best_retry, min_retry, max_retry))

    if best_score is not None and last_workers == best_workers and int(last_selected.get("retry_budget", best_retry)) == best_retry:
        candidates = [
            (best_workers + 2, best_retry),
            (best_workers - 2, best_retry),
            (best_workers, best_retry + 5),
            (best_workers, best_retry - 5),
        ]
        for w, r in candidates:
            w = clamp(w, min_workers, max_workers)
            r = clamp(r, min_retry, max_retry)
            if (w, r) not in recent_pairs:
                return (w, r)

    return (clamp(best_workers, min_workers, max_workers), clamp(best_retry, min_retry, max_retry))


def write_run_metrics(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def fetch_raw_segment_rows(conn: sqlite3.Connection, type_id: int, location_ids: Optional[List[int]]) -> List[Dict]:
    where = "type_id = ?"
    params: List = [int(type_id)]
    if location_ids is not None:
        if not location_ids:
            return []
        placeholders = ",".join("?" for _ in location_ids)
        where += f" AND location_id IN ({placeholders})"
        params.extend(int(x) for x in location_ids)

    sql = f"""
    SELECT type_id, is_buy, volume_remain, price, issued, order_id
    FROM orders
    WHERE {where}
    ORDER BY
      is_buy ASC,
      CASE WHEN is_buy = 0 THEN price END ASC,
      CASE WHEN is_buy = 1 THEN price END DESC,
      issued ASC,
      order_id DESC
    """
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def aggregate_entries_for_flags(rows: List[Dict]) -> Tuple[List[Tuple[float, int]], List[Tuple[float, int]]]:
    sell_map: Dict[float, int] = {}
    buy_map: Dict[float, int] = {}
    for r in rows:
        price = float(r["price"])
        vol = int(r["volume_remain"])
        if int(r["is_buy"]) == 1:
            buy_map[price] = buy_map.get(price, 0) + vol
        else:
            sell_map[price] = sell_map.get(price, 0) + vol

    sell_entries = sorted(sell_map.items(), key=lambda x: x[0])
    buy_entries = sorted(buy_map.items(), key=lambda x: x[0], reverse=True)
    return buy_entries, sell_entries


def summarize_market_line(type_id: int, type_name: str, hub_id, hub_name: str, buy_entries, sell_entries) -> Dict:
    entry = build_segment_entry(hub_id, hub_name, buy_entries, sell_entries)
    out = {
        "type_id": int(type_id),
        "type": str(type_name),
        "sellPrice": entry["sellPrice"],
        "sellUnits": entry["sellUnits"],
        "sellTotalUnits": entry["sellTotalUnits"],
        "sellFMI": entry["sellFMI"],
        "sellFMIn": entry["sellFMIn"],
        "buyPrice": entry["buyPrice"],
        "buyUnits": entry["buyUnits"],
        "buyTotalUnits": entry["buyTotalUnits"],
        "buyFMI": entry["buyFMI"],
        "buyFMIn": entry["buyFMIn"],
    }
    if hub_id is not None:
        out["hub_id"] = int(hub_id)
        out["hub"] = str(hub_name)
    return out


def write_test_file(path: Path, rows: List[Dict], summary: Dict, buy_flag_info: Dict, sell_flag_info: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            price = float(r["price"])
            is_buy = bool(int(r["is_buy"]))

            flag = None
            if is_buy:
                if price in buy_flag_info["highliner_prices"]:
                    flag = "highliner"
                elif price in buy_flag_info["lowliner_prices"]:
                    flag = "lowliner"
            else:
                if price in sell_flag_info["highliner_prices"]:
                    flag = "highliner"
                elif price in sell_flag_info["lowliner_prices"]:
                    flag = "lowliner"

            obj = {
                "type_id": int(r["type_id"]),
                "is_buy_order": is_buy,
                "volume_remain": int(r["volume_remain"]),
                "price": float(r["price"]),
                "issued": format_issued(str(r["issued"])),
                "order_id": int(r["order_id"]),
                "flag": flag,
            }
            f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")

        f.write(json.dumps({"_summary": summary}, ensure_ascii=False) + "\n")


def generate_test_outputs(
    conn: sqlite3.Connection,
    test_dir: Path,
    test_type_id: int,
    type_name: str,
    hubs: List[Dict],
) -> None:
    universe_rows = fetch_raw_segment_rows(conn, test_type_id, None)
    universe_buy_entries, universe_sell_entries = aggregate_entries_for_flags(universe_rows)
    universe_buy_info = analyze_buy(universe_buy_entries)
    universe_sell_info = analyze_sell(universe_sell_entries)
    universe_summary = summarize_market_line(
        test_type_id,
        type_name,
        None,
        "universe",
        universe_buy_entries,
        universe_sell_entries,
    )
    write_test_file(
        test_dir / "typeUniverseTest.jsonl",
        universe_rows,
        universe_summary,
        universe_buy_info,
        universe_sell_info,
    )

    hub_ids = [int(h["stationID"]) for h in hubs]
    hubs_rows = fetch_raw_segment_rows(conn, test_type_id, hub_ids)
    hubs_buy_entries, hubs_sell_entries = aggregate_entries_for_flags(hubs_rows)
    hubs_buy_info = analyze_buy(hubs_buy_entries)
    hubs_sell_info = analyze_sell(hubs_sell_entries)
    hubs_summary = summarize_market_line(
        test_type_id,
        type_name,
        None,
        "hubs",
        hubs_buy_entries,
        hubs_sell_entries,
    )
    write_test_file(
        test_dir / "typeHubsTest.jsonl",
        hubs_rows,
        hubs_summary,
        hubs_buy_info,
        hubs_sell_info,
    )

    for idx, hub in enumerate(hubs, start=1):
        hub_id = int(hub["stationID"])
        hub_name = str(hub["station"])
        hub_rows = fetch_raw_segment_rows(conn, test_type_id, [hub_id])
        hub_buy_entries, hub_sell_entries = aggregate_entries_for_flags(hub_rows)
        hub_buy_info = analyze_buy(hub_buy_entries)
        hub_sell_info = analyze_sell(hub_sell_entries)
        hub_summary = summarize_market_line(
            test_type_id,
            type_name,
            hub_id,
            hub_name,
            hub_buy_entries,
            hub_sell_entries,
        )
        write_test_file(
            test_dir / f"typeHub{idx}Test.jsonl",
            hub_rows,
            hub_summary,
            hub_buy_info,
            hub_sell_info,
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landu-root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--hubs-out", required=True)
    ap.add_argument("--pages-cache", required=True)
    ap.add_argument("--tuning-state", required=True)
    ap.add_argument("--run-metrics-path", required=True)
    ap.add_argument("--test-type-config", required=True)
    ap.add_argument("--test-dir", required=True)
    ap.add_argument("--sheets-id", required=True)
    ap.add_argument("--sheets-range", required=True)
    ap.add_argument("--esi-base", required=True)
    ap.add_argument("--datasource", required=True)
    ap.add_argument("--user-agent", required=True)
    ap.add_argument("--base-max-workers", type=int, default=10)
    ap.add_argument("--base-retry-budget", type=int, default=10)
    ap.add_argument("--force-refresh", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    start_ts = time.perf_counter()
    ok = False
    stats = StatsCollector()
    retry_budget_obj: RetryBudget | None = None

    landu_root = Path(args.landu_root)
    out_path = Path(args.out)
    hubs_out_path = Path(args.hubs_out)
    pages_cache_path = Path(args.pages_cache)
    tuning_state_path = Path(args.tuning_state)
    run_metrics_path = Path(args.run_metrics_path)
    test_type_config_path = Path(args.test_type_config)
    test_dir = Path(args.test_dir)

    test_type_id = load_test_type_id(test_type_config_path)

    tuning_state = load_tuning_state(
        tuning_state_path,
        base_max_workers=int(args.base_max_workers),
        base_retry_budget=int(args.base_retry_budget),
    )

    selected_workers, selected_retry_budget = choose_tuned_parameters(
        tuning_state,
        base_workers=int(args.base_max_workers),
        base_retry_budget=int(args.base_retry_budget),
    )

    LOG.info("Autotuning selected max_workers=%s retry_budget=%s", selected_workers, selected_retry_budget)
    LOG.info("Test type_id=%s", test_type_id)

    try:
        LOG.info("Loading inputs from landu repo at %s", landu_root)

        types_path = detect_types_file(landu_root)
        types = load_types(types_path)
        type_map = {int(t.type_id): str(t.type_name) for t in types}
        type_name = type_map.get(int(test_type_id), f"type_{test_type_id}")

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
            est = cache["stations"].get(r.region_id, 1) if cache else 1
            entities.append(Entity(kind="region", id=r.region_id, name=r.region_name, pages_est=est))

        for sid, sname in structures_map.items():
            est = cache["structures"].get(sid, 1) if cache else 1
            entities.append(Entity(kind="structure", id=sid, name=sname, pages_est=est))

        bins = greedy_balance(entities, selected_workers)
        LOG.info("Planned %d entities across %d workers", len(entities), selected_workers)

        retry_budget_obj = RetryBudget(selected_retry_budget)
        client = EsiClient(base=args.esi_base, datasource=args.datasource, user_agent=args.user_agent)

        tmpdir = Path(tempfile.mkdtemp(prefix="eou_marketPrices_esi_gh_"))
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
                    retry_budget=retry_budget_obj,
                    push_orders_fn=push_orders,
                    stats=stats,
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

        LOG.info("Starting ingestion")
        with concurrent.futures.ThreadPoolExecutor(max_workers=selected_workers) as ex:
            futs = [ex.submit(worker_fn, b) for b in bins if b]
            for f in concurrent.futures.as_completed(futs):
                f.result()

        writer.stop()
        LOG.info("Ingestion complete; DB at %s", db_path)

        conn = connect(db_path)

        total_orders, hubs = compute_hubs(conn, location_names)
        LOG.info("Hubs passing 1.75%% threshold: %d", len(hubs))

        conn.execute("DROP TABLE IF EXISTS hubs;")
        conn.execute("CREATE TEMP TABLE hubs (location_id INTEGER PRIMARY KEY, rank INTEGER NOT NULL, name TEXT NOT NULL);")
        for rank, hub in enumerate(hubs, start=1):
            conn.execute(
                "INSERT INTO hubs(location_id, rank, name) VALUES (?,?,?)",
                (int(hub["stationID"]), int(rank), str(hub["station"])),
            )
        conn.commit()

        conn.execute("DROP TABLE IF EXISTS universe_buy;")
        conn.execute(
            """
            CREATE TEMP TABLE universe_buy AS
            SELECT type_id, price, SUM(volume_remain) AS vol
            FROM orders WHERE is_buy=1
            GROUP BY type_id, price
            ORDER BY type_id ASC, price DESC;
            """
        )
        conn.execute("DROP TABLE IF EXISTS universe_sell;")
        conn.execute(
            """
            CREATE TEMP TABLE universe_sell AS
            SELECT type_id, price, SUM(volume_remain) AS vol
            FROM orders WHERE is_buy=0
            GROUP BY type_id, price
            ORDER BY type_id ASC, price ASC;
            """
        )

        conn.execute("DROP TABLE IF EXISTS hubs_buy;")
        conn.execute(
            """
            CREATE TEMP TABLE hubs_buy AS
            SELECT o.type_id, o.location_id, o.price, SUM(o.volume_remain) AS vol
            FROM orders o
            JOIN hubs h ON h.location_id = o.location_id
            WHERE o.is_buy=1
            GROUP BY o.type_id, o.location_id, o.price
            ORDER BY o.type_id ASC, o.location_id ASC, o.price DESC;
            """
        )
        conn.execute("DROP TABLE IF EXISTS hubs_sell;")
        conn.execute(
            """
            CREATE TEMP TABLE hubs_sell AS
            SELECT o.type_id, o.location_id, o.price, SUM(o.volume_remain) AS vol
            FROM orders o
            JOIN hubs h ON h.location_id = o.location_id
            WHERE o.is_buy=0
            GROUP BY o.type_id, o.location_id, o.price
            ORDER BY o.type_id ASC, o.location_id ASC, o.price ASC;
            """
        )
        conn.commit()

        ub_stream = TypeRowStream(conn.execute("SELECT type_id, price, vol FROM universe_buy ORDER BY type_id, price DESC;"))
        us_stream = TypeRowStream(conn.execute("SELECT type_id, price, vol FROM universe_sell ORDER BY type_id, price ASC;"))
        hb_stream = TypeRowStream(conn.execute("SELECT type_id, location_id, price, vol FROM hubs_buy ORDER BY type_id, location_id, price DESC;"))
        hs_stream = TypeRowStream(conn.execute("SELECT type_id, location_id, price, vol FROM hubs_sell ORDER BY type_id, location_id, price ASC;"))

        out_path.parent.mkdir(parents=True, exist_ok=True)
        LOG.info("Writing NDJSON.gz to %s", out_path)

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

                for hub in hubs:
                    loc = int(hub["stationID"])
                    name = str(hub["station"])
                    prices.append(build_segment_entry(loc, name, buy_by_loc.get(loc, []), sell_by_loc.get(loc, [])))

                gz.write(
                    json.dumps(
                        {"typeID": tid, "type": t.type_name, "prices": prices},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )

        write_hubs_json(hubs_out_path, total_orders, hubs)
        LOG.info("Wrote hubs json to %s", hubs_out_path)

        generate_test_outputs(
            conn=conn,
            test_dir=test_dir,
            test_type_id=test_type_id,
            type_name=type_name,
            hubs=hubs,
        )
        LOG.info("Wrote test files to %s", test_dir)

        regions_cache_rows = [(r.region_id, r.region_name, int(observed_regions.get(r.region_id, 1))) for r in regions]
        structs_cache_rows = []
        for sid, sname in structures_map.items():
            if sid in ignored_structs:
                continue
            structs_cache_rows.append((sid, sname, int(observed_structs.get(sid, 1))))

        write_pages_cache(pages_cache_path, regions_cache_rows, structs_cache_rows)
        LOG.info("Wrote pages cache to %s", pages_cache_path)

        conn.close()
        ok = True
        LOG.info("Done.")

    finally:
        elapsed = time.perf_counter() - start_ts
        stats_snapshot = stats.snapshot()
        retries_used = retry_budget_obj.used() if retry_budget_obj is not None else 0

        tuning_state = load_tuning_state(
            tuning_state_path,
            base_max_workers=int(args.base_max_workers),
            base_retry_budget=int(args.base_retry_budget),
        )

        ts = _utc_now_iso()

        history_item = {
            "ts": ts,
            "selected": {
                "max_workers": int(selected_workers),
                "retry_budget": int(selected_retry_budget),
            },
            "result": {
                "ok": bool(ok),
                "ingestSeconds": float(elapsed),
                "retriesUsed": int(retries_used),
                "http401": int(stats_snapshot.get("http401", 0)),
                "http429": int(stats_snapshot.get("http429", 0)),
                "backoffSeconds": float(stats_snapshot.get("backoff_seconds", 0.0)),
                "requests": int(stats_snapshot.get("requests", 0)),
                "score": compute_score(
                    ok=ok,
                    ingest_seconds=elapsed,
                    http401=int(stats_snapshot.get("http401", 0)),
                    http429=int(stats_snapshot.get("http429", 0)),
                    backoff_seconds=float(stats_snapshot.get("backoff_seconds", 0.0)),
                ),
            },
        }

        history = list(tuning_state.get("history", []))
        history.append(history_item)
        history = history[-5:]

        best = dict(tuning_state.get("best", {}))
        this_score = history_item["result"]["score"]
        if ok and (best.get("score") is None or float(this_score) < float(best["score"])):
            best = {
                "max_workers": int(selected_workers),
                "retry_budget": int(selected_retry_budget),
                "score": float(this_score),
                "ts": ts,
            }

        temp_state = {
            **tuning_state,
            "history": history,
            "best": best,
            "current": {"max_workers": selected_workers, "retry_budget": selected_retry_budget},
        }

        next_workers, next_retry_budget = choose_tuned_parameters(
            temp_state,
            base_workers=selected_workers,
            base_retry_budget=selected_retry_budget,
        )

        new_tuning_state = {
            "version": int(tuning_state.get("version", 1)),
            "status": tuning_state.get("status", "in progress"),
            "next_run": tuning_state.get("next_run"),
            "failed": int(tuning_state.get("failed", 0)),
            "current": {
                "max_workers": int(next_workers),
                "retry_budget": int(next_retry_budget),
            },
            "best": best,
            "history": history,
        }

        write_tuning_state(tuning_state_path, new_tuning_state)
        write_run_metrics(
            run_metrics_path,
            {
                "ts": ts,
                "selected": {
                    "max_workers": int(selected_workers),
                    "retry_budget": int(selected_retry_budget),
                },
                "result": history_item["result"],
                "next_current": new_tuning_state["current"],
                "maxLastModified": stats_snapshot.get("max_last_modified"),
            },
        )
        LOG.info("Wrote tuning state to %s", tuning_state_path)
        LOG.info("Wrote run metrics to %s", run_metrics_path)


if __name__ == "__main__":
    main()
