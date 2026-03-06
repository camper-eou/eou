#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import importlib.util
import json
import logging
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

load_regions = sde_mod.load_regions
load_stations_map = sde_mod.load_stations_map
load_structures_map = sde_mod.load_structures_map

read_tokens_from_sheet = sheets_mod.read_tokens_from_sheet

OrdersWriter = sqlite_mod.OrdersWriter
connect = sqlite_mod.connect

detect_types_file = types_mod.detect_types_file
load_types = types_mod.load_types


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
        (int(h.get("selected", {}).get("max_workers", best_workers)),
         int(h.get("selected", {}).get("retry_budget", best_retry)))
        for h in history[-5:]
    }

    if not last_ok:
        return (clamp(best_workers, min_workers, max_workers), clamp(min(best_retry + 5, max_retry), min_retry, max_retry))

    if last_429 > 0 or last_backoff > 120 or (last_budget > 0 and last_retries >= int(last_budget * 0.8)):
        return (clamp(best_workers - 2, min_workers, max_workers), clamp(best_retry + 5, min_retry, max_retry))

    if best_score is not None and last_score > float(best_score):
        # Si el último empeora, volvemos al best.
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


def update_tuning_state(
    state: Dict,
    selected_workers: int,
    selected_retry_budget: int,
    ok: bool,
    ingest_seconds: float,
    stats: Dict[str, float],
    retries_used: int,
) -> Dict:
    ts = _utc_now_iso()

    history_item = {
        "ts": ts,
        "selected": {
            "max_workers": int(selected_workers),
            "retry_budget": int(selected_retry_budget),
        },
        "result": {
            "ok": bool(ok),
            "ingestSeconds": float(ingest_seconds),
            "retriesUsed": int(retries_used),
            "http401": int(stats.get("http401", 0)),
            "http429": int(stats.get("http429", 0)),
            "backoffSeconds": float(stats.get("backoff_seconds", 0.0)),
            "requests": int(stats.get("requests", 0)),
            "score": compute_score(
                ok=ok,
                ingest_seconds=ingest_seconds,
                http401=int(stats.get("http401", 0)),
                http429=int(stats.get("http429", 0)),
                backoff_seconds=float(stats.get("backoff_seconds", 0.0)),
            ),
        },
    }

    history = list(state.get("history", []))
    history.append(history_item)
    history = history[-5:]

    best = dict(state.get("best", {}))
    this_score = history_item["result"]["score"]
    if ok and (best.get("score") is None or float(this_score) < float(best["score"])):
        best = {
            "max_workers": int(selected_workers),
            "retry_budget": int(selected_retry_budget),
            "score": float(this_score),
            "ts": ts,
        }

    temp_state = {
        **state,
        "history": history,
        "best": best,
        "current": {"max_workers": selected_workers, "retry_budget": selected_retry_budget},
    }

    next_workers, next_retry_budget = choose_tuned_parameters(
        temp_state,
        base_workers=selected_workers,
        base_retry_budget=selected_retry_budget,
    )

    return {
        "version": int(state.get("version", 1)),
        "status": state.get("status", "in progress"),
        "next_run": state.get("next_run"),
        "failed": int(state.get("failed", 0)),
        "current": {
            "max_workers": int(next_workers),
            "retry_budget": int(next_retry_budget),
        },
        "best": best,
        "history": history,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landu-root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--hubs-out", required=True)
    ap.add_argument("--pages-cache", required=True)
    ap.add_argument("--tuning-state", required=True)
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

    LOG.info(
        "Autotuning selected max_workers=%s retry_budget=%s",
        selected_workers,
        selected_retry_budget,
    )

    try:
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

        new_tuning_state = update_tuning_state(
            state=tuning_state,
            selected_workers=selected_workers,
            selected_retry_budget=selected_retry_budget,
            ok=ok,
            ingest_seconds=elapsed,
            stats=stats_snapshot,
            retries_used=retries_used,
        )
        write_tuning_state(tuning_state_path, new_tuning_state)
        LOG.info("Wrote tuning state to %s", tuning_state_path)


if __name__ == "__main__":
    main()
