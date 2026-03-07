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
    """
    Fecha UTC compacta usada para run_metrics y tuning history.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_module(module_tag: str):
    """
    Carga módulos hermanos usando el prefijo común del workflow.
    """
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
    """
    Carga el planner histórico de páginas por región/estructura.

    Si no existe, el reparto entre workers se basa en pages_est=1.
    """
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
    """
    Reescribe por completo el cache planner.

    - regions: todas las regiones del truth set
    - structures: solo estructuras no ignoradas
    """
    out = {
        "stations": [{"regionID": rid, "region": rname, "pages": pages} for rid, rname, pages in regions],
        "structures": [{"stationID": sid, "station": sname, "pages": pages} for sid, sname, pages in structures],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def greedy_balance(entities: List[Entity], workers: int) -> List[List[Entity]]:
    """
    Reparto greedy por pages_est para equilibrar workers.

    Se asigna cada entidad al worker con menor carga acumulada.
    """
    bins: List[List[Entity]] = [[] for _ in range(workers)]
    loads = [0] * workers
    entities_sorted = sorted(entities, key=lambda e: int(e.pages_est or 1), reverse=True)
    for e in entities_sorted:
        i = min(range(workers), key=lambda k: loads[k])
        bins[i].append(e)
        loads[i] += int(e.pages_est or 1)
    return bins


def compute_hubs(conn, location_names: Dict[int, str]) -> Tuple[int, List[Dict]]:
    """
    Determina los hubs del universo según el umbral de ordersShare > 1.75%.

    Desempate:
      1) orders desc
      2) types desc
      3) sum(price * volume_remain) desc
      4) location_id asc
    """
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


def build_segment_entry(hub_id, hub_name: str, buy_entries, sell_entries) -> Dict:
    """
    Calcula el bloque de salida para:
      - universe
      - hubs
      - cada hub individual
    """
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
    """
    Escribe el resumen estructurado de hubs en landu-eou/eou/data/esi/hubs.json.
    """
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
    """
    Carga el estado de tuning.

    Si no existe, se construye uno por defecto con los valores base.
    """
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
    """
    Guarda el estado de tuning de forma legible.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))


def compute_score(ok: bool, ingest_seconds: float, http401: int, http429: int, backoff_seconds: float) -> float:
    """
    Score simple para autotuning:
      menor = mejor

    Penaliza:
      - duración
      - 401
      - 429
      - segundos de backoff
      - fallo completo del run
    """
    score = float(ingest_seconds)
    score += 15.0 * float(http429)
    score += 5.0 * float(http401)
    score += 0.5 * float(backoff_seconds)
    if not ok:
        score += 300.0
    return score


def choose_tuned_parameters(state: Dict, base_workers: int, base_retry_budget: int) -> Tuple[int, int]:
    """
    Selecciona la combinación a probar en este run.

    Mantiene la estrategia actual:
      - explora alrededor del mejor
      - baja workers si hubo presión/rate limit
      - sube retry_budget si el run fue problemático
    """
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
    """
    Guarda métricas del run para que tuning/finalización puedan reutilizarlas.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def fetch_universe_entries(conn: sqlite3.Connection, type_id: int) -> Tuple[List[Tuple[float, int]], List[Tuple[float, int]]]:
    """
    Recupera el libro agregado del universo para un type_id.
    """
    buy_rows = conn.execute(
        """
        SELECT price, vol
        FROM universe_buy
        WHERE type_id = ?
        ORDER BY price DESC
        """,
        (int(type_id),),
    ).fetchall()

    sell_rows = conn.execute(
        """
        SELECT price, vol
        FROM universe_sell
        WHERE type_id = ?
        ORDER BY price ASC
        """,
        (int(type_id),),
    ).fetchall()

    buy_entries = [(float(r["price"]), int(r["vol"])) for r in buy_rows]
    sell_entries = [(float(r["price"]), int(r["vol"])) for r in sell_rows]
    return buy_entries, sell_entries


def fetch_hub_entries_by_loc(conn: sqlite3.Connection, type_id: int) -> Tuple[Dict[int, List[Tuple[float, int]]], Dict[int, List[Tuple[float, int]]]]:
    """
    Recupera el libro agregado por hub para un type_id.

    Devuelve dos diccionarios:
      - buy_by_loc[location_id] = [(price, vol), ...]
      - sell_by_loc[location_id] = [(price, vol), ...]
    """
    buy_rows = conn.execute(
        """
        SELECT location_id, price, vol
        FROM hubs_buy
        WHERE type_id = ?
        ORDER BY location_id ASC, price DESC
        """,
        (int(type_id),),
    ).fetchall()

    sell_rows = conn.execute(
        """
        SELECT location_id, price, vol
        FROM hubs_sell
        WHERE type_id = ?
        ORDER BY location_id ASC, price ASC
        """,
        (int(type_id),),
    ).fetchall()

    buy_by_loc: Dict[int, List[Tuple[float, int]]] = {}
    sell_by_loc: Dict[int, List[Tuple[float, int]]] = {}

    for r in buy_rows:
        loc = int(r["location_id"])
        buy_by_loc.setdefault(loc, []).append((float(r["price"]), int(r["vol"])))

    for r in sell_rows:
        loc = int(r["location_id"])
        sell_by_loc.setdefault(loc, []).append((float(r["price"]), int(r["vol"])))

    return buy_by_loc, sell_by_loc


def main() -> None:
    """
    Flujo principal:

      1) Carga inputs y estado
      2) Decide max_workers / retry_budget
      3) Ingiere órdenes ESI hacia SQLite temporal
      4) Determina hubs
      5) Agrega universe / hubs / cada hub
      6) Escribe marketPices.json.gz y hubs.json
      7) Reescribe pages cache
      8) Actualiza tuning + run_metrics
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--landu-root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--hubs-out", required=True)
    ap.add_argument("--pages-cache", required=True)
    ap.add_argument("--tuning-state", required=True)
    ap.add_argument("--run-metrics-path", required=True)
    ap.add_argument("--sheets-id", required=True)
    ap.add_argument("--sheets-range", required=True)
    ap.add_argument("--esi-base", required=True)
    ap.add_argument("--datasource", required=True)
    ap.add_argument("--user-agent", required=True)
    ap.add_argument("--base-max-workers", type=int, default=10)
    ap.add_argument("--base-retry-budget", type=int, default=10)
    ap.add_argument("--force-refresh", action="store_true")
    args = ap.parse_args()

    # Se mantiene logging por compatibilidad, pero no se emiten mensajes informativos.
    logging.basicConfig(level=logging.CRITICAL)

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

    try:
        types_path = detect_types_file(landu_root)
        types = load_types(types_path)
        regions = load_regions(landu_root)

        stations_map = load_stations_map(landu_root)
        structures_map = load_structures_map(landu_root)
        location_names = {**stations_map, **structures_map}

        tokens = read_tokens_from_sheet(args.sheets_id, args.sheets_range)
        if not tokens:
            raise SystemExit("No tokens returned from Google Sheets.")
        token_mgr = TokenManager(tokens)

        cache = None if args.force_refresh else load_pages_cache(pages_cache_path)

        entities: List[Entity] = []
        for r in regions:
            est = cache["stations"].get(r.region_id, 1) if cache else 1
            entities.append(Entity(kind="region", id=r.region_id, name=r.region_name, pages_est=est))

        for sid, sname in structures_map.items():
            est = cache["structures"].get(sid, 1) if cache else 1
            entities.append(Entity(kind="structure", id=sid, name=sname, pages_est=est))

        bins = greedy_balance(entities, selected_workers)

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

        with concurrent.futures.ThreadPoolExecutor(max_workers=selected_workers) as ex:
            futs = [ex.submit(worker_fn, b) for b in bins if b]
            for f in concurrent.futures.as_completed(futs):
                f.result()

        writer.stop()

        conn = connect(db_path)

        total_orders, hubs = compute_hubs(conn, location_names)

        conn.execute("DROP TABLE IF EXISTS hubs;")
        conn.execute("CREATE TEMP TABLE hubs (location_id INTEGER PRIMARY KEY, rank INTEGER NOT NULL, name TEXT NOT NULL);")
        for rank, hub in enumerate(hubs, start=1):
            conn.execute(
                "INSERT INTO hubs(location_id, rank, name) VALUES (?,?,?)",
                (int(hub["stationID"]), int(rank), str(hub["station"])),
            )
        conn.commit()

        # Tablas agregadas base.
        conn.execute("DROP TABLE IF EXISTS universe_buy;")
        conn.execute(
            """
            CREATE TEMP TABLE universe_buy AS
            SELECT type_id, price, SUM(volume_remain) AS vol
            FROM orders
            WHERE is_buy = 1
            GROUP BY type_id, price;
            """
        )
        conn.execute("DROP TABLE IF EXISTS universe_sell;")
        conn.execute(
            """
            CREATE TEMP TABLE universe_sell AS
            SELECT type_id, price, SUM(volume_remain) AS vol
            FROM orders
            WHERE is_buy = 0
            GROUP BY type_id, price;
            """
        )

        conn.execute("DROP TABLE IF EXISTS hubs_buy;")
        conn.execute(
            """
            CREATE TEMP TABLE hubs_buy AS
            SELECT o.type_id, o.location_id, o.price, SUM(o.volume_remain) AS vol
            FROM orders o
            JOIN hubs h ON h.location_id = o.location_id
            WHERE o.is_buy = 1
            GROUP BY o.type_id, o.location_id, o.price;
            """
        )
        conn.execute("DROP TABLE IF EXISTS hubs_sell;")
        conn.execute(
            """
            CREATE TEMP TABLE hubs_sell AS
            SELECT o.type_id, o.location_id, o.price, SUM(o.volume_remain) AS vol
            FROM orders o
            JOIN hubs h ON h.location_id = o.location_id
            WHERE o.is_buy = 0
            GROUP BY o.type_id, o.location_id, o.price;
            """
        )

        # Índices para consulta rápida por type_id y por hub.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_universe_buy_type_price ON universe_buy(type_id, price);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_universe_sell_type_price ON universe_sell(type_id, price);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hubs_buy_type_loc_price ON hubs_buy(type_id, location_id, price);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hubs_sell_type_loc_price ON hubs_sell(type_id, location_id, price);")
        conn.commit()

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Salida principal NDJSON.gz:
        # una línea por typeID con bloques universe / hubs / hub1..hubN
        with gzip.open(out_path, "wt", encoding="utf-8") as gz:
            for t in types:
                tid = int(t.type_id)

                buy_univ, sell_univ = fetch_universe_entries(conn, tid)
                buy_by_loc, sell_by_loc = fetch_hub_entries_by_loc(conn, tid)

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
                    prices.append(
                        build_segment_entry(
                            loc,
                            name,
                            buy_by_loc.get(loc, []),
                            sell_by_loc.get(loc, []),
                        )
                    )

                gz.write(
                    json.dumps(
                        {"typeID": tid, "type": t.type_name, "prices": prices},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )

        write_hubs_json(hubs_out_path, total_orders, hubs)

        regions_cache_rows = [(r.region_id, r.region_name, int(observed_regions.get(r.region_id, 1))) for r in regions]
        structs_cache_rows = []
        for sid, sname in structures_map.items():
            if sid in ignored_structs:
                continue
            structs_cache_rows.append((sid, sname, int(observed_structs.get(sid, 1))))

        write_pages_cache(pages_cache_path, regions_cache_rows, structs_cache_rows)

        conn.close()
        ok = True

    finally:
        # Se guarda siempre el resultado del run, incluso si hubo excepción.
        elapsed = time.perf_counter() - start_ts
        stats_snapshot = stats.snapshot()
        retries_used = retry_budget_obj.used() if retry_budget_obj is not None else 0

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


if __name__ == "__main__":
    main()
