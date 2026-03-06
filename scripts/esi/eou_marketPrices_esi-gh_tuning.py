#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))


def default_state(base_workers: int = 10, base_retry_budget: int = 10) -> Dict:
    return {
        "version": 1,
        "failed": 0,
        "status": "completed",
        "next_run": None,
        "current": {
            "max_workers": int(base_workers),
            "retry_budget": int(base_retry_budget),
        },
        "best": {
            "max_workers": int(base_workers),
            "retry_budget": int(base_retry_budget),
            "score": None,
            "ts": None,
        },
        "history": [],
    }


def load_tuning_state(path: Path, base_workers: int = 10, base_retry_budget: int = 10) -> Dict:
    if not path.exists():
        return default_state(base_workers=base_workers, base_retry_budget=base_retry_budget)

    obj = json.loads(path.read_text(encoding="utf-8"))
    base = default_state(base_workers=base_workers, base_retry_budget=base_retry_budget)
    base.update(obj)
    base["current"] = {**default_state()["current"], **obj.get("current", {})}
    base["best"] = {**default_state()["best"], **obj.get("best", {})}
    base["history"] = obj.get("history", [])
    base["failed"] = int(obj.get("failed", 0))
    base["status"] = str(obj.get("status", "completed"))
    base["next_run"] = obj.get("next_run")
    return base


def write_tuning_state(path: Path, state: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def compute_score(ok: bool, ingest_seconds: float, http401: int, http429: int, backoff_seconds: float) -> float:
    score = float(ingest_seconds)
    score += 15.0 * float(http429)
    score += 5.0 * float(http401)
    score += 0.5 * float(backoff_seconds)
    if not ok:
        score += 300.0
    return score


def recent_bad_probe(history: List[Dict], candidate_workers: int, best_score: float) -> bool:
    for item in reversed(history[-5:]):
        selected = item.get("selected", {})
        result = item.get("result", {})
        if int(selected.get("max_workers", -1)) != int(candidate_workers):
            continue
        if not bool(result.get("ingestOk", result.get("ok", False))):
            continue
        score = result.get("score")
        if score is None:
            continue
        if float(score) > float(best_score) * 1.02:
            return True
    return False


def choose_tuned_parameters(state: Dict, base_workers: int, base_retry_budget: int) -> Tuple[int, int]:
    min_workers, max_workers = 4, 16
    min_retry, max_retry = 10, 50

    current = state.get("current") or {}
    history = state.get("history") or []
    best = state.get("best") or {}

    workers = int(current.get("max_workers", base_workers))
    retry_budget = int(current.get("retry_budget", base_retry_budget))

    if not history:
        return clamp(workers, min_workers, max_workers), clamp(retry_budget, min_retry, max_retry)

    best_score = best.get("score")
    best_workers = int(best.get("max_workers", workers))
    best_retry = int(best.get("retry_budget", retry_budget))

    last = history[-1]
    last_selected = last.get("selected", {})
    last_result = last.get("result", {})

    ingest_ok = bool(last_result.get("ingestOk", last_result.get("ok", False)))
    last_score = last_result.get("score")
    last_workers = int(last_selected.get("max_workers", workers))
    last_retry = int(last_selected.get("retry_budget", retry_budget))
    last_429 = int(last_result.get("http429", 0))
    last_backoff = float(last_result.get("backoffSeconds", 0.0))
    last_retries = int(last_result.get("retriesUsed", 0))

    # Si no hay best score todavía, usa current/base
    if best_score is None:
        if not ingest_ok:
            return clamp(last_workers - 2, min_workers, max_workers), clamp(last_retry + 5, min_retry, max_retry)
        return clamp(workers, min_workers, max_workers), clamp(retry_budget, min_retry, max_retry)

    # Si la última ingesta falló, vuelve al best y/o baja agresividad.
    if not ingest_ok:
        return clamp(best_workers, min_workers, max_workers), clamp(max(best_retry, last_retry + 5), min_retry, max_retry)

    # Si la última exploración es peor que el best, vuelve al best.
    if last_score is not None and float(last_score) > float(best_score) * 1.02:
        return clamp(best_workers, min_workers, max_workers), clamp(best_retry, min_retry, max_retry)

    # Si el último run fue el best y estuvo limpio, explora ligeramente por encima.
    if (
        last_workers == best_workers
        and last_score is not None
        and abs(float(last_score) - float(best_score)) <= 1e-9
        and last_429 == 0
        and last_backoff < 30.0
        and last_retries <= 2
    ):
        candidate_workers = clamp(best_workers + 2, min_workers, max_workers)
        candidate_retry = clamp(best_retry, min_retry, max_retry)

        # Si ya sabemos que ese candidato fue peor recientemente, no reexplorar.
        if candidate_workers != best_workers and not recent_bad_probe(history, candidate_workers, float(best_score)):
            return candidate_workers, candidate_retry

        return clamp(best_workers, min_workers, max_workers), clamp(best_retry, min_retry, max_retry)

    # Si no hay señales malas, quedarse en el best.
    return clamp(best_workers, min_workers, max_workers), clamp(best_retry, min_retry, max_retry)


def cmd_init(args) -> None:
    path = Path(args.tuning_state)
    state = load_tuning_state(path, base_workers=args.base_max_workers, base_retry_budget=args.base_retry_budget)

    now = utc_now()
    state["status"] = "in_progress"
    state["next_run"] = iso_z(now + timedelta(seconds=int(args.lock_time)))

    write_tuning_state(path, state)


def cmd_finalize(args) -> None:
    path = Path(args.tuning_state)
    state = load_tuning_state(path)

    metrics_path = Path(args.metrics_in)
    summary_out = Path(args.summary_out)
    overall_status = str(args.overall_status).strip()

    metrics: Dict = {}
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    now = utc_now()

    if overall_status == "completed":
        failed = 0
        next_run_dt = now + timedelta(minutes=5)
        status = "completed"
    else:
        failed = int(state.get("failed", 0)) + 1
        status = "failed"
        if failed < 10:
            next_run_dt = now + timedelta(minutes=1)
        elif failed < 20:
            next_run_dt = now + timedelta(minutes=30)
        else:
            next_run_dt = now + timedelta(hours=24)

    state["failed"] = failed
    state["status"] = status
    state["next_run"] = iso_z(next_run_dt)

    selected = metrics.get("selected", {})
    result = metrics.get("result", {})

    if selected and result:
        history = state.get("history", [])

        entry = {
            "ts": iso_z(now),
            "selected": {
                "max_workers": int(selected.get("max_workers", state["current"]["max_workers"])),
                "retry_budget": int(selected.get("retry_budget", state["current"]["retry_budget"])),
            },
            "result": {
                "ok": bool(overall_status == "completed"),
                "ingestOk": bool(result.get("pipelineOk", False)),
                "ingestSeconds": float(result.get("ingestSeconds", 0.0)),
                "retriesUsed": int(result.get("retriesUsed", 0)),
                "http401": int(result.get("http401", 0)),
                "http429": int(result.get("http429", 0)),
                "backoffSeconds": float(result.get("backoffSeconds", 0.0)),
                "requests": int(result.get("requests", 0)),
                "score": compute_score(
                    ok=bool(result.get("pipelineOk", False)),
                    ingest_seconds=float(result.get("ingestSeconds", 0.0)),
                    http401=int(result.get("http401", 0)),
                    http429=int(result.get("http429", 0)),
                    backoff_seconds=float(result.get("backoffSeconds", 0.0)),
                ),
            },
        }

        history.append(entry)
        state["history"] = history[-5:]

        # Best se actualiza con la calidad de la ingesta (no con el éxito del commit cross-repo)
        if entry["result"]["ingestOk"]:
            best = state.get("best", {})
            best_score = best.get("score")
            if best_score is None or float(entry["result"]["score"]) < float(best_score):
                state["best"] = {
                    "max_workers": int(entry["selected"]["max_workers"]),
                    "retry_budget": int(entry["selected"]["retry_budget"]),
                    "score": float(entry["result"]["score"]),
                    "ts": entry["ts"],
                }

    next_workers, next_retry_budget = choose_tuned_parameters(
        state,
        base_workers=int(state["current"]["max_workers"]),
        base_retry_budget=int(state["current"]["retry_budget"]),
    )
    state["current"] = {
        "max_workers": int(next_workers),
        "retry_budget": int(next_retry_budget),
    }

    write_tuning_state(path, state)

    summary = {
        "status": status,
        "failed": int(failed),
        "next_run": state["next_run"],
        "next_run_epoch": int(next_run_dt.timestamp()),
        "current": state["current"],
    }
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init")
    p_init.add_argument("--tuning-state", required=True)
    p_init.add_argument("--base-max-workers", type=int, default=10)
    p_init.add_argument("--base-retry-budget", type=int, default=10)
    p_init.add_argument("--lock-time", type=int, required=True)
    p_init.set_defaults(func=cmd_init)

    p_fin = sub.add_parser("finalize")
    p_fin.add_argument("--tuning-state", required=True)
    p_fin.add_argument("--metrics-in", required=True)
    p_fin.add_argument("--summary-out", required=True)
    p_fin.add_argument("--overall-status", required=True, choices=["completed", "failed"])
    p_fin.set_defaults(func=cmd_finalize)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
