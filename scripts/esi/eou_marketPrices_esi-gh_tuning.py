from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple

MIN_WORKERS = 4
MAX_WORKERS = 16
MIN_RETRY_BUDGET = 10
MAX_RETRY_BUDGET = 50
DEFAULT_WORKERS = 10
DEFAULT_RETRY_BUDGET = 10
MAX_HISTORY = 60


def _clamp(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(v)))


def load_tuning_state(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {
            "current": {
                "max_workers": DEFAULT_WORKERS,
                "retry_budget": DEFAULT_RETRY_BUDGET,
            },
            "best": None,
            "history": [],
        }
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def choose_params(
    path: str | Path,
    override_workers: int,
    override_retry_budget: int,
) -> Tuple[int, int, Dict[str, Any]]:
    state = load_tuning_state(path)

    if int(override_workers) > 0:
        workers = _clamp(int(override_workers), MIN_WORKERS, MAX_WORKERS)
    else:
        workers = _clamp(
            int(state.get("current", {}).get("max_workers", DEFAULT_WORKERS)),
            MIN_WORKERS,
            MAX_WORKERS,
        )

    if int(override_retry_budget) > 0:
        retry_budget = _clamp(int(override_retry_budget), MIN_RETRY_BUDGET, MAX_RETRY_BUDGET)
    else:
        retry_budget = _clamp(
            int(state.get("current", {}).get("retry_budget", DEFAULT_RETRY_BUDGET)),
            MIN_RETRY_BUDGET,
            MAX_RETRY_BUDGET,
        )

    return workers, retry_budget, state


def _score(run: Dict[str, Any]) -> float:
    # Objetivo: mínimo tiempo, pero penalizando conductas malas
    ingest_seconds = float(run.get("ingestSeconds", 0.0))
    http401 = int(run.get("http401", 0))
    http420 = int(run.get("http420", 0))
    http429 = int(run.get("http429", 0))
    backoff_seconds = float(run.get("backoffSeconds", 0.0))
    ok = bool(run.get("ok", False))

    score = ingest_seconds
    score += 5.0 * http401
    score += 15.0 * http420
    score += 20.0 * http429
    score += 0.5 * backoff_seconds
    if not ok:
        score += 300.0
    return round(score, 3)


def _propose_next(current_workers: int, current_retry_budget: int, run: Dict[str, Any]) -> Tuple[int, int]:
    ok = bool(run.get("ok", False))
    http429 = int(run.get("http429", 0))
    http420 = int(run.get("http420", 0))
    retries_used = int(run.get("retriesUsed", 0))
    backoff_seconds = float(run.get("backoffSeconds", 0.0))

    next_workers = current_workers
    next_retry_budget = current_retry_budget

    # Si el run fue mal o hubo mucha presión, bajar agresividad
    if (not ok) or http429 > 0 or http420 > 0 or backoff_seconds > 120.0:
        next_workers = _clamp(current_workers - 2, MIN_WORKERS, MAX_WORKERS)
        next_retry_budget = _clamp(max(current_retry_budget, retries_used + 10), MIN_RETRY_BUDGET, MAX_RETRY_BUDGET)
        return next_workers, next_retry_budget

    # Si fue bien y casi no hubo presión, explorar un poco hacia arriba
    if ok and http429 == 0 and http420 == 0 and backoff_seconds < 30.0 and retries_used <= max(2, current_retry_budget // 5):
        next_workers = _clamp(current_workers + 1, MIN_WORKERS, MAX_WORKERS)
        next_retry_budget = _clamp(max(MIN_RETRY_BUDGET, current_retry_budget - 1), MIN_RETRY_BUDGET, MAX_RETRY_BUDGET)
        return next_workers, next_retry_budget

    # Zona media: mantener workers y ajustar poco el budget
    if retries_used >= max(3, int(current_retry_budget * 0.7)):
        next_retry_budget = _clamp(current_retry_budget + 5, MIN_RETRY_BUDGET, MAX_RETRY_BUDGET)
    elif retries_used <= max(1, int(current_retry_budget * 0.2)):
        next_retry_budget = _clamp(current_retry_budget - 1, MIN_RETRY_BUDGET, MAX_RETRY_BUDGET)

    return next_workers, next_retry_budget


def update_tuning_state(
    path: str | Path,
    previous_state: Dict[str, Any],
    run_metrics: Dict[str, Any],
    override_workers: int,
    override_retry_budget: int,
) -> Dict[str, Any]:
    state = previous_state or {"current": {}, "best": None, "history": []}

    run = dict(run_metrics)
    run["score"] = _score(run)

    history = list(state.get("history", []))
    history.append(run)
    history = history[-MAX_HISTORY:]

    best = state.get("best")
    if bool(run.get("ok", False)):
        if best is None or float(run["score"]) < float(best.get("score", 10**18)):
            best = {
                "max_workers": int(run["max_workers"]),
                "retry_budget": int(run["retry_budget"]),
                "score": float(run["score"]),
                "ingestSeconds": float(run["ingestSeconds"]),
            }

    if int(override_workers) > 0 or int(override_retry_budget) > 0:
        # Si hubo override manual, no tocamos current automáticamente
        current = {
            "max_workers": int(run["max_workers"]),
            "retry_budget": int(run["retry_budget"]),
        }
    else:
        next_workers, next_retry_budget = _propose_next(
            int(run["max_workers"]),
            int(run["retry_budget"]),
            run,
        )
        current = {
            "max_workers": int(next_workers),
            "retry_budget": int(next_retry_budget),
        }

    new_state = {
        "current": current,
        "best": best,
        "history": history,
    }

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(new_state, f, indent=2, ensure_ascii=False)
        f.write("\n")

    return new_state
