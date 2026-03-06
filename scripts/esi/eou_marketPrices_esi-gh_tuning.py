#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_state(base_max_workers: int = 10, base_retry_budget: int = 10) -> Dict[str, Any]:
    return {
        "version": 1,
        "status": "idle",
        "next_run": None,
        "failed": 0,
        "current": {
            "max_workers": int(base_max_workers),
            "retry_budget": int(base_retry_budget),
        },
        "best": {
            "max_workers": int(base_max_workers),
            "retry_budget": int(base_retry_budget),
            "score": None,
            "ts": None,
        },
        "history": [],
    }


def load_state(path: Path, base_max_workers: int = 10, base_retry_budget: int = 10) -> Dict[str, Any]:
    if not path.exists():
        return default_state(base_max_workers, base_retry_budget)

    obj = json.loads(path.read_text(encoding="utf-8"))
    obj.setdefault("version", 1)
    obj.setdefault("status", "idle")
    obj.setdefault("next_run", None)
    obj.setdefault("failed", 0)
    obj.setdefault(
        "current",
        {"max_workers": int(base_max_workers), "retry_budget": int(base_retry_budget)},
    )
    obj.setdefault(
        "best",
        {
            "max_workers": int(base_max_workers),
            "retry_budget": int(base_retry_budget),
            "score": None,
            "ts": None,
        },
    )
    obj.setdefault("history", [])
    return obj


def write_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def cmd_init(path: Path, lock_seconds: int, base_max_workers: int, base_retry_budget: int) -> None:
    state = load_state(path, base_max_workers, base_retry_budget)
    state["status"] = "in progress"
    state["next_run"] = iso_z(utc_now() + timedelta(seconds=int(lock_seconds)))
    write_state(path, state)


def cmd_finalize_success(path: Path) -> None:
    state = load_state(path)
    state["status"] = "completed"
    state["failed"] = 0
    state["next_run"] = iso_z(utc_now() + timedelta(minutes=5))
    write_state(path, state)


def cmd_finalize_failure(path: Path) -> None:
    state = load_state(path)
    new_failed = int(state.get("failed", 0)) + 1
    state["failed"] = new_failed
    state["status"] = "failed"

    if new_failed < 10:
        delta = timedelta(minutes=1)
    elif new_failed < 20:
        delta = timedelta(minutes=30)
    else:
        delta = timedelta(hours=24)

    state["next_run"] = iso_z(utc_now() + delta)
    write_state(path, state)


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_init = sub.add_parser("init")
    ap_init.add_argument("--path", required=True)
    ap_init.add_argument("--lock-seconds", required=True, type=int)
    ap_init.add_argument("--base-max-workers", required=True, type=int)
    ap_init.add_argument("--base-retry-budget", required=True, type=int)

    ap_ok = sub.add_parser("finalize-success")
    ap_ok.add_argument("--path", required=True)

    ap_fail = sub.add_parser("finalize-failure")
    ap_fail.add_argument("--path", required=True)

    args = ap.parse_args()
    path = Path(args.path)

    if args.cmd == "init":
        cmd_init(path, args.lock_seconds, args.base_max_workers, args.base_retry_budget)
    elif args.cmd == "finalize-success":
        cmd_finalize_success(path)
    elif args.cmd == "finalize-failure":
        cmd_finalize_failure(path)
    else:
        raise SystemExit(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
