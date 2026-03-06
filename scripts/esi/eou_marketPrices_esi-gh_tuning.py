#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_z(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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


def write_summary(path: Path, status: str, next_run: str, last_modified: Optional[str], write_last_modified: bool) -> None:
    payload = {
        "status": status,
        "next_run": next_run,
        "last_modified": last_modified,
        "write_last_modified": bool(write_last_modified),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def cmd_init(path: Path, lock_seconds: int, base_max_workers: int, base_retry_budget: int, summary_path: Path) -> None:
    state = load_state(path, base_max_workers, base_retry_budget)
    next_run = iso_z(utc_now() + timedelta(seconds=int(lock_seconds)))
    state["status"] = "in progress"
    state["next_run"] = next_run
    write_state(path, state)
    write_summary(summary_path, "in progress", next_run, None, False)


def cmd_finalize_success(path: Path, metrics_path: Path, summary_path: Path) -> None:
    state = load_state(path)
    now = utc_now()

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    last_modified_iso = metrics.get("maxLastModified")

    candidate_now = now + timedelta(minutes=2)
    candidate_lm = None
    if last_modified_iso:
        lm_dt = parse_iso_z(last_modified_iso)
        if lm_dt is not None:
            candidate_lm = lm_dt + timedelta(minutes=5)

    if candidate_lm is not None and candidate_lm > candidate_now:
        next_run_dt = candidate_lm
    else:
        next_run_dt = candidate_now

    next_run = iso_z(next_run_dt)

    state["status"] = "completed"
    state["failed"] = 0
    state["next_run"] = next_run
    write_state(path, state)
    write_summary(summary_path, "completed", next_run, last_modified_iso, bool(last_modified_iso))


def cmd_finalize_failure(path: Path, summary_path: Path) -> None:
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

    next_run = iso_z(utc_now() + delta)
    state["next_run"] = next_run
    write_state(path, state)
    write_summary(summary_path, "failed", next_run, None, False)


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_init = sub.add_parser("init")
    ap_init.add_argument("--path", required=True)
    ap_init.add_argument("--lock-seconds", required=True, type=int)
    ap_init.add_argument("--base-max-workers", required=True, type=int)
    ap_init.add_argument("--base-retry-budget", required=True, type=int)
    ap_init.add_argument("--summary-path", required=True)

    ap_ok = sub.add_parser("finalize-success")
    ap_ok.add_argument("--path", required=True)
    ap_ok.add_argument("--metrics-path", required=True)
    ap_ok.add_argument("--summary-path", required=True)

    ap_fail = sub.add_parser("finalize-failure")
    ap_fail.add_argument("--path", required=True)
    ap_fail.add_argument("--summary-path", required=True)

    args = ap.parse_args()

    if args.cmd == "init":
        cmd_init(
            path=Path(args.path),
            lock_seconds=int(args.lock_seconds),
            base_max_workers=int(args.base_max_workers),
            base_retry_budget=int(args.base_retry_budget),
            summary_path=Path(args.summary_path),
        )
    elif args.cmd == "finalize-success":
        cmd_finalize_success(
            path=Path(args.path),
            metrics_path=Path(args.metrics_path),
            summary_path=Path(args.summary_path),
        )
    elif args.cmd == "finalize-failure":
        cmd_finalize_failure(
            path=Path(args.path),
            summary_path=Path(args.summary_path),
        )
    else:
        raise SystemExit(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
