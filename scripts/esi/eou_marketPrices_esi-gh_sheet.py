#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import google.auth
from googleapiclient.discovery import build

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


def parse_iso_z(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_sheets_serial(dt: datetime) -> float:
    """
    Convierte datetime UTC a serial numérico de Google Sheets / Excel.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    epoch_seconds = dt.timestamp()
    return epoch_seconds / 86400.0 + 25569.0


def batch_update_values(spreadsheet_id: str, data: list[dict]) -> None:
    creds, _ = google.auth.default(scopes=[SHEETS_SCOPE])
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    body = {
        "valueInputOption": "USER_ENTERED",
        "data": data,
    }

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body,
    ).execute()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spreadsheet-id", required=True)
    ap.add_argument("--sheet-tab", required=True)
    ap.add_argument("--row", required=True)
    ap.add_argument("--summary-path", required=True)
    args = ap.parse_args()

    summary = json.loads(Path(args.summary_path).read_text(encoding="utf-8"))
    row = str(args.row)
    tab = str(args.sheet_tab)

    status = str(summary["status"])
    next_run_iso = str(summary["next_run"])
    next_run_dt = parse_iso_z(next_run_iso)
    next_run_serial = to_sheets_serial(next_run_dt)

    data = [
        {
            "range": f"{tab}!B{row}",
            "values": [[status]],
        },
        {
            "range": f"{tab}!D{row}",
            "values": [[next_run_serial]],
        },
    ]

    if bool(summary.get("write_last_modified")) and summary.get("last_modified"):
        lm_dt = parse_iso_z(str(summary["last_modified"]))
        lm_serial = to_sheets_serial(lm_dt)
        data.append(
            {
                "range": f"{tab}!I{row}",
                "values": [[lm_serial]],
            }
        )

    batch_update_values(args.spreadsheet_id, data)


if __name__ == "__main__":
    main()
