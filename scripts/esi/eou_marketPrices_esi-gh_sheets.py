from __future__ import annotations

import random
import time
from typing import List

import google.auth
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build

SHEETS_READONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"


def read_tokens_from_sheet(spreadsheet_id: str, range_a1: str) -> List[str]:
    """
    Lee los access tokens de estructuras desde Google Sheets.

    Reglas ya integradas:
      - filtra vacíos
      - trim
      - invierte el orden para devolver D12 → ... → D2
    """
    max_attempts = 6
    base_sleep = 2.0
    last_err: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            creds, _ = google.auth.default(scopes=[SHEETS_READONLY_SCOPE])
            service = build("sheets", "v4", credentials=creds, cache_discovery=False)

            resp = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=range_a1)
                .execute()
            )

            values = resp.get("values", [])
            tokens: List[str] = []

            for row in values:
                if not row:
                    continue
                tok = str(row[0]).strip()
                if tok:
                    tokens.append(tok)

            tokens.reverse()
            return tokens

        except RefreshError as e:
            last_err = e
        except Exception as e:
            last_err = e

        if attempt < max_attempts:
            sleep_s = (base_sleep * (2 ** (attempt - 1))) + random.uniform(0.0, 1.0)
            time.sleep(sleep_s)

    raise RuntimeError(
        f"Failed to read tokens from Google Sheets after {max_attempts} attempts: {last_err}"
    )
