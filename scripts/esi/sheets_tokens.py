from __future__ import annotations

from typing import List

import google.auth
from googleapiclient.discovery import build

SHEETS_READONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"


def read_tokens_from_sheet(spreadsheet_id: str, range_a1: str) -> List[str]:
    # Uses Application Default Credentials provided by google-github-actions/auth (WIF/OIDC)
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

    # D12 → ... → D2
    tokens.reverse()
    return tokens
