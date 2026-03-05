from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from io_utils import peek_jsonl_gz, read_jsonl_gz


@dataclass(frozen=True)
class TypeRow:
    type_id: int
    type_name: str


CANDIDATES = [
    "data/sde/types.jsonl.gz",
    "data/sde/typeIDs.jsonl.gz",
    "data/sde/items.jsonl.gz",
    "data/sde/types_min.jsonl.gz",
]


def detect_types_file(landu_root: str | Path) -> Path:
    root = Path(landu_root)

    for rel in CANDIDATES:
        p = root / rel
        if p.exists():
            sample = peek_jsonl_gz(p, n=5)
            if sample and all(("typeID" in s and "type" in s) for s in sample):
                return p

    sde = root / "data" / "sde"
    if sde.exists():
        for p in sde.glob("*.jsonl.gz"):
            try:
                sample = peek_jsonl_gz(p, n=5)
            except Exception:
                continue
            if sample and all(("typeID" in s and "type" in s) for s in sample):
                return p

    raise FileNotFoundError(
        "No types jsonl.gz found containing keys 'typeID' and 'type' under landu_root/data/sde"
    )


def load_types(types_path: str | Path) -> List[TypeRow]:
    rows: List[TypeRow] = []
    for obj in read_jsonl_gz(types_path):
        if "typeID" not in obj or "type" not in obj:
            continue
        try:
            tid = int(obj["typeID"])
            name = str(obj["type"])
        except Exception:
            continue
        rows.append(TypeRow(tid, name))

    rows.sort(key=lambda r: r.type_id)
    return rows
