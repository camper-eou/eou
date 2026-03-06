from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List


def _read_jsonl_gz(path: str | Path) -> Iterator[Dict]:
    p = Path(path)
    with gzip.open(p, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _peek_jsonl_gz(path: str | Path, n: int = 5) -> List[Dict]:
    out: List[Dict] = []
    for i, obj in enumerate(_read_jsonl_gz(path)):
        out.append(obj)
        if i + 1 >= n:
            break
    return out


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
            sample = _peek_jsonl_gz(p, n=5)
            if sample and all(("typeID" in s and "type" in s) for s in sample):
                return p

    sde = root / "data" / "sde"
    if sde.exists():
        for p in sde.glob("*.jsonl.gz"):
            try:
                sample = _peek_jsonl_gz(p, n=5)
            except Exception:
                continue
            if sample and all(("typeID" in s and "type" in s) for s in sample):
                return p

    raise FileNotFoundError(
        "No types jsonl.gz found containing keys 'typeID' and 'type' under landu_root/data/sde"
    )


def load_types(types_path: str | Path) -> List[TypeRow]:
    rows: List[TypeRow] = []
    for obj in _read_jsonl_gz(types_path):
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
