from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Dict, Iterator, List


def read_jsonl_gz(path: str | Path) -> Iterator[Dict]:
    p = Path(path)
    with gzip.open(p, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def peek_jsonl_gz(path: str | Path, n: int = 5) -> List[Dict]:
    out: List[Dict] = []
    for i, obj in enumerate(read_jsonl_gz(path)):
        out.append(obj)
        if i + 1 >= n:
            break
    return out
