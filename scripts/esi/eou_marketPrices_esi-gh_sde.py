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


@dataclass(frozen=True)
class RegionRow:
    region_id: int
    region_name: str


def load_regions(landu_root: str | Path) -> List[RegionRow]:
    p = Path(landu_root) / "data" / "sde" / "regions.jsonl.gz"
    regions: List[RegionRow] = []
    for obj in _read_jsonl_gz(p):
        regions.append(RegionRow(int(obj["regionID"]), str(obj["region"])))
    regions.sort(key=lambda r: r.region_id)
    return regions


def load_stations_map(landu_root: str | Path) -> Dict[int, str]:
    p = Path(landu_root) / "data" / "sde" / "stations.jsonl.gz"
    out: Dict[int, str] = {}
    for obj in _read_jsonl_gz(p):
        out[int(obj["stationID"])] = str(obj["station"])
    return out


def load_structures_map(landu_root: str | Path) -> Dict[int, str]:
    p = Path(landu_root) / "data" / "esi" / "structures.jsonl.gz"
    out: Dict[int, str] = {}
    for obj in _read_jsonl_gz(p):
        out[int(obj["stationID"])] = str(obj["station"])
    return out
