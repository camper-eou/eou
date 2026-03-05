from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from io_utils import read_jsonl_gz


@dataclass(frozen=True)
class RegionRow:
    region_id: int
    region_name: str


def load_regions(landu_root: str | Path) -> List[RegionRow]:
    p = Path(landu_root) / "data" / "sde" / "regions.jsonl.gz"
    regions: List[RegionRow] = []
    for obj in read_jsonl_gz(p):
        regions.append(RegionRow(int(obj["regionID"]), str(obj["region"])))
    regions.sort(key=lambda r: r.region_id)
    return regions


def load_stations_map(landu_root: str | Path) -> Dict[int, str]:
    p = Path(landu_root) / "data" / "sde" / "stations.jsonl.gz"
    out: Dict[int, str] = {}
    for obj in read_jsonl_gz(p):
        out[int(obj["stationID"])] = str(obj["station"])
    return out


def load_structures_map(landu_root: str | Path) -> Dict[int, str]:
    p = Path(landu_root) / "data" / "esi" / "structures.jsonl.gz"
    out: Dict[int, str] = {}
    for obj in read_jsonl_gz(p):
        out[int(obj["stationID"])] = str(obj["station"])
    return out
