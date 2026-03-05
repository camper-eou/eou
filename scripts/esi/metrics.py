from __future__ import annotations

from typing import List, Optional, Tuple

MAD_TO_SIGMA = 1.4826
TAU = 3.0
BETA = 0.10
G_MIN = 0.50


def _median(xs: List[float]) -> Optional[float]:
    n = len(xs)
    if n == 0:
        return None
    xs = sorted(xs)
    mid = n // 2
    if n % 2 == 1:
        return float(xs[mid])
    return (xs[mid - 1] + xs[mid]) / 2.0


def _vwap(entries: List[Tuple[float, int]]) -> Optional[float]:
    num = 0.0
    den = 0
    for p, u in entries:
        if u <= 0:
            continue
        num += float(p) * int(u)
        den += int(u)
    if den <= 0:
        return None
    return num / den


def _classify_ifm(ifm: float) -> str:
    if ifm >= 0.75:
        return "secure"
    if ifm >= 0.50:
        return "normal"
    if ifm >= 0.25:
        return "weak"
    return "insecure"


def compute_buy(entries: List[Tuple[float, int]]) -> dict:
    total_units = sum(int(u) for _, u in entries if u > 0)
    if total_units <= 0:
        return {"price": None, "units": 0, "totalUnits": 0, "ifm": 0.0, "ifmName": "insecure"}

    prices = [float(p) for p, u in entries if u > 0]
    raw_vwap = _vwap(entries)

    med = _median(prices)
    sigma_rob = None
    if med is not None:
        devs = [abs(p - med) for p in prices]
        mad = _median(devs) or 0.0
        sigma_rob = MAD_TO_SIGMA * mad

    L = None
    if med is not None and sigma_rob is not None:
        L = med - TAU * sigma_rob

    highliner_prices = set()
    g = 0.0
    if L is not None and L <= 0:
        uniq = sorted(set(prices), reverse=True)
        if len(uniq) >= 2 and uniq[1] > 0:
            p1, p2 = uniq[0], uniq[1]
            g = (p1 - p2) / p2
            if g >= G_MIN:
                highliner_prices.add(p1)

    dirty_vwap = _vwap([(p, u) for p, u in entries if float(p) not in highliner_prices]) if highliner_prices else raw_vwap

    entries_wo_high = [(float(p), int(u)) for p, u in entries if int(u) > 0 and float(p) not in highliner_prices]
    prices_wo_high = [p for p, _ in entries_wo_high]

    med2 = _median(prices_wo_high)
    sigma2 = None
    if med2 is not None:
        devs2 = [abs(p - med2) for p in prices_wo_high]
        mad2 = _median(devs2) or 0.0
        sigma2 = MAD_TO_SIGMA * mad2

    L1 = 0.0
    if med2 is not None and sigma2 is not None:
        L1 = med2 - TAU * sigma2
        if L1 < 0:
            L1 = 0.0

    U_beta = BETA * total_units
    lowliner_prices = {p for p, u in entries_wo_high if p < L1 and u > U_beta}

    clean_entries = [(float(p), int(u)) for p, u in entries if int(u) > 0 and float(p) not in highliner_prices and float(p) not in lowliner_prices]
    clean_vwap = _vwap(clean_entries)

    if clean_vwap is None or clean_vwap <= 0:
        return {"price": None, "units": 0, "totalUnits": int(total_units), "ifm": 0.0, "ifmName": "insecure"}

    units = sum(u for p, u in entries if u > 0 and p >= clean_vwap and float(p) not in lowliner_prices)

    credible_units = sum(u for _, u in clean_entries)
    PR = credible_units / total_units if total_units else 0.0

    max_u = max((u for _, u in entries if u > 0), default=0)
    CR = max_u / total_units if total_units else 1.0

    dv = 1.0
    if raw_vwap is not None and dirty_vwap is not None and clean_vwap > 0:
        dv1 = abs(raw_vwap - dirty_vwap) / clean_vwap
        dv2 = abs(dirty_vwap - clean_vwap) / clean_vwap
        dv = max(dv1, dv2)
    dv = min(dv, 1.0)

    GN = (g / G_MIN) if highliner_prices else 0.0
    GN = min(GN, 1.0)

    ifm = PR * (1.0 - CR) * (1.0 - dv) * (1.0 - GN)
    if ifm < 0:
        ifm = 0.0

    return {"price": float(clean_vwap), "units": int(units), "totalUnits": int(total_units), "ifm": float(ifm), "ifmName": _classify_ifm(ifm)}


def compute_sell(entries: List[Tuple[float, int]]) -> dict:
    total_units = sum(int(u) for _, u in entries if u > 0)
    if total_units <= 0:
        return {"price": None, "units": 0, "totalUnits": 0, "ifm": 0.0, "ifmName": "insecure"}

    prices = [float(p) for p, u in entries if u > 0]
    raw_vwap = _vwap(entries)

    med = _median(prices)
    sigma_rob = None
    if med is not None:
        devs = [abs(p - med) for p in prices]
        mad = _median(devs) or 0.0
        sigma_rob = MAD_TO_SIGMA * mad

    U = None
    if med is not None and sigma_rob is not None:
        U = med + TAU * sigma_rob

    lowliner_prices = set()
    g_low = 0.0
    if U is not None:
        uniq = sorted(set(prices))
        if len(uniq) >= 2 and uniq[1] > 0:
            p1, p2 = uniq[0], uniq[1]
            U_non_info = (U >= 10.0 * p2)
            if U_non_info:
                g_low = (p2 - p1) / p2
                if g_low >= G_MIN:
                    lowliner_prices.add(p1)

    dirty_vwap = _vwap([(p, u) for p, u in entries if float(p) not in lowliner_prices]) if lowliner_prices else raw_vwap

    entries_wo_low = [(float(p), int(u)) for p, u in entries if int(u) > 0 and float(p) not in lowliner_prices]
    prices_wo_low = [p for p, _ in entries_wo_low]

    med2 = _median(prices_wo_low)
    sigma2 = None
    if med2 is not None:
        devs2 = [abs(p - med2) for p in prices_wo_low]
        mad2 = _median(devs2) or 0.0
        sigma2 = MAD_TO_SIGMA * mad2

    U1 = (med2 + TAU * sigma2) if (med2 is not None and sigma2 is not None) else None
    U_beta = BETA * total_units

    highliner_prices = set()
    if U1 is not None:
        for p, u in entries_wo_low:
            if p > U1 and u > U_beta:
                highliner_prices.add(p)

    clean_entries = [(float(p), int(u)) for p, u in entries if int(u) > 0 and float(p) not in lowliner_prices and float(p) not in highliner_prices]
    clean_vwap = _vwap(clean_entries)

    if clean_vwap is None or clean_vwap <= 0:
        return {"price": None, "units": 0, "totalUnits": int(total_units), "ifm": 0.0, "ifmName": "insecure"}

    units = sum(u for p, u in entries if u > 0 and p <= clean_vwap and float(p) not in lowliner_prices)

    credible_units = sum(u for _, u in clean_entries)
    PR = credible_units / total_units if total_units else 0.0

    max_u = max((u for _, u in entries if u > 0), default=0)
    CR = max_u / total_units if total_units else 1.0

    dv = 1.0
    if raw_vwap is not None and dirty_vwap is not None and clean_vwap > 0:
        dv1 = abs(raw_vwap - dirty_vwap) / clean_vwap
        dv2 = abs(dirty_vwap - clean_vwap) / clean_vwap
        dv = max(dv1, dv2)
    dv = min(dv, 1.0)

    GN = (g_low / G_MIN) if lowliner_prices else 0.0
    GN = min(GN, 1.0)

    ifm = PR * (1.0 - CR) * (1.0 - dv) * (1.0 - GN)
    if ifm < 0:
        ifm = 0.0

    return {"price": float(clean_vwap), "units": int(units), "totalUnits": int(total_units), "ifm": float(ifm), "ifmName": _classify_ifm(ifm)}
