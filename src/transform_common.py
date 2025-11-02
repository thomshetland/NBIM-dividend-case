from __future__ import annotations
from decimal import Decimal
from typing import Dict, Any, Tuple

from .map_headers import map_headers
from .normalize import (
    normalize_date, normalize_decimal, normalize_ccy, NormalizationError
)
from .event_key import build_event_key

def row_to_ces(row: Dict[str, Any], source_system: str, colmap: Dict[str, str]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Map a raw row dict to CES dict and collect provenance notes (by field path)."""
    prov: Dict[str, str] = {}

    def put(d: Dict, path: str, val):
        # nest by dotted path
        keys = path.split(".")
        cur = d
        for k in keys[:-1]:
            if k not in cur or not isinstance(cur[k], dict):
                cur[k] = {}
            cur = cur[k]
        cur[keys[-1]] = val

    ces: Dict[str, Any] = {
        "event_key": None,
        "instrument": {},
        "dates": {},
        "currencies": {},
        "fx": {},
        "rate": {},
        "positions": {},
        "amounts_quote": {},
        "amounts_settle": {},
        "source": {
            "system": source_system,
            "file_row_id": str(row.get("__rownum__", "")),
            "provenance_notes": ""
        },
    }

    # iterate mapped columns
    for orig_col, ces_path in colmap.items():
        if not ces_path:
            continue
        raw_val = row.get(orig_col)
        try:
            # choose normalizer by path
            if ces_path.startswith(("dates.")):
                val = normalize_date(raw_val)
            elif ces_path.startswith((
                "amounts_",                        # amounts_quote.*, amounts_settle.*
                "rate.div_per_share",
                "rate.tax_rate",
                "rate.adr_fee_rate",
                "positions.nominal_basis",
                "fx.quote_to_portfolio_fx",
                "amounts_quote.adr_fee"
            )):
                val = normalize_decimal(raw_val)
                if val is not None:
                    val = float(val)  # store as float for JSON simplicity
            elif ces_path.startswith(("currencies.",)):
                val = normalize_ccy(raw_val)
            else:
                # strings or unknown
                val = raw_val if raw_val is None else str(raw_val)
            put(ces, ces_path, val)
        except NormalizationError as e:
            prov[ces_path] = f"normalize_error:{e}"
            put(ces, ces_path, None)

    # Safe derivations
    gross = ces.get("amounts_quote", {}).get("gross")
    net   = ces.get("amounts_quote", {}).get("net")
    tax   = ces.get("amounts_quote", {}).get("tax")
    if isinstance(gross, (int, float)) and isinstance(net, (int, float)) and (tax is None):
        derived_tax = float(gross - net)
        ces["amounts_quote"]["tax"] = derived_tax
        prov["amounts_quote.tax"] = "derived: tax=gross-net"

    q = ces.get("currencies", {}).get("quote_ccy")
    s = ces.get("currencies", {}).get("settle_ccy")
    fx = ces.get("fx", {}).get("quote_to_portfolio_fx")
    if fx is None and q and s and q == s:
        ces["fx"]["quote_to_portfolio_fx"] = 1.0
        prov["fx.quote_to_portfolio_fx"] = "default: 1.0 (same ccy)"

    # Build event key: prefer vendor id if present
    vendor = ces.get("source", {}).get("vendor_event_key")
    if vendor and str(vendor).strip():
        ces["event_key"] = str(vendor).strip()
    else:
        isin  = ces.get("instrument", {}).get("isin")
        exd   = ces.get("dates", {}).get("ex_date")
        pay   = ces.get("dates", {}).get("pay_date")
        quote = ces.get("currencies", {}).get("quote_ccy")
        ces["event_key"] = build_event_key(isin, exd, pay, quote)

    # Attach provenance
    ces["source"]["provenance_notes"] = "; ".join([f"{k}:{v}" for k, v in prov.items()])
    return ces, prov
