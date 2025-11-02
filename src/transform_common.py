# src/transform_common.py
from __future__ import annotations
from decimal import Decimal
from typing import Dict, Any, Tuple

from .normalize import Normalizer
from .event_key import build_event_key


def row_to_ces(
    row: Dict[str, Any],
    source_system: str,
    colmap: Dict[str, str],
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    Map a raw row dict to a CES dict and collect provenance notes (by field path).
    Uses Normalizer for all conversions and safe derivations.
    """
    prov: Dict[str, str] = {}

    def put(d: Dict, path: str, val):
        """Create nested dicts along a dotted path and assign the value."""
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
            "provenance_notes": "",
        },
    }

    # Deterministic field mapping + normalization 
    for orig_col, ces_path in colmap.items():
        if not ces_path:
            continue
        raw_val = row.get(orig_col)

        # Choose normalizer by CES path
        if ces_path.startswith("dates."):
            val = Normalizer.normalize_date(raw_val)

        elif ces_path.startswith((
            "amounts_",                  # amounts_quote.*, amounts_settle.*
            "rate.div_per_share",
            "rate.tax_rate",
            "rate.adr_fee_rate",
            "positions.nominal_basis",
            "fx.quote_to_portfolio_fx",
            "amounts_quote.adr_fee",
        )):
            dec = Normalizer.normalize_decimal(raw_val)
            val = float(dec) if dec is not None else None  # store as JSON-friendly float

        elif ces_path.startswith("currencies."):
            val = Normalizer.normalize_ccy(raw_val)

        else:
            # Strings or unknowns: stringify if present
            val = raw_val if raw_val is None else str(raw_val)

        put(ces, ces_path, val)

    # Derive amounts_quote.tax if missing and we have gross/net
    g = ces.get("amounts_quote", {}).get("gross")
    n = ces.get("amounts_quote", {}).get("net")
    t = ces.get("amounts_quote", {}).get("tax")

    # Convert to Decimal for accurate math
    g_dec = Decimal(str(g)) if isinstance(g, (int, float)) else None
    n_dec = Decimal(str(n)) if isinstance(n, (int, float)) else None
    t_dec = Decimal(str(t)) if isinstance(t, (int, float)) else None

    t_new, t_note = Normalizer.derive_missing_tax(g_dec, n_dec, t_dec)
    if t_new is not None and t is None:
        ces["amounts_quote"]["tax"] = float(t_new)
        if t_note:
            prov["amounts_quote.tax"] = t_note

    # Default FX = 1.0 when quote_ccy == settle_ccy and FX missing
    q = ces.get("currencies", {}).get("quote_ccy")
    s = ces.get("currencies", {}).get("settle_ccy")
    fx_val = ces.get("fx", {}).get("quote_to_portfolio_fx")
    fx_dec = Decimal(str(fx_val)) if isinstance(fx_val, (int, float)) else None

    fx_new, fx_note = Normalizer.default_fx_if_same_ccy(q, s, fx_dec)
    if fx_new is not None and fx_val is None:
        ces["fx"]["quote_to_portfolio_fx"] = float(fx_new)
        if fx_note:
            prov["fx.quote_to_portfolio_fx"] = fx_note

    # Stable event_key 
    vendor = ces.get("source", {}).get("vendor_event_key")
    if vendor and str(vendor).strip():
        ces["event_key"] = str(vendor).strip()
    else:
        isin  = ces.get("instrument", {}).get("isin")
        exd   = ces.get("dates", {}).get("ex_date")
        pay   = ces.get("dates", {}).get("pay_date")
        quote = ces.get("currencies", {}).get("quote_ccy")
        ces["event_key"] = build_event_key(isin, exd, pay, quote)

    # Attach provenance notes 
    ces["source"]["provenance_notes"] = "; ".join(f"{k}:{v}" for k, v in prov.items())

    return ces, prov
