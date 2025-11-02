import json
import os
from typing import Dict, Any, List, Optional

def _safe_sum(values: List[Optional[float]]) -> Optional[float]:
    if not values:
        return None
    total = 0.0
    any_non_null = False
    for v in values:
        if v is not None:
            total += float(v)
            any_non_null = True
    return total if any_non_null else None

def _load_jsonl_grouped(path: str) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            k = obj.get("event_key")
            groups.setdefault(k, []).append(obj)
    return groups

def _agg_group(records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Aggregate multiple tranches/rows for the same event_key *within one source*.
    - Sum numeric amounts/positions across records.
    - Take identifiers/dates/currencies from the first record.
    """
    if not records:
        return None
    base = records[0]

    def g(bucket: str, field: str) -> Optional[float]:
        return _safe_sum([(r.get(bucket, {}) or {}).get(field) for r in records])

    agg = {
        "event_key": base.get("event_key"),
        "instrument": base.get("instrument"),
        "dates": base.get("dates"),
        "currencies": base.get("currencies"),
        "fx": base.get("fx"),
        "rate": base.get("rate"),
        "positions": {
            "nominal_basis": _safe_sum([(r.get("positions", {}) or {}).get("nominal_basis") for r in records])
        },
        "amounts_quote": {
            "gross": g("amounts_quote", "gross"),
            "tax":   g("amounts_quote", "tax"),
            "net":   g("amounts_quote", "net"),
            "adr_fee": g("amounts_quote", "adr_fee"),
        },
        "amounts_settle": {
            "gross": g("amounts_settle", "gross"),
            "tax":   g("amounts_settle", "tax"),
            "net":   g("amounts_settle", "net"),
        },
        "source": base.get("source"),
    }

    # If same-ccy but FX is far from 1.0, annotate provenance (suspicious) — keep as note only.
    q = (agg.get("currencies") or {}).get("quote_ccy")
    s = (agg.get("currencies") or {}).get("settle_ccy")
    fx = (agg.get("fx") or {}).get("quote_to_portfolio_fx")
    if q and s and q == s and fx is not None and abs(float(fx) - 1.0) > 1e-3:
        src = agg.setdefault("source", {})
        note = (src.get("provenance_notes") or "")
        if "fx_suspicious_for_same_ccy" not in note:
            src["provenance_notes"] = (note + " | fx_suspicious_for_same_ccy").strip(" |")

    return agg

def _delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None and b is None:
        return None
    a = a or 0.0
    b = b or 0.0
    return b - a  # custody - nbim

def align_and_compare(nbim_events_path: str, custody_events_path: str, out_path: str) -> int:
    nbim_groups = _load_jsonl_grouped(nbim_events_path) if os.path.exists(nbim_events_path) else {}
    custody_groups = _load_jsonl_grouped(custody_events_path) if os.path.exists(custody_events_path) else {}

    keys = set(nbim_groups.keys()) | set(custody_groups.keys())

    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for k in keys:
            nb = _agg_group(nbim_groups.get(k, []))
            cu = _agg_group(custody_groups.get(k, []))

            derived = {"delta": {}, "flags": []}

            def get_amounts(src, bucket, field):
                return (src or {}).get(bucket, {}).get(field)

            # Compare quote-currency amounts
            gross_nb = get_amounts(nb, "amounts_quote", "gross")
            tax_nb   = get_amounts(nb, "amounts_quote", "tax")
            net_nb   = get_amounts(nb, "amounts_quote", "net")

            gross_cu = get_amounts(cu, "amounts_quote", "gross")
            tax_cu   = get_amounts(cu, "amounts_quote", "tax")
            net_cu   = get_amounts(cu, "amounts_quote", "net")

            derived["delta"]["gross_quote"] = _delta(gross_nb, gross_cu)
            derived["delta"]["tax_quote"]   = _delta(tax_nb,   tax_cu)
            derived["delta"]["net_quote"]   = _delta(net_nb,   net_cu)

            # FX flags: only compare when quote_ccy != settle_ccy (true cross-currency).
            fx_nb = (nb or {}).get("fx", {}).get("quote_to_portfolio_fx") if nb else None
            fx_cu = (cu or {}).get("fx", {}).get("quote_to_portfolio_fx") if cu else None
            q = ((nb or cu) or {}).get("currencies", {}).get("quote_ccy")
            s = ((nb or cu) or {}).get("currencies", {}).get("settle_ccy")
            if q and s and q != s:
                if (fx_nb is not None and fx_cu is not None
                        and abs(float(fx_cu) - float(fx_nb)) > 1e-9):
                    derived["flags"].append("fx_mismatch")
            # If quote==settle, intentionally do not add FX flags.

            # ADR fee present on either side
            adr_nb = (nb or {}).get("amounts_quote", {}).get("adr_fee")
            adr_cu = (cu or {}).get("amounts_quote", {}).get("adr_fee")
            if (adr_nb or 0) != (adr_cu or 0):
                if (adr_nb or 0) != 0 or (adr_cu or 0) != 0:
                    derived["flags"].append("adr_fee_present")

            # Missing tax rate flag
            tr_nb = (nb or {}).get("rate", {}).get("tax_rate")
            tr_cu = (cu or {}).get("rate", {}).get("tax_rate")
            if tr_nb is None or tr_cu is None:
                derived["flags"].append("missing_tax_rate")

            out = {
                "event_key": k,
                "nbim": nb,
                "custody": cu,
                "derived": derived,
            }
            f.write(json.dumps(out) + "\n")
            count += 1

    return count
