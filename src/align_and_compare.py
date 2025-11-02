# src/align_and_compare.py
from __future__ import annotations
import json, os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

Number = Optional[float]


class AlignerComparator:
    """Load NBIM/Custody JSONL events, aggregate by event_key, and emit deltas + flags."""

    # stateless helpers 
    @staticmethod
    def _safe_sum(values: List[Number]) -> Number:
        if not values:
            return None
        total = 0.0
        any_non_null = False
        for v in values:
            if v is not None:
                total += float(v)
                any_non_null = True
        return total if any_non_null else None

    @staticmethod
    def _load_jsonl_grouped(path: Path) -> Dict[str, List[Dict[str, Any]]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                k = obj.get("event_key")
                groups.setdefault(k, []).append(obj)
        return groups

    @staticmethod
    def _agg_group(records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Aggregate multiple tranches/rows for the same event_key *within one source*.
        - Sum numeric amounts/positions across records.
        - Carry identifiers/dates/currencies from the first record.
        """
        if not records:
            return None
        base = records[0]

        def g(bucket: str, field: str) -> Number:
            return AlignerComparator._safe_sum([(r.get(bucket, {}) or {}).get(field) for r in records])

        agg = {
            "event_key": base.get("event_key"),
            "instrument": base.get("instrument"),
            "dates": base.get("dates"),
            "currencies": base.get("currencies"),
            "fx": base.get("fx"),
            "rate": base.get("rate"),
            "positions": {
                "nominal_basis": AlignerComparator._safe_sum([(r.get("positions", {}) or {}).get("nominal_basis") for r in records])
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

        # Note if FX looks wrong for same-ccy cases (annotation only)
        q = (agg.get("currencies") or {}).get("quote_ccy")
        s = (agg.get("currencies") or {}).get("settle_ccy")
        fx = (agg.get("fx") or {}).get("quote_to_portfolio_fx")
        if q and s and q == s and fx is not None and abs(float(fx) - 1.0) > 1e-3:
            src = agg.setdefault("source", {})
            note = (src.get("provenance_notes") or "")
            if "fx_suspicious_for_same_ccy" not in note:
                src["provenance_notes"] = (note + " | fx_suspicious_for_same_ccy").strip(" |")

        return agg

    @staticmethod
    def _delta(a: Number, b: Number) -> Number:
        if a is None and b is None:
            return None
        return (b or 0.0) - (a or 0.0)  # custody - nbim

    # --------- main entry point ---------
    def run(self, nbim_events_path: str | Path, custody_events_path: str | Path, out_path: str | Path) -> int:
        nbim_p = Path(nbim_events_path)
        cust_p = Path(custody_events_path)
        out_p  = Path(out_path)

        nbim_groups = self._load_jsonl_grouped(nbim_p)   if nbim_p.exists() else {}
        custody_groups = self._load_jsonl_grouped(cust_p) if cust_p.exists() else {}
        keys = set(nbim_groups.keys()) | set(custody_groups.keys())

        out_p.parent.mkdir(parents=True, exist_ok=True)

        count = 0
        with out_p.open("w", encoding="utf-8") as f:
            for k in keys:
                nb = self._agg_group(nbim_groups.get(k, []))
                cu = self._agg_group(custody_groups.get(k, []))

                derived = {"delta": {}, "flags": []}
                def get_amounts(src, bucket, field):
                    return (src or {}).get(bucket, {}).get(field)

                # Quote-currency deltas
                derived["delta"]["gross_quote"] = self._delta(get_amounts(nb, "amounts_quote", "gross"),
                                                              get_amounts(cu, "amounts_quote", "gross"))
                derived["delta"]["tax_quote"]   = self._delta(get_amounts(nb, "amounts_quote", "tax"),
                                                              get_amounts(cu, "amounts_quote", "tax"))
                derived["delta"]["net_quote"]   = self._delta(get_amounts(nb, "amounts_quote", "net"),
                                                              get_amounts(cu, "amounts_quote", "net"))

                # FX flag only for true cross-ccy
                fx_nb = (nb or {}).get("fx", {}).get("quote_to_portfolio_fx") if nb else None
                fx_cu = (cu or {}).get("fx", {}).get("quote_to_portfolio_fx") if cu else None
                q = ((nb or cu) or {}).get("currencies", {}).get("quote_ccy")
                s = ((nb or cu) or {}).get("currencies", {}).get("settle_ccy")
                if q and s and q != s:
                    if (fx_nb is not None and fx_cu is not None
                            and abs(float(fx_cu) - float(fx_nb)) > 1e-9):
                        derived["flags"].append("fx_mismatch")

                # ADR fee presence
                adr_nb = (nb or {}).get("amounts_quote", {}).get("adr_fee")
                adr_cu = (cu or {}).get("amounts_quote", {}).get("adr_fee")
                if (adr_nb or 0) != (adr_cu or 0):
                    if (adr_nb or 0) != 0 or (adr_cu or 0) != 0:
                        derived["flags"].append("adr_fee_present")

                # Missing tax rate
                tr_nb = (nb or {}).get("rate", {}).get("tax_rate")
                tr_cu = (cu or {}).get("rate", {}).get("tax_rate")
                if tr_nb is None or tr_cu is None:
                    derived["flags"].append("missing_tax_rate")

                out = {"event_key": k, "nbim": nb, "custody": cu, "derived": derived}
                f.write(json.dumps(out) + "\n")
                count += 1

        return count


# api
def align_and_compare(nbim_events_path: str, custody_events_path: str, out_path: str) -> int:
    """
    Back-compat wrapper so callers don't have to change.
    """
    return AlignerComparator().run(nbim_events_path, custody_events_path, out_path)
