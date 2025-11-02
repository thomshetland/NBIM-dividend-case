# src/agents/tools.py
from typing import Optional, Dict, Any
from src.transform import Transformer
from src.align_and_compare import AlignerComparator
from src.normalize import Normalizer

def compute_delta(nb: Optional[float], cu: Optional[float]) -> Optional[float]:
    """Return cu - nb (None-safe)."""
    if nb is None and cu is None: return None
    return (cu or 0.0) - (nb or 0.0)

def check_fx_rule(quote_ccy: str|None, settle_ccy: str|None,
                  fx_nb: float|None, fx_cu: float|None) -> Dict[str, Any]:
    """Return {flag: bool, reason: str} for fx_mismatch rule."""
    if quote_ccy and settle_ccy and quote_ccy != settle_ccy:
        if fx_nb is not None and fx_cu is not None and abs(fx_nb - fx_cu) > 1e-9:
            return {"flag": True, "reason": "fx differs in cross-ccy"}
    return {"flag": False, "reason": ""}

def check_tax_presence(tax_rate_nb: float|None, tax_rate_cu: float|None) -> Dict[str, Any]:
    missing = tax_rate_nb is None or tax_rate_cu is None
    return {"flag": missing, "reason": "missing tax_rate on one side" if missing else ""}

def propose_resolution(delta_net_quote: float|None,
                       adr_fee_nb: float|None, adr_fee_cu: float|None) -> Dict[str, Any]:
    """
    Return a *candidate* journal or action with rationale. Purely advisory.
    """
    out: Dict[str, Any] = {"action": "none", "rationale": ""}
    if (adr_fee_nb or 0) != (adr_fee_cu or 0):
        out["action"] = "book_adr_fee_adjustment"
        out["rationale"] = "ADR fee asymmetry explains net delta"
    elif delta_net_quote and abs(delta_net_quote) > 0:
        out["action"] = "investigate_delta"
        out["rationale"] = "Unexplained net delta remains"
    return out
