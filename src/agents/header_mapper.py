# src/agents/header_mapper.py
from __future__ import annotations
from typing import List, Dict, Any, Optional
from pathlib import Path
import os, json
import pandas as pd
import anthropic

from src.normalize import normalize_date, normalize_decimal, normalize_ccy, NormalizationError
from src.transform import Transformer
from src.map_headers import map_headers

# ---- Allowed CES targets and expected types ----
CES_TARGETS: Dict[str, str] = {
    "dates.ex_date": "date",
    "dates.pay_date": "date",
    "dates.record_date": "date",
    "currencies.quote_ccy": "ccy",
    "currencies.settle_ccy": "ccy",
    "fx.quote_to_portfolio_fx": "number",
    "rate.div_per_share": "number",
    "rate.tax_rate": "number",
    "rate.adr_fee_rate": "number",
    "positions.nominal_basis": "number",
    "amounts_quote.gross": "number",
    "amounts_quote.tax": "number",
    "amounts_quote.net": "number",
    "amounts_quote.adr_fee": "number",
    "amounts_settle.gross": "number",
    "amounts_settle.tax": "number",
    "amounts_settle.net": "number",
    "source.event_type": "string",
    "source.custodian": "string",
    "source.bank_account": "string",
    "source.restitution.rate": "number",
    "source.restitution.possible_payment": "string",
    "source.restitution.possible_amount": "number",
}

def _validate_values(values: List[Any], ces_type: str) -> float:
    """Return pass rate [0..1] by running normalizers on a small sample (informational only)."""
    if not values:
        return 0.0
    ok = 0
    total = 0
    for v in values:
        total += 1
        try:
            if ces_type == "date":
                _ = normalize_date(v)
            elif ces_type == "number":
                _ = normalize_decimal(v)
            elif ces_type == "ccy":
                _ = normalize_ccy(v)
            elif ces_type == "string":
                _ = str(v) if v is not None else ""
            else:
                raise NormalizationError("unknown type")
            ok += 1
        except NormalizationError:
            pass
    return ok / max(total, 1)

class HeaderMapper:
    """LLM-assisted header-mapping with file I/O baked in (replaces suggest_mappings + run)."""

    def __init__(self, model: Optional[str], api_key: Optional[str] = None,
                 accept_threshold_conf: float = 0.80):
        self.model = model
        self.accept_threshold_conf = float(accept_threshold_conf)
        api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("Missing ANTHROPIC_API_KEY for header mapper.")
        self.client = anthropic.Anthropic(api_key=api_key)

        # Single tool schema
        self.tools: List[Dict[str, Any]] = [{
            "name": "propose_header_mapping",
            "description": "Map a single CSV header to exactly one allowed CES target with confidence.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "header": {"type": "string"},
                    "candidate": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "reason": {"type": "string"}
                },
                "required": ["header", "candidate", "confidence"]
            }
        }]
        self.system = (
            "You map ONE CSV header to ONE allowed CES target.\n"
            "Use only the allowed list. Consider header semantics AND sample values.\n"
            "Always respond by calling the tool."
        )

    # ---------- core ----------
    def _llm_propose(self, header: str, values: List[Any]) -> Dict[str, Any]:
        if not self.model or not self.client:
            # Heuristics-only mode: no LLM → return empty candidate
            return {"header": header, "candidate": None, "confidence": 0.0, "reason": "no_llm"}
        user_msg = (
            f"HEADER: {header}\n"
            f"SAMPLE_VALUES: {json.dumps(values[:15], ensure_ascii=False)}\n"
            f"ALLOWED_TARGETS: {json.dumps(list(CES_TARGETS.keys()))}"
        )
        msg = self.client.messages.create(
            model=self.model,
            max_tokens=400,
            system=self.system,
            tools=self.tools,
            messages=[{"role": "user", "content": user_msg}],
        )
        for part in msg.content:
            if getattr(part, "type", None) == "tool_use" and part.name == "propose_header_mapping":
                data = part.input or {}
                return {
                    "header": data.get("header", header),
                    "candidate": data.get("candidate"),
                    "confidence": float(data.get("confidence", 0.0)),
                    "reason": data.get("reason", "")
                }
        raise RuntimeError("Model did not use propose_header_mapping")

    def suggest_mappings(self, df: pd.DataFrame, unmapped_headers: List[str]) -> Dict[str, Dict[str, Any]]:
        """Pure mapping suggestions over a dataframe and a header list."""
        out: Dict[str, Dict[str, Any]] = {}
        for h in unmapped_headers:
            sample_vals = [v for v in df[h].dropna().tolist()[:25]] if h in df.columns else []
            try:
                prop = self._llm_propose(h, sample_vals)
            except Exception as e:
                prop = {"candidate": None, "confidence": 0.0, "reason": f"llm_error: {e}"}

            cand = prop.get("candidate")
            conf = float(prop.get("confidence", 0.0))
            ces_type = CES_TARGETS.get(cand, "string") if cand else "string"
            pass_rate = _validate_values(sample_vals, ces_type) if cand else 0.0
            accepted = (cand is not None) and (conf >= self.accept_threshold_conf)

            out[h] = {
                "candidate": cand,
                "confidence": round(conf, 3),
                "pass_rate": round(pass_rate, 3),
                "final_score": round(conf, 3),  # keep UI compatibility
                "accepted": bool(accepted),
                "reason": prop.get("reason", "")
            }
        return out

    # api
    def run(self, nbim_csv: str | Path, custody_csv: str | Path, mapping_path: str | Path) -> Dict[str, Dict[str, Any]]:
        """Load CSVs, compute unmapped, call suggest_mappings, and write patch files."""
        nbim_csv, custody_csv, mapping_path = Path(nbim_csv), Path(custody_csv), Path(mapping_path)

        # Reuse the single robust reader for file paths
        nbim = Transformer.robust_read_csv(str(nbim_csv))
        custody = Transformer.robust_read_csv(str(custody_csv))

        nbim_colmap = map_headers(list(nbim.columns))
        custody_colmap = map_headers(list(custody.columns))
        unmapped = sorted({h for h, p in nbim_colmap.items() if not p} |
                          {h for h, p in custody_colmap.items() if not p})

        cols = [c for c in unmapped if c in nbim.columns or c in custody.columns]
        union_df = pd.concat(
            [nbim.reindex(columns=cols, fill_value=None), custody.reindex(columns=cols, fill_value=None)],
            axis=0, ignore_index=True
        )

        suggestions = self.suggest_mappings(union_df, unmapped)

        mapping_path.parent.mkdir(parents=True, exist_ok=True)
        mapping_path.write_text(json.dumps({"suggestions": suggestions}, indent=2), encoding="utf-8")

        accepted_map = {h: v["candidate"] for h, v in suggestions.items() if v["accepted"]}
        accepted_path = mapping_path.with_suffix(".accepted.json")
        accepted_path.write_text(json.dumps({"accepted": accepted_map}, indent=2), encoding="utf-8")

        return suggestions
