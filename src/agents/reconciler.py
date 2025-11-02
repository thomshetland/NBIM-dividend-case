# src/agents/reconciler.py
import os, json, anthropic
from typing import Optional
from src.agents.rec_tools import load_and_align, compute_delta, check_fx_rule, check_tax_presence, propose_resolution
TOOLS = [
  {
    "name": "compute_delta",
    "description": "Return custody-minus-nbim for a numeric field.",
    "input_schema": {"type":"object","properties":{"nb":{"type":["number","null"]},"cu":{"type":["number","null"]}},"required":["nb","cu"]}
  },
  {
    "name": "check_fx_rule",
    "description": "Flag fx_mismatch when cross-ccy and FX differs.",
    "input_schema": {"type":"object","properties":{
      "quote_ccy":{"type":["string","null"]},"settle_ccy":{"type":["string","null"]},
      "fx_nb":{"type":["number","null"]},"fx_cu":{"type":["number","null"]}
    },"required":["quote_ccy","settle_ccy","fx_nb","fx_cu"]}
  },
  {
    "name": "check_tax_presence",
    "description": "Flag missing tax_rate.",
    "input_schema": {"type":"object","properties":{"tax_rate_nb":{"type":["number","null"]},"tax_rate_cu":{"type":["number","null"]}},"required":["tax_rate_nb","tax_rate_cu"]}
  },
  {
    "name": "propose_resolution",
    "description": "Suggest a candidate action with rationale.",
    "input_schema": {"type":"object","properties":{
      "delta_net_quote":{"type":["number","null"]},"adr_fee_nb":{"type":["number","null"]},"adr_fee_cu":{"type":["number","null"]}
    },"required":["delta_net_quote","adr_fee_nb","adr_fee_cu"]}
  }
]

SYSTEM = (
  "You are a reconciliation agent. You MUST use tools for all calculations. "
  "Work per event_key from comparison_frame.jsonl. For each, compute deltas, set flags, "
  "and propose a candidate resolution. Never invent data; only use tool outputs. "
  "Return only JSON with fields: {event_key, deltas, flags, resolution}."
)

class ReconcilerAgent:
    def __init__(self, model: str):
        self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        self.model = model

    def run(self, nbim_csv: str, custody_csv: str, out_jsonl: str):
        # 1) Load & align via tool
        msg = self.client.messages.create(
            model=self.model, system=SYSTEM, tools=TOOLS, max_tokens=1000,
            messages=[{"role":"user","content": json.dumps({"task":"reconcile","nbim_csv":nbim_csv,"custody_csv":custody_csv})}]
        )
        # Handle tool call to load_and_align, then iterate comparison_frame.jsonl yourself:
        # For brevity, call the Python function directly here; in production, wire via a tool server.

        
        cmp_path = load_and_align(nbim_csv, custody_csv)

        # Iterate events and ask the model to orchestrate calls per record OR do direct tool calls and let LLM summarize.
        results = []
        with open(cmp_path, "r", encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                ek = item["event_key"]
                nb, cu = item.get("nbim") or {}, item.get("custody") or {}
                dr = item.get("derived") or {}

                # Example: do deterministic tool calls here (fast)…
                d_g = compute_delta(
                    (nb.get("amounts_quote") or {}).get("gross"),
                    (cu.get("amounts_quote") or {}).get("gross"),
                )
                d_t = compute_delta(
                    (nb.get("amounts_quote") or {}).get("tax"),
                    (cu.get("amounts_quote") or {}).get("tax"),
                )
                d_n = compute_delta(
                    (nb.get("amounts_quote") or {}).get("net"),
                    (cu.get("amounts_quote") or {}).get("net"),
                )

                fx_flag = check_fx_rule(
                    ((nb.get("currencies") or {}) or (cu.get("currencies") or {})).get("quote_ccy"),
                    ((nb.get("currencies") or {}) or (cu.get("currencies") or {})).get("settle_ccy"),
                    (nb.get("fx") or {}).get("quote_to_portfolio_fx"),
                    (cu.get("fx") or {}).get("quote_to_portfolio_fx"),
                )
                tax_flag = check_tax_presence(
                    (nb.get("rate") or {}).get("tax_rate"),
                    (cu.get("rate") or {}).get("tax_rate"),
                )
                res = propose_resolution(
                    d_n,
                    (nb.get("amounts_quote") or {}).get("adr_fee"),
                    (cu.get("amounts_quote") or {}).get("adr_fee"),
                )

                results.append({
                    "event_key": ek,
                    "deltas": {"gross_quote": d_g, "tax_quote": d_t, "net_quote": d_n},
                    "flags": [f for f in ["fx_mismatch" if fx_flag["flag"] else None,
                                          "missing_tax_rate" if tax_flag["flag"] else None]
                              if f],
                    "resolution": res
                })

        with open(out_jsonl, "w", encoding="utf-8") as out:
            for r in results:
                out.write(json.dumps(r) + "\n")
