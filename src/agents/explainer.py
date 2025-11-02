# src/agents/explainer.py
from __future__ import annotations
import os, json
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterable
import anthropic

SYSTEM = (
    "You are a precise financial reconciler. You receive a normalized dividend comparison JSON "
    "with fields: nbim, custody, and derived (delta and flags). Explain differences in clear, "
    "concise language for a human reviewer. Use only provided fields and numbers; do not invent data. "
    "Prefer short sentences. Include drivers (FX, ADR fees, tax rate, position). "
    "Always respond by calling the tool."
)

class Explainer:
    def __init__(self, model: str, api_key: Optional[str] = None):
        if not model:
            raise RuntimeError("LLM model required for explainer.")
        api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("Missing ANTHROPIC_API_KEY.")
        self.model = model
        self.client = anthropic.Anthropic(api_key=api_key)
        self.tools: list[anthropic.types.ToolParam] = [
            {
                "name": "report_explanation",
                "description": "Return bullet points (2-3) and one concise paragraph explaining the reconciliation.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "bullets": {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 5},
                        "paragraph": {"type": "string"}
                    },
                    "required": ["bullets", "paragraph"]
                }
            }
        ]

    def explain_one(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return {'bullets': [...], 'paragraph': '...'} for a single comparison item."""
        prompt = "Given the following JSON, report the explanation using the tool:\n" + \
                 json.dumps(item, separators=(",", ":"))
        msg = self.client.messages.create(
            model=self.model,
            max_tokens=800,
            system=SYSTEM,
            tools=self.tools,
            messages=[{"role": "user", "content": prompt}],
        )
        for part in msg.content:
            if getattr(part, "type", None) == "tool_use" and part.name == "report_explanation":
                data = part.input or {}
                bullets = data.get("bullets")
                paragraph = data.get("paragraph")
                if not isinstance(bullets, list) or not isinstance(paragraph, str):
                    raise RuntimeError("Tool payload must have 'bullets' (list) and 'paragraph' (string).")
                return {"bullets": [str(b) for b in bullets], "paragraph": paragraph}
        raise RuntimeError("Model did not use report_explanation tool.")

    def run(self, input_jsonl: str | Path, out_jsonl: str | Path, out_md: str | Path) -> None:
        """Batch over comparison JSONL and write JSONL + Markdown outputs."""
        input_jsonl, out_jsonl, out_md = Path(input_jsonl), Path(out_jsonl), Path(out_md)
        out_jsonl.parent.mkdir(parents=True, exist_ok=True)

        records: list[tuple[dict, dict]] = []
        with input_jsonl.open("r", encoding="utf-8") as f_in, out_jsonl.open("w", encoding="utf-8") as f_out:
            for line in f_in:
                if not line.strip():
                    continue
                item = json.loads(line)
                exp = self.explain_one(item)
                rec = {"event_key": item.get("event_key"), "explanation": exp}
                f_out.write(json.dumps(rec) + "\n")
                records.append((item, exp))

        with out_md.open("w", encoding="utf-8") as md:
            md.write("# Explanations\n\n")
            for item, exp in records:
                ek = item.get("event_key")
                # pull ISIN from either side
                src = item.get("nbim") or item.get("custody") or {}
                isin = (src.get("instrument") or {}).get("isin", "")
                md.write(f"## {ek}  \n")
                if isin:
                    md.write(f"*ISIN:* `{isin}`  \n")
                md.write(exp["paragraph"].rstrip(".") + ".\n\n")
                for b in exp["bullets"]:
                    md.write(f"- {b}\n")
                md.write("\n")
