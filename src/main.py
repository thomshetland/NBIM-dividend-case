# main.py
import os
import json
import sys

sys.path.append(".")
sys.path.append("src")

from dotenv import load_dotenv
load_dotenv()

from src.transform import Transformer
from src.align_and_compare import AlignerComparator   # <-- use the class
from src.report_qa import report_qa
import pandas as pd


def _to_bool(v, default=False):
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


# paths from .env (fallbacks so script still runs if unset)
NBIM_CSV    = os.environ.get("NBIM_CSV", "data/NBIM_Dividend_Bookings.csv")
CUSTODY_CSV = os.environ.get("CUSTODY_CSV", "data/CUSTODY_Dividend_Bookings.csv")
OUT_DIR     = os.environ.get("OUT_DIR", "out")

# mapping strategy: deterministic | agent | hybrid
MAPPING_STRATEGY = os.environ.get("MAPPING_STRATEGY", "deterministic").strip().lower()

# header agent model (optional)
HEADER_MODEL = os.environ.get("HEADER_MODEL")

# Explainer (LLM only)
USE_LLM   = _to_bool(os.environ.get("USE_LLM", "true"))
LLM_MODEL = os.environ.get("LLM_MODEL", "claude-3-5-haiku-20241022")


# Header agent
def _apply_header_overlay_runtime(accepted_map: dict):
    """Overlay accepted mappings after deterministic rules (fill gaps only)."""
    if not accepted_map:
        return
    import src.map_headers as MH
    _orig = MH.map_headers

    def _patched_map_headers(columns):
        base = _orig(columns)
        for col in columns:
            if (not base.get(col)) and (col in accepted_map):
                base[col] = accepted_map[col]
        return base

    MH.map_headers = _patched_map_headers


def _run_header_mapper() -> dict:
    """Run header-mapper agent; returns accepted mapping dict (may be empty)."""
    try:
        from src.agents.header_mapper import HeaderMapper
    except Exception as e:
        print(f"[header-agent] skipped: module not found ({e})")
        return {}

    os.makedirs(OUT_DIR, exist_ok=True)
    patch_json = os.path.join(OUT_DIR, "header_mapping_patch.json")
    print("[header-agent] running…")
    mapper = HeaderMapper(model=(HEADER_MODEL if HEADER_MODEL else None))
    suggestions = mapper.run(NBIM_CSV, CUSTODY_CSV, patch_json)

    accepted_path = os.path.splitext(patch_json)[0] + ".accepted.json"
    accepted_map = {}
    if os.path.exists(accepted_path):
        with open(accepted_path, "r", encoding="utf-8") as f:
            accepted_map = (json.load(f).get("accepted") or {})

    print(f"[header-agent] accepted: {len(accepted_map)}  skipped: {len(suggestions) - len(accepted_map)}")
    for h, tgt in accepted_map.items():
        print(f"  + {h} -> {tgt}")
    return accepted_map


# coverage reporting
def _mapping_coverage(df: pd.DataFrame):
    from src.map_headers import map_headers, coverage
    colmap = map_headers(list(df.columns))
    hits, total, pct, unmapped = coverage(colmap)
    return {"total": total, "hits": hits, "pct": round(pct, 2), "unmapped": unmapped, "colmap": colmap}


def _print_cov(label: str, cov: dict):
    print(f"[coverage:{label}] {cov['hits']}/{cov['total']} → {cov['pct']}%")
    if cov["unmapped"]:
        print("  unmapped (first 12):", cov["unmapped"][:12])


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    nbim_events    = os.path.join(OUT_DIR, "nbim.events.jsonl")
    custody_events = os.path.join(OUT_DIR, "custody.events.jsonl")
    compare_out    = os.path.join(OUT_DIR, "comparison_frame.jsonl")

    print(f"[ingest] NBIM:    {NBIM_CSV}")
    print(f"[ingest] CUSTODY: {CUSTODY_CSV}")
    print(f"[ingest] OUT:     {OUT_DIR}")
    print(f"[mapping] strategy = {MAPPING_STRATEGY}")

    # Coverage / header agent preview
    nbim_df    = Transformer.robust_read_csv(NBIM_CSV)
    custody_df = Transformer.robust_read_csv(CUSTODY_CSV)

    det_nb_cov = _mapping_coverage(nbim_df)
    det_cu_cov = _mapping_coverage(custody_df)
    _print_cov("deterministic:NBIM", det_nb_cov)
    _print_cov("deterministic:CUSTODY", det_cu_cov)

    accepted_map = {}
    if MAPPING_STRATEGY in {"agent", "hybrid"}:
        accepted_map = _run_header_mapper()
        if accepted_map:
            _apply_header_overlay_runtime(accepted_map)
            # Recompute coverage after overlay
            ag_nb_cov = _mapping_coverage(nbim_df)
            ag_cu_cov = _mapping_coverage(custody_df)
            _print_cov("agent:NBIM", ag_nb_cov)
            _print_cov("agent:CUSTODY", ag_cu_cov)

            if MAPPING_STRATEGY == "hybrid":
                def delta_str(before, after):
                    return f"{before['hits']}→{after['hits']} ({before['pct']}%→{after['pct']}%)"
                print("[coverage:delta] NBIM   ", delta_str(det_nb_cov, ag_nb_cov))
                print("[coverage:delta] CUSTODY", delta_str(det_cu_cov, ag_cu_cov))
        else:
            print("[header-agent] no accepted suggestions (proceeding with deterministic mapping)")

    # --- Transform → Compare → QA ---
    n_nb  = Transformer("NBIM").transform(NBIM_CSV, nbim_events)
    n_cu  = Transformer("CUSTODY").transform(CUSTODY_CSV, custody_events)

    # NEW: use the class-based comparator
    n_cmp = AlignerComparator().run(nbim_events, custody_events, compare_out)

    summary = {
        "outputs": {
            "nbim_events": nbim_events,
            "custody_events": custody_events,
            "comparison_frame": compare_out
        },
        "counts": {
            "nbim_rows": n_nb,
            "custody_rows": n_cu,
            "comparison_records": n_cmp
        }
    }
    print(json.dumps(summary, indent=2))

    qa_md = os.path.join(OUT_DIR, "qa_summary.md")
    report_qa(compare_out, qa_md)
    print(f"[qa] wrote {qa_md}")

    # --- Explainer (LLM-only) ---
    try:
        from src.agents.explainer import Explainer
        explanations_jsonl = os.path.join(OUT_DIR, "explanations.jsonl")
        explanations_md    = os.path.join(OUT_DIR, "explanations.md")
        if not USE_LLM:
            raise RuntimeError("USE_LLM is false in .env; explainer is LLM-only.")
        if not LLM_MODEL:
            raise RuntimeError("LLM_MODEL not set in .env for LLM-only explainer.")
        Explainer(model=LLM_MODEL).run(compare_out, explanations_jsonl, explanations_md)
        print(f"[explainer] wrote {explanations_jsonl} and {explanations_md}")
    except Exception as e:
        print(f"[explainer] skipped: {e}")


if __name__ == "__main__":
    main()
