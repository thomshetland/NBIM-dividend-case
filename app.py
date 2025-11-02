# app.py (cleaned)
import os
import io
import json
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import streamlit as st

import sys
sys.path.append(".")
sys.path.append("src")

from dotenv import load_dotenv
load_dotenv()

# pipeline modules
from src.transform import Transformer
from src.align_and_compare import align_and_compare
from src.report_qa import report_qa
import src.map_headers as MH

# agents
from src.agents.header_mapper import HeaderMapper
from src.agents.explainer import Explainer

# anthropic models
ANTHROPIC_ALIASES = {
    "haiku-3.5": "claude-3-5-haiku-20241022",
    "claude-3-5-haiku-latest": "claude-3-5-haiku-20241022",
    "claude-3-5-haiku": "claude-3-5-haiku-20241022",
}
def resolve_model(name: str | None) -> str | None:
    return ANTHROPIC_ALIASES.get(name.strip(), name.strip()) if name else None

# small helpers 
def _apply_header_overlay_runtime(accepted_map: dict):
    """Overlay accepted mappings at runtime (fills only unmapped after rules)."""
    if not accepted_map:
        return
    _orig = MH.map_headers
    def _patched_map_headers(columns):
        base = _orig(columns)
        for col in columns:
            if not base.get(col) and col in accepted_map:
                base[col] = accepted_map[col]
        return base
    MH.map_headers = _patched_map_headers

def _mapping_coverage(df: pd.DataFrame):
    # computes coverage
    colmap = MH.map_headers(list(df.columns))
    hits, total, pct, unmapped = MH.coverage(colmap)
    return {"total": total, "hits": hits, "pct": round(pct, 2), "unmapped": unmapped, "colmap": colmap}

def _status_row(det: str|None, ag: str|None) -> str:
    # classifies how entries are mapped
    d, a = (det or "").strip(), (ag or "").strip()
    if d and a and d == a: return "same"
    if not d and a:        return "added by agent"
    if d and a and d != a: return "changed by agent"
    if not d and not a:    return "unmapped"
    return "deterministic only"

def _build_mapping_table(label: str, columns: list[str], det_map: dict, ag_map: dict|None, suggestions_meta: dict|None):
    # builds mapping diff table rows
    rows = []
    meta = suggestions_meta or {}
    for h in columns:
        det, ag = det_map.get(h), (ag_map or {}).get(h)
        m = meta.get(h, {})
        rows.append({
            "source": label,
            "header": h,
            "deterministic": det or "",
            "agent": ag or "",
            "status": _status_row(det, ag),
            "confidence": "" if m.get("confidence") is None else f"{m['confidence']:.3f}",
            "reason": m.get("reason") or "",
        })
    return rows

def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    # loader for jsonl for showing comparison/explanations
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]

def _nested_get(d: dict, path: str):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur

# UI 
st.set_page_config(page_title="Agentic Dividend Reconciliation System", layout="wide")
st.title("⚡ Dividend Ingestion — Live App")

with st.sidebar:
    st.header("Run Settings")
    mapping_strategy = st.selectbox(
        "Mapping strategy",
        options=["Deterministic", "Agent", "Hybrid (diff only)"],
        index=0
    )
    use_agent = mapping_strategy.lower().startswith("agent") or "hybrid" in mapping_strategy.lower()

    # agents
    use_llm = st.toggle("Use LLM explainer", value=True, help="Requires Anthropic API key.")
    llm_model = st.text_input("LLM model (Anthropic)", value="claude-3-5-haiku-20241022", disabled=not use_llm)
    header_model = st.text_input("Header agent model (optional)", value="claude-3-5-haiku-20241022")

    st.markdown("---")
    if os.environ.get("ANTHROPIC_API_KEY"):
        st.caption("Anthropic key loaded")
    else:
        st.error("No ANTHROPIC_API_KEY found in env")

    if st.button("Test Anthropic call"):
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
            resp = client.messages.create(
                model=resolve_model(llm_model),
                max_tokens=16,
                messages=[{"role": "user", "content": "Say OK"}],
                temperature=0
            )
            st.success(f"Anthropic OK: {resp.content[0].text}")
        except Exception as e:
            st.error(f"Anthropic error: {e}")

st.subheader("1) Upload CSVs")
colA, colB = st.columns(2)
with colA:
    up_nbim = st.file_uploader("NBIM CSV", type=["csv"], key="nbim")
with colB:
    up_cust = st.file_uploader("Custody CSV", type=["csv"], key="custody")

run_btn = st.button("▶ Run pipeline")

# Work area
if "workdir" not in st.session_state:
    st.session_state["workdir"] = tempfile.mkdtemp(prefix="divi_live_")
workdir = Path(st.session_state["workdir"])
out_dir = workdir / "out"
out_dir.mkdir(parents=True, exist_ok=True)

def _save_upload(file, dest: Path):
    dest.write_bytes(file.read())

def _load_agent_meta(patch_json_path: Path) -> dict:
    try:
        return json.loads(patch_json_path.read_text(encoding="utf-8")).get("suggestions", {})
    except Exception:
        return {}

if run_btn:
    if not up_nbim or not up_cust:
        st.error("Please upload both CSVs.")
        st.stop()

    # Reset workdir
    shutil.rmtree(workdir, ignore_errors=True)
    workdir = Path(tempfile.mkdtemp(prefix="divi_live_"))
    out_dir = workdir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save uploads
    nbim_csv = workdir / "NBIM.csv"
    cust_csv = workdir / "CUSTODY.csv"
    _save_upload(up_nbim, nbim_csv)
    _save_upload(up_cust, cust_csv)

    # Coverage + mapping
    nbim_df = Transformer.robust_read_csv(str(nbim_csv))
    cust_df = Transformer.robust_read_csv(str(cust_csv))

    det_nb_map = MH.map_headers(list(nbim_df.columns))
    det_cu_map = MH.map_headers(list(cust_df.columns))
    det_nb_cov = _mapping_coverage(nbim_df)
    det_cu_cov = _mapping_coverage(cust_df)

    st.write(f"**Deterministic coverage — NBIM:** {det_nb_cov['hits']}/{det_nb_cov['total']} → {det_nb_cov['pct']}%")
    st.write(f"**Deterministic coverage — CUSTODY:** {det_cu_cov['hits']}/{det_cu_cov['total']} → {det_cu_cov['pct']}%")

    accepted_map: dict = {}
    patch_json = out_dir / "header_mapping_patch.json"
    suggestions_meta = {}

    if use_agent:
        if not HeaderMapper:
            st.warning("Header-mapper agent not available. Proceeding deterministic.")
        else:
            with st.spinner("Running header-mapper agent…"):
               HeaderMapper(model=resolve_model(header_model) if header_model else None).run(
                   str(nbim_csv), str(cust_csv), str(patch_json)
                )
            accepted_path = Path(str(patch_json).replace(".json", ".accepted.json"))
            if accepted_path.exists():
                accepted_map = json.loads(accepted_path.read_text(encoding="utf-8")).get("accepted", {})
            suggestions_meta = _load_agent_meta(patch_json)
            _apply_header_overlay_runtime(accepted_map)

    # Maps after overlay (or deterministic if no overlay)
    ag_nb_map = MH.map_headers(list(nbim_df.columns))
    ag_cu_map = MH.map_headers(list(cust_df.columns))
    ag_nb_cov = _mapping_coverage(nbim_df)
    ag_cu_cov = _mapping_coverage(cust_df)

    if use_agent:
        st.write(f"**Agent coverage — NBIM:** {ag_nb_cov['hits']}/{ag_nb_cov['total']} → {ag_nb_cov['pct']}%")
        st.write(f"**Agent coverage — CUSTODY:** {ag_cu_cov['hits']}/{ag_cu_cov['total']} → {ag_cu_cov['pct']}%")
        if "hybrid" in mapping_strategy.lower():
            st.info(f"NBIM coverage delta: {det_nb_cov['hits']}→{ag_nb_cov['hits']} ({det_nb_cov['pct']}%→{ag_nb_cov['pct']}%)")
            st.info(f"CUSTODY coverage delta: {det_cu_cov['hits']}→{ag_cu_cov['hits']} ({det_cu_cov['pct']}%→{ag_cu_cov['pct']}%)")

    # Mapping diff table
    nb_rows = _build_mapping_table("NBIM", list(nbim_df.columns), det_nb_map, ag_nb_map, suggestions_meta)
    cu_rows = _build_mapping_table("CUSTODY", list(cust_df.columns), det_cu_map, ag_cu_map, suggestions_meta)
    df_map = pd.DataFrame(nb_rows + cu_rows)

    st.subheader("2) Mapping diff")
    if df_map.empty:
        st.info("No mapping rows available.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            sources = st.multiselect("Source", sorted(df_map["source"].unique()), default=sorted(df_map["source"].unique()))
        with col2:
            statuses = st.multiselect("Status", sorted(df_map["status"].unique()), default=sorted(df_map["status"].unique()))
        q = st.text_input("Search header")
        view = df_map[df_map["source"].isin(sources) & df_map["status"].isin(statuses)]
        if q:
            view = view[view["header"].str.contains(q, case=False, na=False)]
        st.dataframe(view, use_container_width=True)
        st.download_button("Download mapping diff (CSV)", data=view.to_csv(index=False), file_name="mapping_diff.csv", mime="text/csv")

    # ---- Pipeline outputs
    nbim_events = out_dir / "nbim.events.jsonl"
    custody_events = out_dir / "custody.events.jsonl"
    compare_out = out_dir / "comparison_frame.jsonl"
    qa_md = out_dir / "qa_summary.md"

    with st.spinner("Transforming & comparing…"):
        n_nb = Transformer("NBIM").transform(str(nbim_csv), str(nbim_events))
        n_cu = Transformer("CUSTODY").transform(str(cust_csv), str(custody_events))
        n_cmp = align_and_compare(str(nbim_events), str(custody_events), str(compare_out))
        report_qa(str(compare_out), str(qa_md))

    st.success(f"Pipeline done: NBIM rows={n_nb}, CUSTODY rows={n_cu}, comparison records={n_cmp}")

    # ---- Explanations (LLM only)
    explanations_jsonl = out_dir / "explanations.jsonl"
    explanations_md = out_dir / "explanations.md"
    if use_llm and Explainer:
        with st.spinner("Generating LLM explanations…"):
            try:
                expl = Explainer(model=resolve_model(llm_model))
                expl.run(str(compare_out), str(explanations_jsonl), str(explanations_md))
                st.success("Explanations generated.")
            except Exception as e:
                st.error(f"Explainer failed: {e}")
    elif use_llm and not Explainer:
        st.warning("Explainer module not available (src/agents/run_explain.py).")

    # ---- Summary & QA
    st.subheader("3) Summary & QA")
    st.markdown(qa_md.read_text(encoding="utf-8")) if qa_md.exists() else st.info("No QA summary found.")

    # ---- Explanations
    st.subheader("4) Explanations")
    if explanations_jsonl.exists():
        rows = _load_jsonl(explanations_jsonl)
        flat = [{"event_key": r.get("event_key"),
                 "paragraph": (r.get("explanation", {}) or {}).get("paragraph", ""),
                 "bullets": " • ".join([str(b) for b in (r.get("explanation", {}) or {}).get("bullets", [])])}
                for r in rows]
        df = pd.DataFrame(flat)
        q2 = st.text_input("Filter explanations")
        if q2:
            m = df["paragraph"].str.contains(q2, case=False, na=False) | df["bullets"].str.contains(q2, case=False, na=False)
            df = df[m]
        st.dataframe(df, use_container_width=True)
        if explanations_md.exists():
            with st.expander("Markdown version"):
                st.markdown(explanations_md.read_text(encoding="utf-8"))
        st.download_button("Download explanations (JSONL)", data="\n".join([json.dumps(r) for r in rows]), file_name="explanations.jsonl")
    else:
        st.info("No explanations generated (or explainer disabled).")

    # ---- Comparison browser
    st.subheader("5) Comparison Browser")
    cmp_rows = _load_jsonl(compare_out)
    if not cmp_rows:
        st.info("No comparison data.")
    else:
        view = []
        for item in cmp_rows:
            nb, cu, dr = item.get("nbim") or {}, item.get("custody") or {}, item.get("derived") or {}
            view.append({
                "event_key": item.get("event_key"),
                "isin": _nested_get(nb, "instrument.isin") or _nested_get(cu, "instrument.isin"),
                "quote_ccy": _nested_get(nb, "currencies.quote_ccy") or _nested_get(cu, "currencies.quote_ccy"),
                "gross_delta": _nested_get(dr, "delta.gross_quote"),
                "tax_delta": _nested_get(dr, "delta.tax_quote"),
                "net_delta": _nested_get(dr, "delta.net_quote"),
                "flags": ", ".join(dr.get("flags", [])),
            })
        df_cmp = pd.DataFrame(view)
        c1, c2, c3 = st.columns(3)
        with c1:
            isin_sel = st.text_input("Filter ISIN contains")
        with c2:
            flag_sel = st.text_input("Filter flag contains")
        with c3:
            hide_zero = st.checkbox("Hide zero-delta", value=False)

        mask = pd.Series(True, index=df_cmp.index)
        if isin_sel:
            mask &= df_cmp["isin"].astype(str).str.contains(isin_sel, case=False, na=False)
        if flag_sel:
            mask &= df_cmp["flags"].str.contains(flag_sel, case=False, na=False)
        if hide_zero:
            mask &= (df_cmp[["gross_delta","tax_delta","net_delta"]].fillna(0).abs().sum(axis=1) > 0)
        st.dataframe(df_cmp[mask], use_container_width=True)

        st.markdown("---")
        ek = st.text_input("Open event_key")
        if ek:
            rec = next((r for r in cmp_rows if r.get("event_key") == ek), None)
            st.json(rec) if rec else st.warning("event_key not found.")

    # ---- Downloads
    st.subheader("6) Artifacts")
    files = [
        ("NBIM events (JSONL)", nbim_events),
        ("Custody events (JSONL)", custody_events),
        ("Comparison frame (JSONL)", compare_out),
        ("QA summary (MD)", qa_md),
        ("Header mapping patch (JSON)", patch_json),
        ("Explanations (JSONL)", explanations_jsonl),
        ("Explanations (MD)", explanations_md),
    ]
    for label, p in files:
        if p.exists():
            with p.open("rb") as fh:
                st.download_button(f"Download: {label}", data=fh, file_name=p.name)
        else:
            st.caption(f"Missing: {label}")
