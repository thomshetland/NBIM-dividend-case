# NBIM Dividend Ingestion & Reconciliation — README

## Overview

This project ingests two dividend files (NBIM vs Custody), normalizes them to a **Common Event Schema (CES)**, aligns comparable events, and produces human‑readable QA outputs. It includes a Streamlit UI for analysts and a fully deterministic batch flow for reproducibility.

Why deterministic? We ship two mapping modes—**Deterministic** and **Agent**—for mapping source headers to CES fields. Agentic mapping can be helpful on messy files but is non‑deterministic and may hallucinate. For the interview case and any production‑like run, we use **Deterministic mapping** to guarantee repeatable results.

---

## Key ideas

* **Deterministic header mapping** (`map_headers.py`): regex/heuristic rules map raw CSV headers to CES paths. No LLM needed → fully reproducible.
* **Agent header mapping (optional)** (`header_mapper.py`): an Anthropic‑powered helper that *suggests* mappings for unknown columns using sampled values. Useful for exploration; disabled by default.
* **Pre‑processing**: we tidy and normalize rows (dates, decimals, currencies) so downstream alignment—and even other agents—operate on a clean, structured format.
* **Event Keying** (`event_key.py`): stable hash built from `(ISIN, ex_date, pay_date, quote_ccy)` if no vendor key is provided.
* **Alignment & comparison** (`align_and_compare.py`): groups by `event_key`, aggregates amounts, computes deltas/flags.
* **QA reporting** (`report_qa.py`): concise markdown summary of flags and largest discrepancies.
* **LLM Explainer (optional)** (`explainer.py`): generates short natural‑language rationales for differences (FX, ADR fees, tax rate, etc.).

---

## Repository layout

```
├─ app.py                      # Streamlit UI
├─ main.py                     # Batch pipeline entrypoint
├─ map_headers.py              # Deterministic header mapper
├─ header_mapper.py            # Optional agentic header mapper (Anthropic)
├─ transform.py / transform_common.py / normalize.py
├─ align_and_compare.py        # Event alignment + delta computation
├─ event_key.py                # Stable key builder
├─ report_qa.py                # QA markdown summary
├─ explainer.py                # Optional LLM explanations
├─ requirements.txt
├─ source/
│  ├─ nbim.mapping.json       # Deterministic mapping for NBIM input
│  └─ custody.mapping.json    # Deterministic mapping for Custody input
└─ out/                        # All generated artifacts
   ├─ nbim.events.jsonl        # CES rows for NBIM
   ├─ custody.events.jsonl     # CES rows for Custody
   ├─ comparison_frame.jsonl   # Per‑event deltas + flags
   ├─ qa_summary.md            # QA summary (counts + top deltas)
   ├─ explanations.jsonl       # (optional) LLM per‑event explanations
   └─ explanations.md          # (optional) human‑readable explanations
```

> The `source/` and `out/` folders are part of the deliverable. Place the mapping JSONs in `source/`. The pipeline writes all outputs to `out/`.

---

## Common Event Schema (CES) — selected fields

* `event_key`: stable string (vendor key if provided; else hash of ISIN/ex_date/pay_date/quote_ccy)
* `instrument.{isin, sedol, ticker, name}`
* `dates.{ex_date, record_date, pay_date}` (ISO `YYYY-MM-DD`)
* `currencies.{quote_ccy, settle_ccy}` (3‑letter codes)
* `amounts_quote.{gross, tax, net}` (Decimal)
* `amounts_settle.{gross, tax, net}` (Decimal)
* `fx.quote_to_portfolio_fx`
* `rate.tax_rate`
* `source.{system, vendor_event_key, provenance_notes}`

Normalization utilities ensure robust parsing of dates, decimals (dot/comma, positives/negatives), and currencies.

---

## Quickstart

### 1) Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

Optional (only if you’ll use agent/explanations): create a `.env` with your Anthropic key.

```
ANTHROPIC_API_KEY=sk-ant-...
LLM_MODEL=claude-3-5-haiku-20241022
```

### 2) Provide deterministic mappings

Edit or drop in:

```
source/nbim.mapping.json
source/custody.mapping.json
```

Each file is a JSON object `{ "<raw header>": "<ces.path>" }`. Any header missing in JSON falls back to the deterministic rules in `map_headers.py`.

### 3) Run the batch pipeline

```bash
python main.py \
  --nbim /path/to/NBIM_dividends.csv \
  --custody /path/to/Custody_dividends.csv \
  --out out \
  --mapping deterministic \
  [--with-explainer]   # optional, requires Anthropic key
```

**Outputs** land in `out/` as listed above. See `qa_summary.md` for a one‑page view of quality flags and biggest deltas.

> If `--mapping agent` is used, the header suggestion tool will attempt to map unknown columns using sample values. Results are written as `out/header_mapping_patch.json` and applied on top of deterministic rules for transparency. In **Hybrid** mode, only unmapped headers are sent to the agent, and diffs are saved alongside coverage stats.

### 4) Streamlit UI (analyst mode)

```bash
streamlit run app.py
```

* Upload NBIM and Custody CSVs
* Choose **Deterministic** (default) or **Agent/Hybrid** (exploratory)
* Toggle **LLM explainer** (optional)
* Download the generated artifacts from the sidebar

---

## Deterministic vs Agent mapping

* **Deterministic** (default): fast, reproducible, reviewable. Uses curated regex rules + `source/*.mapping.json`. Ideal for audits and production.
* **Agent**: probabilistic; can propose mappings for previously unseen headers using value samples. Helpful for ad‑hoc files but may vary between runs.
* **Project stance**: determinism over variability. We *pre‑process and structure the CSVs* so any downstream agent (ours or others) operates on a clean, consistent schema—reducing hallucination risk and improving explainability.

---

## Flags and QA heuristics

* `fx_mismatch` when quote_ccy ≠ settle_ccy and FX differs across sources
* `gross_tax_net_mismatch` when any of gross/tax/net differs beyond tolerance
* `missing_tax_rate` when either side lacks a tax rate
  `report_qa.py` compiles totals and lists the **top 10 deltas** (|gross|+|tax|+|net|).

---

## Extending the system

* Add new regex rules in `map_headers.py`
* Add/override per‑source mappings in `source/*.mapping.json`
* Customize derivations in `normalize.py` (e.g., default FX=1.0 when quote=settle)
* Expand CES or comparison logic in `transform_common.py` / `align_and_compare.py`

---

## Troubleshooting

* *A header isn’t mapped*: add it to `source/*mapping.json` or extend `map_headers.py`
* *Decimals look wrong*: input may use commas; normalization handles it—verify `normalize_decimal`
* *Missing outputs*: ensure `--out out` is writable and files are not empty
* *LLM features skipped*: set `ANTHROPIC_API_KEY` and `LLM_MODEL` in `.env`

---


