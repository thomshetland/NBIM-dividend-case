import json, os, collections

def report_qa(comparison_path: str, out_md: str):
    total = 0
    flags = collections.Counter()
    big_deltas = []
    with open(comparison_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            obj = json.loads(line)
            total += 1
            for fl in obj.get("derived",{}).get("flags",[]):
                flags[fl] += 1
            delta = obj.get("derived",{}).get("delta",{})
            gross = abs(delta.get("gross_quote") or 0.0)
            tax   = abs(delta.get("tax_quote") or 0.0)
            net   = abs(delta.get("net_quote") or 0.0)
            score = gross + tax + net
            big_deltas.append((score, obj["event_key"], delta))
    big_deltas.sort(reverse=True)
    top = big_deltas[:10]

    os.makedirs(os.path.dirname(out_md), exist_ok=True)
    with open(out_md, "w", encoding="utf-8") as w:
        w.write("# QA Summary\n\n")
        w.write(f"- Comparison records: **{total}**\n")
        w.write(f"- Flag counts: `{dict(flags)}`\n\n")
        w.write("## Top 10 deltas (|gross|+|tax|+|net|)\n")
        for score, key, delta in top:
            w.write(f"- `{key}` → {delta}\n")
