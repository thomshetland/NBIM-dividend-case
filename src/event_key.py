import hashlib

def build_event_key(isin: str|None, ex_date: str|None, pay_date: str|None, quote_ccy: str|None) -> str:
    """Stable key across sources. Missing pieces become empty strings but still hashed."""
    parts = [
        (isin or "").upper(),
        (ex_date or ""),
        (pay_date or ""),
        (quote_ccy or "").upper(),
    ]
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()