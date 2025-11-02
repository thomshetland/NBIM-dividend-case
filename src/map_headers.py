import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

@dataclass
class Rule:
    pattern: re.Pattern
    ces_path: str

def _compile_rules() -> List[Rule]:
    pairs = [
        (r"^ISIN$", "instrument.isin"),
        (r"^SEDOL$", "instrument.sedol"),
        (r"^TICKER$", "instrument.ticker"),
        (r"^(INSTRUMENT_DESCRIPTION|SECURITY_NAME)$", "instrument.name"),
        (r"^(EXDATE|EX_DATE)$", "dates.ex_date"),
        (r"^(PAYMENT_DATE|PAY_DATE)$", "dates.pay_date"),
        (r"^(PAY_REC_DATE|RECORD_DATE)$", "dates.record_date"),
        (r"^(QUOTATION_CURRENCY|CURRENCIES)$", "currencies.quote_ccy"),
        (r"^(SETTLEMENT_CURRENCY|SETTLED_CURRENCY)$", "currencies.settle_ccy"),
        (r"^(AVG_FX_RATE_QUOTATION_TO_PORTFOLIO|FX_RATE)$", "fx.quote_to_portfolio_fx"),
        (r"^(DIVIDENDS_PER_SHARE|DIV_RATE)$", "rate.div_per_share"),
        (r"^(TAX_RATE|WTHTAX_RATE)$", "rate.tax_rate"),
        (r"^ADR_FEE_RATE$", "rate.adr_fee_rate"),
        (r"^(NOMINAL_BASIS)$", "positions.nominal_basis"),
        (r"^(GROSS_AMOUNT_QUOTATION|GROSS_AMOUNT|GROSS_AMOUNT_QC)$", "amounts_quote.gross"),
        (r"^(WITHHOLDING_TAX_AMOUNT_QUOTATION|TAX)$", "amounts_quote.tax"),
        (r"^(ADR_FEE)$", "amounts_quote.adr_fee"),
        (r"^(NET_AMOUNT_QUOTATION|NET_AMOUNT_QC)$", "amounts_quote.net"),
        (r"^GROSS_AMOUNT_SC$", "amounts_settle.gross"),
        (r"^NET_AMOUNT_SC$", "amounts_settle.net"),
        (r"^WITHHOLDING_TAX_AMOUNT_SETTLEMENT$", "amounts_settle.tax"),
        (r"^COAC_EVENT_KEY$", "source.vendor_event_key"),
        (r"^CUSTODIAN$", "source.custodian"),
        (r"^(BANK_ACCOUNT|BANK_ACCOUNTS)$", "source.bank_account"),
        (r"^ORGANISATION_NAME$", "source.organisation_name"),
        (r"^EVENT_PAYMENT_DATE$", "dates.pay_date"),
    ]
    return [Rule(re.compile(p, re.IGNORECASE), ces) for p, ces in pairs]

RULES = _compile_rules()

def map_header_to_ces(col: str) -> Tuple[str, str]:
    """Return (original_col, ces_path) where ces_path may be '' if unmapped."""
    for rule in RULES:
        if rule.pattern.search(col.strip()):
            return col, rule.ces_path
    return col, ""

def map_headers(columns: List[str]) -> Dict[str, str]:
    """Map a list of headers to CES paths using deterministic rules."""
    return {c: map_header_to_ces(c)[1] for c in columns}

def coverage(mapped: Dict[str, str]):
    """Return (hits, total, pct, unmapped_list)."""
    total = len(mapped)
    hits = sum(1 for v in mapped.values() if v)
    pct = (hits / total * 100.0) if total else 100.0
    unmapped = [k for k, v in mapped.items() if not v]
    return hits, total, pct, unmapped