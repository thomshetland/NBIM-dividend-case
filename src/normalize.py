from __future__ import annotations
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import Optional, Tuple

class NormalizationError(Exception):
    pass

def _strip(s):
    return s.strip() if isinstance(s, str) else s

# --- DATE NORMALIZATION ---
def normalize_date(value: Optional[str]) -> Optional[str]:
    """
    Convert a variety of date formats to ISO YYYY-MM-DD.
    Accepts:
      - DD.MM.YYYY (e.g., 07.02.2025)
      - YYYY-MM-DD
      - DD/MM/YYYY, MM/DD/YYYY (heuristic: if first token >12, it's DD/MM/YYYY)
      - YYYY/MM/DD
    Returns ISO string or None if empty. Raises NormalizationError for impossible dates.
    """
    if value is None:
        return None
    s = _strip(str(value))
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None

    # Common separators
    if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", s):
        # DD.MM.YYYY
        try:
            dt = datetime.strptime(s, "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError as e:
            raise NormalizationError(f"Invalid DD.MM.YYYY date: {s}") from e

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        # YYYY-MM-DD
        return s

    if re.fullmatch(r"\d{4}/\d{2}/\d{2}", s):
        # YYYY/MM/DD
        try:
            dt = datetime.strptime(s, "%Y/%m/%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError as e:
            raise NormalizationError(f"Invalid YYYY/MM/DD date: {s}") from e

    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        # ambiguous: DD/MM/YYYY or MM/DD/YYYY
        dd, mm, yyyy = s.split("/")
        d = int(dd); m = int(mm); y = int(yyyy)
        # heuristic: if first token > 12, treat as DD/MM/YYYY
        if d > 12:
            fmt = "%d/%m/%Y"
        else:
            # if second token > 12, it's MM/DD/YYYY must be swapped accordingly
            fmt = "%m/%d/%Y"
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError as e:
            raise NormalizationError(f"Invalid slash date: {s}") from e

    # Fallback: try parsing YYYYMMDD
    if re.fullmatch(r"\d{8}", s):
        try:
            dt = datetime.strptime(s, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError as e:
            raise NormalizationError(f"Invalid YYYYMMDD date: {s}") from e

    raise NormalizationError(f"Unrecognized date format: {s}")

# --- DECIMAL NORMALIZATION ---
def normalize_decimal(value) -> Optional[Decimal]:
    """
    Convert a numeric-looking string to Decimal.
    Handles thousand separators and comma decimal: '318,750.00', '318.750,00', '0,25'.
    Returns None for empty. Raises NormalizationError for irrecoverable values.
    """
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except Exception as e:
            raise NormalizationError(f"Invalid numeric value: {value}") from e

    s = _strip(str(value))
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None

    # Remove spaces
    s = s.replace(" ", "")

    # If both comma and dot present, assume comma is thousands sep if dot comes after comma
    if "," in s and "." in s:
        # Case like 318,750.00 -> remove commas
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            # 318.750,00 -> remove dots, replace comma with dot
            s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        # "0,25" -> decimal comma
        s = s.replace(",", ".")
    # else: only dot or clean

    try:
        return Decimal(s)
    except (InvalidOperation, ValueError) as e:
        raise NormalizationError(f"Invalid numeric after cleanup: {value} -> {s}") from e

# --- CURRENCY NORMALIZATION ---
def normalize_ccy(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = _strip(str(value)).upper()
    if s in {"", "NAN", "NONE", "NULL"}:
        return None
    # Keep only 3-letter A-Z sequences; pass through if already correct
    m = re.match(r"^[A-Z]{3}$", s)
    if m:
        return s
    # Try to extract a 3-letter code from within
    m = re.search(r"[A-Z]{3}", s)
    return m.group(0) if m else None

# --- SAFE DERIVATIONS & HELPERS ---
def derive_missing_tax(gross: Optional[Decimal], net: Optional[Decimal], tax: Optional[Decimal]) -> Tuple[Optional[Decimal], str]:
    """
    If tax is None but gross and net present, compute tax = gross - net.
    Returns (tax, provenance_note).
    """
    if tax is None and gross is not None and net is not None:
        try:
            return (gross - net, "derived: tax = gross - net")
        except Exception:
            return (None, "derive_failed: tax")
    return (tax, "")

def default_fx_if_same_ccy(quote_ccy: Optional[str], settle_ccy: Optional[str], fx: Optional[Decimal]) -> Tuple[Optional[Decimal], str]:
    """
    If fx is None and quote_ccy == settle_ccy and both present, set fx = 1.0
    """
    if fx is None and quote_ccy and settle_ccy and quote_ccy == settle_ccy:
        return (Decimal("1.0"), "default: fx=1.0 (same ccy)")
    return (fx, "")