# src/normalize.py
from __future__ import annotations
import re
from decimal import Decimal
from datetime import datetime
from typing import Optional, Tuple

class NormalizationError(Exception):
    """Raised when normalization fails."""
    pass

class Normalizer:
    """
    Central place for all input normalization and safe derivations.
    Stateless by default; you could add settings later (e.g., date locales).
    """

    # -------------------- DATE --------------------
    @staticmethod
    def normalize_date(value: Optional[str]) -> Optional[str]:
        """
        Convert a variety of date formats to ISO YYYY-MM-DD.
        Accepts:
          - DD.MM.YYYY (e.g., 07.02.2025)
          - YYYY-MM-DD
          - DD/MM/YYYY, MM/DD/YYYY (heuristic: if first token > 12 -> DD/MM/YYYY)
          - YYYY/MM/DD
          - YYYYMMDD
        Returns ISO string or None if empty. Raises NormalizationError for impossible dates.
        """
        if value is None:
            return None
        s = Normalizer._strip(str(value))
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return None

        # DD.MM.YYYY
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", s):
            dt = datetime.strptime(s, "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")


        # YYYY-MM-DD
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return s

        # YYYY/MM/DD
        if re.fullmatch(r"\d{4}/\d{2}/\d{2}", s):
            dt = datetime.strptime(s, "%Y/%m/%d")
            return dt.strftime("%Y-%m-%d")

        # DD/MM/YYYY or MM/DD/YYYY
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
            dd, mm, yyyy = s.split("/")
            d = int(dd); m = int(mm)
            fmt = "%d/%m/%Y" if d > 12 else "%m/%d/%Y"
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")

        # YYYYMMDD
        if re.fullmatch(r"\d{8}", s):
            dt = datetime.strptime(s, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")

    # ------------------- DECIMAL -------------------
    @staticmethod
    def normalize_decimal(value) -> Optional[Decimal]:
        """
        Convert a numeric-looking string to Decimal.
        Handles: thousand sep + comma decimal ('318,750.00', '318.750,00', '0,25').
        Returns None for empty. Raises NormalizationError for irrecoverable values.
        """
        if value is None:
            return None
        if isinstance(value, (int, float, Decimal)):
            return Decimal(str(value))


        s = Normalizer._strip(str(value))
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return None

        s = s.replace(" ", "")
        if "," in s and "." in s:
            # 318,750.00 -> remove commas; 318.750,00 -> remove dots, comma->dot
            if s.rfind(".") > s.rfind(","):
                s = s.replace(",", "")
            else:
                s = s.replace(".", "").replace(",", ".")
        elif "," in s and "." not in s:
            s = s.replace(",", ".")
        # else only dot or clean
        return Decimal(s)


    # ------------------- CURRENCY -------------------
    @staticmethod
    def normalize_ccy(value: Optional[str]) -> Optional[str]:
        """
        Normalize currency to a 3-letter ISO-like code (best effort).
        Returns None for empty/unknown.
        """
        if value is None:
            return None
        s = Normalizer._strip(str(value)).upper()
        if s in {"", "NAN", "NONE", "NULL"}:
            return None
        if re.fullmatch(r"[A-Z]{3}", s):
            return s
        m = re.search(r"[A-Z]{3}", s)
        return m.group(0) if m else None

    # ----------------- SAFE DERIVATIONS -----------------
    @staticmethod
    def derive_missing_tax(
        gross: Optional[Decimal],
        net: Optional[Decimal],
        tax: Optional[Decimal],
    ) -> Tuple[Optional[Decimal], str]:
        """
        If tax is None but gross and net are present, compute tax = gross - net.
        Returns (tax, provenance_note).
        """
        if tax is None and gross is not None and net is not None:
            try:
                return (gross - net, "derived: tax = gross - net")
            except Exception:
                return (None, "derive_failed: tax")
        return (tax, "")

    @staticmethod
    def default_fx_if_same_ccy(
        quote_ccy: Optional[str],
        settle_ccy: Optional[str],
        fx: Optional[Decimal],
    ) -> Tuple[Optional[Decimal], str]:
        """
        If fx is None and quote_ccy == settle_ccy (and both present), set fx = 1.0
        """
        if fx is None and quote_ccy and settle_ccy and quote_ccy == settle_ccy:
            return (Decimal("1.0"), "default: fx=1.0 (same ccy)")
        return (fx, "")

    # ------------------- internals -------------------
    @staticmethod
    def _strip(s):
        return s.strip() if isinstance(s, str) else s


