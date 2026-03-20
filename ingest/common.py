"""
# What
Shared helpers for ingestion/normalization across different sources (banks).

# Why
When you ingest multiple banks, you want the *resulting* table to be consistent:
- consistent date format
- consistent amount sign convention
- consistent description cleanup

# How
This module holds small, reusable functions that each parser can call.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone

import pandas as pd


def now_utc_iso() -> str:
    """
    # What
    Current timestamp in UTC as ISO string.

    # Why
    We store `imported_at` for lineage: when did we ingest these rows.

    # How
    Use timezone-aware datetime and `isoformat()`.
    """

    return datetime.now(timezone.utc).isoformat()


def strip_accents(s: str) -> str:
    """
    # What
    Remove accent marks from a string (e.g. 'Descrição' -> 'Descricao').

    # Why
    Bank exports are Portuguese and can include accents. For header matching and
    normalization we want stable ASCII-like keys.

    # How
    Unicode normalize (NFD) splits letters and accents, then we drop accent chars.
    """

    # Normalize to "decomposed" form where accents are separate codepoints.
    normalized = unicodedata.normalize("NFD", s)

    # Keep only characters that are NOT combining marks (i.e., not accents).
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_header(s: str) -> str:
    """
    # What
    Normalize a header label into a comparable form.

    # Why
    Different exports use slightly different labels/casing/spaces. We want to match
    columns even if the exact header text varies.

    # How
    - convert to string
    - strip whitespace
    - remove accents
    - lowercase
    - collapse repeated whitespace
    """

    # Convert anything into a string defensively.
    raw = str(s) if s is not None else ""

    # Remove leading/trailing whitespace.
    raw = raw.strip()

    # Remove accents so 'Descrição' and 'Descricao' match.
    raw = strip_accents(raw)

    # Lowercase for stable comparisons.
    raw = raw.lower()

    # Collapse internal whitespace into single spaces.
    raw = re.sub(r"\s+", " ", raw)

    return raw


def normalize_description(s: str | None) -> str | None:
    """
    # What
    Normalize a transaction description string.

    # Why
    Descriptions are messy (extra spaces, mixed casing). Normalizing makes:
    - searching easier
    - grouping/summarizing easier

    # How
    - handle None
    - strip whitespace
    - collapse repeated spaces
    - uppercase (simple, stable)
    """

    # If the source has no description, keep it as None.
    if s is None:
        return None

    # Convert to string in case pandas gives us non-str objects.
    text = str(s)

    # Remove leading/trailing whitespace.
    text = text.strip()

    # Replace internal runs of whitespace (tabs/newlines/multiple spaces) with single space.
    text = re.sub(r"\s+", " ", text)

    # Uppercase is a simple normalization that works well for grouping.
    return text.upper()


def to_iso_date(s: pd.Series) -> pd.Series:
    """
    # What
    Convert a pandas Series of dates into ISO date strings.

    # Why
    Bank files often use `DD-MM-YYYY`. SQLite storage will be ISO `YYYY-MM-DD`.

    # How
    - Use pandas parsing with `dayfirst=True`
    - Coerce invalid values to NaT
    - Convert to date and then to string
    """

    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return dt.dt.date.astype("string")

