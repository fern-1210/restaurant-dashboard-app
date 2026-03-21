"""
# What
Parse Millennium BCP combined statement exports (XLS) into a normalized DataFrame.

# Why
Your Millennium files are `.xls` (Excel 97-2003 format) and typically contain:
- one or more header rows
- Portuguese column names
- possibly separate debit/credit columns

We want to normalize those into the same schema as other bank sources so the
warehouse table stays consistent.

# How
1) Read the XLS with pandas
2) Normalize header labels to match common possibilities (Portuguese variations)
3) Build a normalized DataFrame with consistent columns
4) Filter out rows that don't look like transactions (unparseable date/amount)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ingest.common import normalize_description, normalize_header, now_utc_iso


@dataclass(frozen=True)
class MillenniumParseResult:
    transactions: pd.DataFrame
    unparsed_rows: pd.DataFrame


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    # What
    Choose the first column in df whose normalized name matches one of the candidates.

    # Why
    Different exports label columns differently (e.g. 'Descrição' vs 'Descritivo').

    # How
    Compare normalized header strings using `normalize_header()`.
    """

    # Build a mapping from normalized header -> original header.
    norm_to_orig: dict[str, str] = {normalize_header(c): c for c in df.columns}

    # Iterate candidates in priority order and return the first match.
    for cand in candidates:
        key = normalize_header(cand)
        if key in norm_to_orig:
            return norm_to_orig[key]

    return None


def _to_iso_date_any(s: pd.Series, *, dayfirst: bool = False) -> pd.Series:
    """
    # What
    Convert a series of date-like values into ISO date strings.

    # Why
    Millennium .xlsx uses DD/MM/YYYY (dayfirst=True); .xls/TSV uses YYYY/MM/DD.
    Excel date objects parse without dayfirst.

    # How
    Let pandas parse with optional dayfirst, then format as `YYYY-MM-DD`.
    """
    dt = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)
    return dt.dt.date.astype("string")


def _is_real_xls(path: Path) -> bool:
    """
    # What
    Detect whether a file is a real binary `.xls` workbook (OLE2/BIFF).

    # Why
    Some bank portals save "XLS" files that are actually tab-separated text
    (as in your Millennium samples).

    # How
    Real legacy Excel `.xls` files start with the OLE2 magic header:
      D0 CF 11 E0 A1 B1 1A E1
    """

    magic = path.read_bytes()[:8]
    return magic == b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"


def _read_millennium_table_from_text(path: Path) -> pd.DataFrame:
    """
    # What
    Read Millennium "XLS" that is actually tab-separated text.

    # Why
    Your file starts with human-readable lines and then a tab-separated table:
      Companhia\tProduto\tConta\tMoeda\tData Lancamento\t...

    # How
    - Decode as latin1 (robust for Portuguese exports)
    - Find the header line starting with 'Companhia'
    - Use pandas.read_csv with sep='\\t'
    """

    text = path.read_text(encoding="latin1", errors="replace")
    lines = text.splitlines()

    header_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("Companhia\t"):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(f"Could not find Millennium table header line in: {path.name}")

    # Read from the header line down; pandas can treat the file as a TSV.
    df = pd.read_csv(
        path,
        sep="\t",
        decimal=",",
        thousands=".",
        skiprows=header_idx,
        encoding="latin1",
        dtype=str,  # read as strings first; we convert explicitly below
    )

    # Drop empty columns that might appear due to trailing separators.
    df = df.dropna(axis=1, how="all")

    return df


def parse_millennium_xls(path: Path, *, bank: str = "millennium", currency: str = "EUR") -> MillenniumParseResult:
    """
    # What
    Parse one Millennium `.xls` file and return normalized transactions.

    # Why
    This powers Chunk 3 ingestion of bank statements into SQLite.

    # How
    - Read excel
    - Guess column meanings via candidate lists
    - Compute signed `amount`
    - Normalize/clean and split parsed vs unparsed
    """

    # Read the Millennium export file.
    #
    # Important discovery from your real files:
    # - .xlsx: modern Excel; use openpyxl. Has 8 metadata rows, then header row 8 (0-based).
    # - .xls: either real OLE2/BIFF or tab-separated text with .xls extension.
    if path.suffix.lower() == ".xlsx":
        df = pd.read_excel(path, engine="openpyxl", header=7)
    elif _is_real_xls(path):
        df = pd.read_excel(path, engine="xlrd")
    else:
        df = _read_millennium_table_from_text(path)

    # Drop fully-empty rows/columns that commonly appear in exports.
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    # Add a source row number (1-based) for debugging/auditing.
    df = df.reset_index(drop=True)
    df["source_row"] = pd.RangeIndex(start=1, stop=len(df) + 1)

    # Identify columns by name heuristics.
    col_date = _pick_column(df, ["data lancamento", "data lançamento", "data", "data mov", "data movimento", "data mov."])
    col_value_date = _pick_column(df, ["data valor", "data-valor", "data valor."])
    col_desc = _pick_column(df, ["descricao", "descrição", "descritivo", "designacao", "descr"])
    col_amount = _pick_column(df, ["valor", "montante", "importe"])
    col_debit = _pick_column(df, ["debito", "débito"])
    col_credit = _pick_column(df, ["credito", "crédito"])
    col_balance = _pick_column(df, ["saldo", "saldo contabilistico", "saldo contabilístico"])
    col_account = _pick_column(df, ["conta"])

    # Fail fast if we can't find a date column; without it, transactions are unusable.
    if col_date is None:
        raise ValueError(f"Could not identify a date column in Millennium file: {path.name}")

    # Build normalized output.
    #
    # Same pandas detail as in the Caixa parser:
    # we pre-create the output DataFrame with the right index so scalar assignments
    # (bank/currency/source_file) populate all rows rather than becoming NaN.
    out = pd.DataFrame(index=df.index)
    out["bank"] = bank
    out["currency"] = currency
    out["source_file"] = path.name

    # Account id: for now, use the parent folder name, which is stable in your repo.
    # If the file includes a 'Conta' column (as in your sample), that is the best account id.
    # Otherwise we fallback to the folder name.
    out["account_id"] = df[col_account] if col_account else path.parent.name

    # Dates: value_date (Data Valor) = main; posting_date (Data Lancamento) = when bank posted.
    # xlsx uses DD/MM/YYYY; xls/TSV uses YYYY/MM/DD.
    dayfirst = path.suffix.lower() == ".xlsx"
    _date = lambda ser: _to_iso_date_any(ser, dayfirst=dayfirst)
    out["value_date"] = _date(df[col_value_date]) if col_value_date else _date(df[col_date])
    out["posting_date"] = _date(df[col_date]) if col_date else None

    # Descriptions.
    out["description_raw"] = df[col_desc] if col_desc else None
    out["description_norm"] = (df[col_desc].apply(normalize_description) if col_desc else None)

    # Amount rules:
    # - If there's a single amount column: use it as signed number (assuming export already signs it)
    # - Else if there are debit/credit columns: amount = credit - debit
    if col_amount:
        raw = df[col_amount]
        if pd.api.types.is_numeric_dtype(raw):
            out["amount"] = pd.to_numeric(raw, errors="coerce")
        else:
            # Portuguese string format: 1.185,08 (thousands=., decimal=,)
            out["amount"] = pd.to_numeric(
                raw.astype(str)
                .str.replace(" ", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False),
                errors="coerce",
            )
    elif col_debit and col_credit:
        debit = pd.to_numeric(df[col_debit], errors="coerce").fillna(0)
        credit = pd.to_numeric(df[col_credit], errors="coerce").fillna(0)
        out["amount"] = credit - debit
    else:
        # No clear amount columns found.
        out["amount"] = pd.Series([None] * len(df))

    # Balance is optional.
    if col_balance:
        out["balance"] = pd.to_numeric(
            df[col_balance].astype(str).str.replace(" ", "", regex=False).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
            errors="coerce",
        )
    else:
        out["balance"] = None

    # Lineage/import metadata.
    out["source_row"] = df["source_row"].astype(int)
    out["imported_at"] = now_utc_iso()

    # Filter out "non-transaction" rows:
    # - value_date missing => not a real transaction row
    # - amount missing => not usable for financial analysis
    bad_mask = out["value_date"].isna() | out["amount"].isna()

    unparsed = out.loc[bad_mask].copy().reset_index(drop=True)
    parsed = out.loc[~bad_mask].copy().reset_index(drop=True)

    return MillenniumParseResult(transactions=parsed, unparsed_rows=unparsed)

