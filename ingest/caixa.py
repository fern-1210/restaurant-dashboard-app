"""
# What
Parse Caixa Geral de Depósitos (CGD) bank statement CSV exports into a normalized DataFrame.

# Why
CGD CSV exports have:
- a metadata "preamble" (account, company, period...) *before* the real table header
- Portuguese formatting (semicolon separator; comma decimals; dot thousands)
- Portuguese column names

We want to transform that into a consistent schema we can load into SQLite.

# How
1) Scan the file until we find the header row that starts with 'Data mov.'
2) Use pandas to read the table portion
3) Normalize columns into:
   account_id, bank, value_date, posting_date, description_raw, description_norm,
   amount, balance, currency, source_file, source_row, imported_at
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ingest.common import normalize_description, now_utc_iso, to_iso_date


@dataclass(frozen=True)
class CaixaParseResult:
    """
    We return both parsed rows and any rows we could not parse so you can audit issues.
    """

    transactions: pd.DataFrame
    unparsed_rows: pd.DataFrame


def _extract_account_id_from_preamble(lines: list[str]) -> str | None:
    """
    # What
    Try to extract the account number from the CSV preamble.

    # Why
    The preamble usually includes something like:
      Consultar saldos e movimentos ... ;=\"0310039741230\"
    Having a real account id in the table is useful when you have multiple accounts.

    # How
    Search the first ~20 lines for a long digit sequence and return it.
    """

    # We'll search only the beginning because preamble is at the top.
    head = lines[:25]

    # Regex: look for 10-20 consecutive digits (bank account id length can vary).
    pat = re.compile(r"(\d{10,20})")

    for line in head:
        m = pat.search(line)
        if m:
            return m.group(1)

    return None


def _find_table_header_line_index(lines: list[str]) -> int:
    """
    # What
    Find the line index where the actual transaction table header begins.

    # Why
    CGD exports include a preamble. If we feed the whole file to pandas,
    it won't parse correctly because the first lines are not CSV table rows.

    # How
    Iterate line by line until we see the header starting with 'Data mov.'.
    If not found, raise an error so you can inspect the raw file.
    """

    for idx, line in enumerate(lines):
        # Strip whitespace to avoid missing the header due to leading spaces.
        text = line.strip()

        # The real header row begins with "Data mov." in your samples.
        if text.startswith("Data mov."):
            return idx

    raise ValueError("Could not find Caixa transaction table header ('Data mov.')")


def parse_caixa_csv(path: Path, *, bank: str = "caixa", currency: str = "EUR") -> CaixaParseResult:
    """
    # What
    Parse one Caixa CSV statement file into normalized transactions.

    # Why
    Each monthly file should become rows in `bank_transactions`.

    # How
    - Read file as raw text (so we can locate where the table starts)
    - Read table with pandas starting at the header line
    - Map/normalize columns
    - Separate out unparsed rows (if date/amount can't be parsed)
    """

    # Read the raw file lines.
    #
    # Why latin1 here:
    # - Many Portuguese bank exports are encoded as Windows-1252 / ISO-8859-1 ("latin1")
    # - Reading as UTF-8 can fail with `UnicodeDecodeError` (we saw that in practice)
    # - latin1 can decode any byte 0x00-0xFF, so it is a robust default for these exports
    raw_bytes = path.read_bytes()
    raw_text = raw_bytes.decode("latin1", errors="replace")
    lines = raw_text.splitlines()

    # Extract account id if present; otherwise fallback to folder name later.
    account_id = _extract_account_id_from_preamble(lines)

    # Find where the real table begins.
    header_idx = _find_table_header_line_index(lines)

    # Load the "table portion" only (starting at header_idx).
    # - sep=';' because CGD uses semicolons
    # - decimal=',' because Portuguese decimal separator is comma
    # - thousands='.' because Portuguese thousands separator is dot
    df = pd.read_csv(
        path,
        sep=";",
        decimal=",",
        thousands=".",
        skiprows=header_idx,
        engine="python",
        encoding="latin1",
        dtype=str,  # read as strings first; we convert explicitly below
    )

    # CGD often has a trailing ';' which creates an extra unnamed column.
    # Drop any fully-empty columns to keep things clean.
    df = df.dropna(axis=1, how="all")

    # For consistent parsing, keep a copy of the original row number in this file section.
    # We add 1 because humans count rows starting at 1 (and it's easier to inspect).
    df["source_row"] = pd.RangeIndex(start=1, stop=len(df) + 1)

    # Identify the columns we need. We use "best effort" matching because encoding issues
    # can corrupt characters (you had 'Descriзгo' in the sample).
    cols = list(df.columns)

    def find_col(predicate) -> str | None:
        for c in cols:
            if predicate(c):
                return c
        return None

    col_posted_date = find_col(lambda c: str(c).strip().lower().startswith("data mov"))
    col_value_date = find_col(lambda c: str(c).strip().lower().startswith("data-valor"))
    col_amount = find_col(lambda c: str(c).strip().lower().startswith("montante"))
    col_balance = find_col(lambda c: "saldo" in str(c).strip().lower())
    col_desc = find_col(lambda c: "descr" in str(c).strip().lower())

    # Fail fast if critical columns are missing; otherwise you'd ingest garbage.
    # value_date (Data Valor) is primary; posting_date (Data mov.) is secondary.
    missing = [name for name, col in [
        ("value_date", col_value_date),
        ("amount", col_amount),
    ] if col is None]
    if missing:
        raise ValueError(f"Missing required CGD columns {missing} in {path.name}. Found: {cols}")

    # Build normalized output DataFrame with explicit columns.
    #
    # Important pandas detail (why we set index=df.index):
    # - If you start with an *empty* DataFrame and assign a scalar column (e.g. out["bank"]="caixa"),
    #   pandas creates an empty column (0 rows).
    # - Later, when you assign a *Series* column (e.g. dates/amounts), pandas expands the DataFrame
    #   to match that Series length, but the earlier scalar columns remain missing (NaN) for those rows.
    # - That would break SQLite inserts because fields like `bank` and `source_file` are NOT NULL.
    #
    # Setting `index=df.index` creates the correct number of rows up-front, so scalar assignments
    # fill all rows properly.
    out = pd.DataFrame(index=df.index)

    # Bank/account metadata
    out["bank"] = bank
    out["currency"] = currency
    out["source_file"] = path.name

    # Account id: use extracted digits if possible; otherwise fallback to folder name.
    out["account_id"] = account_id if account_id else path.parent.name

    # Dates: value_date (Data Valor) = main; posting_date (Data mov.) = when bank posted.
    out["value_date"] = to_iso_date(df[col_value_date])
    out["posting_date"] = to_iso_date(df[col_posted_date]) if col_posted_date else None

    # Description fields.
    out["description_raw"] = df[col_desc] if col_desc else None
    out["description_norm"] = (df[col_desc].apply(normalize_description) if col_desc else None)

    # Amount: pandas read it as string; convert to numeric.
    # We read with decimal/thousands so numeric text often converts cleanly,
    # but because we used dtype=str, we explicitly convert here.
    # Amount values are Portuguese-formatted strings like:
    #   "-685,00"   or   "1.185,08"
    # So we normalize them into "machine" format:
    #   "-685.00"   or   "1185.08"
    out["amount"] = pd.to_numeric(
        df[col_amount]
        .astype(str)
        .str.replace(" ", "", regex=False)   # remove spaces
        .str.replace(".", "", regex=False)   # remove thousands separators
        .str.replace(",", ".", regex=False), # convert decimal comma to decimal dot
        errors="coerce",
    )

    # Balance is optional.
    if col_balance:
        out["balance"] = pd.to_numeric(
            df[col_balance]
            .astype(str)
            .str.replace(" ", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False),
            errors="coerce",
        )
    else:
        out["balance"] = None

    # Lineage fields.
    out["source_row"] = df["source_row"].astype(int)
    out["imported_at"] = now_utc_iso()

    # Define what "unparsed" means:
    # - value_date couldn't be parsed to a date
    # - amount couldn't be parsed to a number
    bad_mask = out["value_date"].isna() | out["amount"].isna()

    unparsed = out.loc[bad_mask].copy()
    parsed = out.loc[~bad_mask].copy()

    # Reset indexes for cleanliness (optional but nice for inspection).
    parsed = parsed.reset_index(drop=True)
    unparsed = unparsed.reset_index(drop=True)

    return CaixaParseResult(transactions=parsed, unparsed_rows=unparsed)

