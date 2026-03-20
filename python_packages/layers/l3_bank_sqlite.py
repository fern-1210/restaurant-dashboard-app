"""
# What
Layer 3 entrypoint: bank statement files (Caixa CSV, Millennium XLS) → SQLite bank_transactions.

# Why
This wrapper makes the L3 data flow visible in one place. Orchestration scripts
import from here instead of multiple modules, so the "layer story" is obvious.

# How
Delegates to ingest.caixa and ingest.millennium for parsing, python_packages.bank_sqlite
for the insert. Uses db, paths, schema for connection and table setup.
"""

from __future__ import annotations

import pandas as pd

from ingest.caixa import parse_caixa_csv
from ingest.millennium import parse_millennium_xls
from python_packages.bank_sqlite import insert_bank_transactions
from python_packages.db import connect_sqlite
from python_packages.paths import DB_PATH, RAW_BANK_DIR, REPORTS_DIR
from python_packages.schema import create_all_tables


def _list_caixa_files() -> list:
    """List Caixa CSV files under the bank statements directory."""
    caixa_dir = RAW_BANK_DIR / "account_1-Caixa-Geral-Depositos"
    return sorted(caixa_dir.glob("*.csv"))


def _list_millennium_files() -> list:
    """List Millennium XLS files under the bank statements directory."""
    mil_dir = RAW_BANK_DIR / "account_2-Millennium-bcp"
    return sorted(mil_dir.glob("*.xls"))


def _within_batch_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Find duplicates within the DataFrame before insert (diagnostic only)."""
    key_cols = ["bank", "account_id", "posted_date", "amount", "description_raw", "source_file", "source_row"]
    dup_mask = df.duplicated(subset=key_cols, keep=False)
    return df.loc[dup_mask].sort_values(key_cols).reset_index(drop=True)


def run_l3_bank_sqlite() -> int:
    """
    # What
    Run Layer 3: ingest bank statements into SQLite bank_transactions table.

    # Why
    Single entrypoint so L3 script stays thin and layer flow is obvious.

    # How
    - Connect, ensure schema
    - Parse Caixa CSV and Millennium XLS via ingest parsers
    - Insert into SQLite with INSERT OR IGNORE (idempotent)
    - Write audit CSVs, print summary, return 0 on success

    Returns:
        0 on success.
    """
    conn = connect_sqlite(DB_PATH)
    create_all_tables(conn)

    all_txns: list[pd.DataFrame] = []
    all_unparsed: list[pd.DataFrame] = []

    for path in _list_caixa_files():
        parsed = parse_caixa_csv(path)
        all_txns.append(parsed.transactions)
        if not parsed.unparsed_rows.empty:
            all_unparsed.append(parsed.unparsed_rows.assign(parse_source="caixa_csv"))

    for path in _list_millennium_files():
        parsed = parse_millennium_xls(path)
        all_txns.append(parsed.transactions)
        if not parsed.unparsed_rows.empty:
            all_unparsed.append(parsed.unparsed_rows.assign(parse_source="millennium_xls"))

    if not all_txns:
        raise FileNotFoundError(f"No bank statement files found under: {RAW_BANK_DIR}")

    txns = pd.concat(all_txns, ignore_index=True) if all_txns else pd.DataFrame()
    dup_df = _within_batch_duplicates(txns) if not txns.empty else pd.DataFrame()

    attempted, inserted = insert_bank_transactions(conn, txns)
    ignored = attempted - inserted

    conn.close()

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    audit_rows: list[dict] = []
    audit_rows.append({"metric": "files_caixa_csv", "value": len(_list_caixa_files())})
    audit_rows.append({"metric": "files_millennium_xls", "value": len(_list_millennium_files())})
    audit_rows.append({"metric": "rows_attempted_insert", "value": attempted})
    audit_rows.append({"metric": "rows_inserted", "value": inserted})
    audit_rows.append({"metric": "rows_ignored_duplicates", "value": ignored})
    audit_rows.append({"metric": "within_batch_duplicates_rows", "value": int(len(dup_df))})

    if not txns.empty:
        audit_rows.append({"metric": "posted_date_min", "value": str(txns["posted_date"].min())})
        audit_rows.append({"metric": "posted_date_max", "value": str(txns["posted_date"].max())})
        credits = txns.loc[txns["amount"] > 0, "amount"].sum()
        debits = txns.loc[txns["amount"] < 0, "amount"].sum()
        audit_rows.append({"metric": "credits_sum", "value": float(credits)})
        audit_rows.append({"metric": "debits_sum", "value": float(debits)})
        audit_rows.append({"metric": "net_sum", "value": float(credits + debits)})

    audit_path = REPORTS_DIR / "bank_transactions_audit.csv"
    pd.DataFrame(audit_rows).to_csv(audit_path, index=False)

    dup_path = REPORTS_DIR / "bank_transactions_duplicates.csv"
    dup_df.to_csv(dup_path, index=False)

    unparsed_path = REPORTS_DIR / "bank_transactions_unparsed_rows.csv"
    if all_unparsed:
        pd.concat(all_unparsed, ignore_index=True).to_csv(unparsed_path, index=False)
    else:
        pd.DataFrame(columns=list(txns.columns) + ["parse_source"]).to_csv(unparsed_path, index=False)

    print("OK: bank statements loaded into SQLite")
    print(f"- DB: {DB_PATH}")
    print(f"- Attempted inserts: {attempted}")
    print(f"- Inserted: {inserted}")
    print(f"- Ignored (duplicates): {ignored}")
    print(f"- Audit: {audit_path}")
    print(f"- Duplicates (within batch): {dup_path}")
    print(f"- Unparsed rows: {unparsed_path}")

    return 0
