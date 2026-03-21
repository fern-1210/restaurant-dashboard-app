"""
# What
Load normalized bank transactions DataFrames into SQLite (`bank_transactions` table).

# Why
We want a single place that defines:
- the INSERT statement for `bank_transactions`
- how we make loads idempotent (avoid duplicates on rerun)
- how we count inserted vs ignored rows for audits

# How
We use `INSERT OR IGNORE` so that rows violating the unique index are skipped.
Then we compare SQLite `total_changes` before/after to estimate how many inserts happened.
"""

from __future__ import annotations

import sqlite3

import pandas as pd


def insert_bank_transactions(conn: sqlite3.Connection, transactions: pd.DataFrame) -> tuple[int, int]:
    """
    # What
    Insert a normalized transaction DataFrame into SQLite.

    # Why
    - We want reruns to be safe (no duplicate rows).
    - We want to audit how many rows were inserted vs ignored.

    # How
    - Convert the DataFrame into a list of tuples in the exact column order
    - Use `executemany()` with `INSERT OR IGNORE`
    - Calculate inserted rows via `conn.total_changes`
    """

    # If there are no rows, there's nothing to do.
    if transactions.empty:
        return (0, 0)

    # Define the column order to match the SQL insert statement.
    cols = [
        "account_id",
        "bank",
        "value_date",
        "posting_date",
        "description_raw",
        "description_norm",
        "amount",
        "balance",
        "currency",
        "source_file",
        "source_row",
        "imported_at",
    ]

    # Make sure all required columns exist; fail fast if a parser didn't provide them.
    missing = [c for c in cols if c not in transactions.columns]
    if missing:
        raise ValueError(f"Transaction DataFrame missing columns: {missing}")

    # Convert DataFrame rows to tuples; replace pandas NA/NaT with None for SQLite.
    def _to_sqlite_val(x):
        if pd.isna(x):
            return None
        return x

    rows = [tuple(_to_sqlite_val(v) for v in transactions.loc[i, cols].tolist()) for i in range(len(transactions))]

    # How many rows we are attempting to insert.
    attempted = len(rows)

    # Track total changes before insert so we can compute how many rows changed.
    before = conn.total_changes

    # Insert, skipping duplicates (because unique index would reject them).
    conn.executemany(
        """
        INSERT OR IGNORE INTO bank_transactions (
            account_id,
            bank,
            value_date,
            posting_date,
            description_raw,
            description_norm,
            amount,
            balance,
            currency,
            source_file,
            source_row,
            imported_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    # Persist the inserts.
    conn.commit()

    # Inserted rows are the difference in total changes.
    inserted = conn.total_changes - before

    return (attempted, inserted)

