"""
# What
Load the trusted daily revenue CSV into SQLite (`revenue_daily` table).

# Why
You already trust the revenue CSV output (Layer 1).
Loading it into SQLite establishes a repeatable pattern for:
- creating tables
- idempotent loads (safe to re-run)
- producing audit outputs

# How
We read `data/warehouse/revenue_daily.csv` with pandas, normalize types,
and then upsert into SQLite by the `date` primary key.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import sqlite3


@dataclass(frozen=True)
class RevenueLoadResult:
    """
    Small structured return object so scripts can print/log what happened.
    """

    rows_in_csv: int
    rows_upserted: int
    audit_path: Path


def _now_utc_iso() -> str:
    """
    # What
    Return the current time as an ISO string in UTC.

    # Why
    We store `imported_at` so later we can see when the warehouse was last refreshed.

    # How
    `datetime.now(timezone.utc).isoformat()` gives a timezone-aware ISO timestamp.
    """

    return datetime.now(timezone.utc).isoformat()


def _to_iso_date_series(s: pd.Series) -> pd.Series:
    """
    # What
    Convert a pandas Series into ISO date strings (`YYYY-MM-DD`).

    # Why
    The CSV stores `date` already as strings like `2024-01-02`, but we want to be
    defensive: if parsing ever changes, SQLite still gets a consistent date format.

    # How
    - Parse to datetime with pandas
    - Convert to date
    - Format back to ISO string
    """

    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.date.astype("string")


def load_revenue_daily_csv_to_sqlite(
    *,
    conn: sqlite3.Connection,
    revenue_csv_path: Path,
    audit_out_path: Path,
) -> RevenueLoadResult:
    """
    # What
    Read `revenue_daily.csv` and upsert into SQLite table `revenue_daily`.

    # Why
    - We want the DB to be the storage/query layer for dashboards and analysis.
    - We want re-runs to be safe and not duplicate data.

    # How
    - Load CSV with pandas
    - Normalize types
    - For each row, insert or update by the primary key (`date`)
    - Write a small audit CSV so you can trust what happened
    """

    # ---- Load the CSV into a DataFrame ----

    # Read the CSV you already generate from Vendus exports (trusted output).
    df = pd.read_csv(revenue_csv_path)

    # Keep a reference count so we can report it in the result/audit.
    rows_in_csv = int(len(df))

    # If the file is empty (or missing), it's better to fail loudly than to create
    # an empty warehouse table that looks "successful".
    if rows_in_csv == 0:
        raise ValueError(f"Revenue CSV is empty: {revenue_csv_path}")

    # ---- Normalize / clean types ----

    # Ensure `date` is a consistent ISO date string.
    df["date"] = _to_iso_date_series(df["date"])

    # Coerce numeric columns so SQLite receives real numbers (or NULL).
    # We intentionally list the known columns so unexpected columns don't silently leak in.
    numeric_cols = [
        "sales_gross",
        "sales_net",
        "costs",
        "profit",
        "num_sales",
        "quantity",
    ]
    for col in numeric_cols:
        # Some columns may be missing if the Vendus export changed; that's OK.
        # We only convert columns that are present.
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure `source_file` exists (your CSV has it, but we keep it defensive).
    if "source_file" not in df.columns:
        df["source_file"] = None

    # Timestamp for this load operation.
    imported_at = _now_utc_iso()

    # ---- Upsert into SQLite ----

    # Use one cursor for the whole batch (faster + simpler).
    cur = conn.cursor()

    # We'll count how many rows we attempted to upsert (same as rows in CSV).
    rows_upserted = 0

    # Prepared statement: insert new row or update existing row on date conflict.
    # This makes the load idempotent: re-running simply updates the same dates.
    sql = """
        INSERT INTO revenue_daily (
            date,
            sales_gross,
            sales_net,
            costs,
            profit,
            num_sales,
            quantity,
            source_file,
            imported_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            sales_gross = excluded.sales_gross,
            sales_net   = excluded.sales_net,
            costs       = excluded.costs,
            profit      = excluded.profit,
            num_sales   = excluded.num_sales,
            quantity    = excluded.quantity,
            source_file = excluded.source_file,
            imported_at = excluded.imported_at
    """

    # Iterate row-by-row so it's easiest to understand while learning.
    # (We can optimize later if needed.)
    for _, row in df.iterrows():
        # Pull the scalar values out of the row in the same order as the SQL placeholders.
        cur.execute(
            sql,
            (
                row.get("date"),
                row.get("sales_gross"),
                row.get("sales_net"),
                row.get("costs"),
                row.get("profit"),
                row.get("num_sales"),
                row.get("quantity"),
                row.get("source_file"),
                imported_at,
            ),
        )
        rows_upserted += 1

    # Persist the writes to disk.
    conn.commit()

    # ---- Audit output ----

    # Read back summary information from SQLite so we can compare to the CSV.
    # These are common sanity checks: row counts, date range, sums.
    audit_rows: list[dict[str, object]] = []

    # CSV-side stats
    audit_rows.append({"metric": "csv_rows", "value": rows_in_csv})
    audit_rows.append({"metric": "csv_date_min", "value": str(df["date"].min())})
    audit_rows.append({"metric": "csv_date_max", "value": str(df["date"].max())})
    audit_rows.append({"metric": "csv_sales_gross_sum", "value": float(df["sales_gross"].fillna(0).sum())})
    audit_rows.append({"metric": "csv_sales_net_sum", "value": float(df["sales_net"].fillna(0).sum())})

    # SQLite-side stats
    db_row = conn.execute(
        """
        SELECT
            COUNT(*) AS rows_count,
            MIN(date) AS date_min,
            MAX(date) AS date_max,
            SUM(COALESCE(sales_gross, 0)) AS sales_gross_sum,
            SUM(COALESCE(sales_net, 0)) AS sales_net_sum
        FROM revenue_daily
        """
    ).fetchone()

    # fetchone() returns a tuple in the same order as the SELECT fields.
    audit_rows.append({"metric": "sqlite_rows", "value": int(db_row[0])})
    audit_rows.append({"metric": "sqlite_date_min", "value": str(db_row[1])})
    audit_rows.append({"metric": "sqlite_date_max", "value": str(db_row[2])})
    audit_rows.append({"metric": "sqlite_sales_gross_sum", "value": float(db_row[3] or 0.0)})
    audit_rows.append({"metric": "sqlite_sales_net_sum", "value": float(db_row[4] or 0.0)})

    # Load metadata
    audit_rows.append({"metric": "rows_upserted_this_run", "value": rows_upserted})
    audit_rows.append({"metric": "imported_at_utc", "value": imported_at})

    # Write the audit CSV to disk for easy inspection in a spreadsheet.
    audit_df = pd.DataFrame(audit_rows)
    audit_out_path.parent.mkdir(parents=True, exist_ok=True)
    audit_df.to_csv(audit_out_path, index=False)

    return RevenueLoadResult(
        rows_in_csv=rows_in_csv,
        rows_upserted=rows_upserted,
        audit_path=audit_out_path,
    )

