"""
# What
Layer 2 entrypoint: revenue_daily.csv → SQLite revenue_daily table.

# Why
This wrapper makes the L2 data flow visible in one place. Orchestration scripts
import from here instead of multiple modules, so the "layer story" is obvious.

# How
Delegates to scripts_pipeline.revenue_sqlite for the CSV→SQLite load.
Uses db, paths, schema for connection and table setup.
"""

from __future__ import annotations

from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import DB_PATH, REPORTS_DIR, REVENUE_DAILY_CSV_PATH
from scripts_pipeline.revenue_sqlite import load_revenue_daily_csv_to_sqlite
from scripts_pipeline.schema import create_all_tables


def run_l2_revenue_sqlite() -> int:
    """
    # What
    Run Layer 2: load revenue_daily.csv into SQLite revenue_daily table.

    # Why
    Single entrypoint so L2 script stays thin and layer flow is obvious.

    # How
    - Connect to SQLite, ensure schema exists
    - Call revenue_sqlite.load_revenue_daily_csv_to_sqlite (idempotent upsert)
    - Print summary, return 0 on success

    Returns:
        0 on success.
    """
    conn = connect_sqlite(DB_PATH)
    create_all_tables(conn)

    audit_path = REPORTS_DIR / "revenue_sqlite_audit.csv"
    result = load_revenue_daily_csv_to_sqlite(
        conn=conn,
        revenue_csv_path=REVENUE_DAILY_CSV_PATH,
        audit_out_path=audit_path,
    )

    conn.close()

    print("OK: revenue loaded into SQLite")
    print(f"- DB: {DB_PATH}")
    print(f"- CSV rows: {result.rows_in_csv}")
    print(f"- Rows upserted this run: {result.rows_upserted}")
    print(f"- Audit: {result.audit_path}")

    return 0
