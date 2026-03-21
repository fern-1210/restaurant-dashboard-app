"""
# What
SQLite schema (tables + indexes) for the local BI warehouse.

# Why
SQLite doesn't have migrations by default, and for an MVP we want the schema to be:
- explicit (easy to inspect)
- created automatically on first run
- idempotent (safe to re-run, won't destroy data)

# How
We run `CREATE TABLE IF NOT EXISTS ...` and `CREATE INDEX IF NOT EXISTS ...`.
"""

from __future__ import annotations

import sqlite3


def create_revenue_daily_table(conn: sqlite3.Connection) -> None:
    """
    # What
    Create the `revenue_daily` table.

    # Why
    This is our first trusted "fact table": one row per day of revenue.
    Making it a proper table (instead of only CSV) establishes the warehouse pattern.

    # How
    We store dates as ISO text (`YYYY-MM-DD`) to keep things simple in SQLite,
    and we use `date` as a primary key because it's one row per day.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS revenue_daily (
            date TEXT PRIMARY KEY,
            sales_gross REAL,
            sales_net REAL,
            costs REAL,
            profit REAL,
            num_sales REAL,
            quantity REAL,
            source_file TEXT,
            imported_at TEXT
        );
        """
    )


def create_bank_transactions_table(conn: sqlite3.Connection) -> None:
    """
    # What
    Create the `bank_transactions` table.

    # Why
    Bank statements are the backbone for expenses and cash-flow analysis.
    We want a normalized representation that is consistent across banks and file formats.

    # How
    - value_date (Data Valor) = main transaction date; used for period filtering.
    - posting_date (Data Lancamento) = when bank posted; kept for audit.
    - Store amount as signed float: credits positive, debits negative.
    - Keep lineage fields (source_file, source_row) for audit + debug.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            bank TEXT NOT NULL,
            value_date TEXT NOT NULL,
            posting_date TEXT,
            description_raw TEXT,
            description_norm TEXT,
            amount REAL NOT NULL,
            balance REAL,
            currency TEXT NOT NULL DEFAULT 'EUR',
            source_file TEXT NOT NULL,
            source_row INTEGER NOT NULL,
            imported_at TEXT NOT NULL
        );
        """
    )

    # Migrate legacy schema (posted_date) to new (posting_date, value_date primary).
    _migrate_bank_transactions_date_columns(conn)

    # Duplicate protection (idempotent loads).
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_bank_transactions_source_row
        ON bank_transactions (
            bank,
            account_id,
            value_date,
            amount,
            description_raw,
            source_file,
            source_row
        );
        """
    )

    # Index for date-range filtering.
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_bank_transactions_value_date
        ON bank_transactions (value_date);
        """
    )


def _migrate_bank_transactions_date_columns(conn: sqlite3.Connection) -> None:
    """
    Migrate bank_transactions from posted_date to value_date+posting_date schema.

    - posted_date (Data Lancamento) -> posting_date
    - value_date (Data Valor) stays; becomes primary for filtering
    """
    cur = conn.execute("PRAGMA table_info(bank_transactions)")
    cols = {row[1] for row in cur.fetchall()}
    if "posted_date" not in cols:
        return
    # Backfill value_date where null so NOT NULL constraint can apply later.
    conn.execute(
        "UPDATE bank_transactions SET value_date = posted_date WHERE value_date IS NULL"
    )
    conn.execute("ALTER TABLE bank_transactions RENAME COLUMN posted_date TO posting_date")
    conn.commit()
    # Recreate indexes (old ones referenced posted_date).
    conn.execute("DROP INDEX IF EXISTS ux_bank_transactions_source_row")
    conn.execute("DROP INDEX IF EXISTS ix_bank_transactions_posted_date")


def create_transaction_category_map_table(conn: sqlite3.Connection) -> None:
    """
    # What
    Create the `transaction_category_map` table.

    # Why
    We want a separate, editable lookup that maps normalized bank descriptions
    to reporting categories. Keeping this outside raw transactions means your
    partner can update categories without re-ingesting bank files.

    # How
    - Store one row per `description_norm` key.
    - Keep 2 category levels for today's MVP (`category`, `subcategory`).
    - Track source + timestamps for auditability.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transaction_category_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description_norm TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT NOT NULL,
            source_label TEXT NOT NULL DEFAULT 'manual_excel',
            notes TEXT,
            updated_at TEXT NOT NULL
        );
        """
    )

    # Keep mapping deterministic: one normalized description -> one category pair.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_transaction_category_map_description_norm
        ON transaction_category_map (description_norm);
        """
    )

    # Helpful when filtering category reports.
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_transaction_category_map_category
        ON transaction_category_map (category, subcategory);
        """
    )


def create_category_taxonomy_table(conn: sqlite3.Connection) -> None:
    """
    # What
    Create the `category_taxonomy` table.

    # Why
    Your current Excel contains the category structure (levels) but not always
    transaction description keys. This table stores the approved hierarchy
    separately so mapping can catch up later without losing taxonomy work.

    # How
    - Keep level_1, level_2, optional level_3.
    - Enforce uniqueness across the full hierarchy row.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS category_taxonomy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level_1 TEXT NOT NULL,
            level_2 TEXT NOT NULL,
            level_3 TEXT,
            source_label TEXT NOT NULL DEFAULT 'manual_excel',
            updated_at TEXT NOT NULL
        );
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_category_taxonomy_levels
        ON category_taxonomy (level_1, level_2, level_3);
        """
    )


def create_all_tables(conn: sqlite3.Connection) -> None:
    """
    # What
    Create all tables and indexes needed for Chunk 2–3.

    # Why
    We want each loader script to be able to call one function that guarantees
    the DB is ready for use.

    # How
    Call the individual create_* functions and commit the changes.
    """

    create_revenue_daily_table(conn)
    create_bank_transactions_table(conn)
    create_transaction_category_map_table(conn)
    create_category_taxonomy_table(conn)
    conn.commit()

