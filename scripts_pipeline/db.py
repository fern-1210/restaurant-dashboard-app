"""
# What
SQLite connection helpers.

# Why
We want every script to:
- open the same SQLite database file
- set SQLite pragmas consistently (safety + performance)
- ensure required folders exist before creating the DB file

# How
We use Python's built-in sqlite3 module (no extra dependency).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def ensure_dir(path: Path) -> None:
    """
    # What
    Create a directory if it doesn't exist.

    # Why
    The database file lives under `data/warehouse/`. If that folder doesn't exist,
    SQLite cannot create the DB file and we'll get confusing errors.

    # How
    `mkdir(parents=True, exist_ok=True)` creates all missing parents and does nothing
    if the directory already exists.
    """

    # The folder we want to ensure exists might be nested (data/warehouse/).
    path.mkdir(parents=True, exist_ok=True)


def connect_sqlite(db_path: Path) -> sqlite3.Connection:    
    """
    # What
    Open a connection to a SQLite database file.

    # Why
    We want a single, predictable way to connect so that:
    - foreign keys are enabled (SQLite defaults to OFF)
    - busy timeout is set (avoid 'database is locked' during short write conflicts)
    - we can use row_factory when we need dict-like rows later

    # How
    We call `sqlite3.connect()` and then set a few SQLite PRAGMA settings.
    """

    # Ensure the parent directory exists before SQLite tries to create the file.
    ensure_dir(db_path.parent)

    # Create/open the DB file.
    conn = sqlite3.connect(db_path)

    # Make SQLite return rows as tuples by default; we can change later if needed.
    # (Leaving default for now keeps things simple.)

    #Julian note : below 3 are standard practise, these are all best practices for SQLite connections and is a good way to ensure that the database is always in a good state and is not corrupted.

    # Enable foreign keys (important once we have related tables).
    conn.execute("PRAGMA foreign_keys = ON;")

    # Set a small busy timeout so write operations wait briefly if the DB is busy.
    conn.execute("PRAGMA busy_timeout = 5000;")  # milliseconds

    # WAL mode improves concurrency for read-heavy workloads (like dashboards); readers and writers can coexist. this created the venn.db-shm and venn.db-wal inside data/warehouse/*venn.db
    conn.execute("PRAGMA journal_mode = WAL;")

    return conn


# other things to consider: future state
#PRAGMA synchronous = NORMAL;
#PRAGMA temp_store = MEMORY;
#PRAGMA cache_size = -10000;