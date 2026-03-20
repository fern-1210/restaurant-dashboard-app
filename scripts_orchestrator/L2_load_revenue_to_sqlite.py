"""
# What
Runnable script for Chunk 2: load trusted revenue into SQLite.

# Why
You already have a CSV pipeline you trust (`data/warehouse/revenue_daily.csv`).
This script is the "bridge" into Layer 2 (Storage) by:
- creating/opening `data/warehouse/venn.db`
- ensuring the schema exists
- upserting daily revenue rows
- producing an audit CSV you can inspect

# How
Run from repo root:
  .venv/bin/python scripts_orchestrator/L2_load_revenue_to_sqlite.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on path so python_packages can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from python_packages.layers.l2_revenue_sqlite import run_l2_revenue_sqlite

if __name__ == "__main__":
    raise SystemExit(run_l2_revenue_sqlite())
