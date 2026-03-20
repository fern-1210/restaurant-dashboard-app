"""
# What
Runnable script for Chunk 3: ingest bank statements into SQLite.

# Why
To build a full financial picture (expenses + cash flow), we need bank transactions
in a queryable format. This script ingests:
- Caixa CSV statements
- Millennium XLS statements

and loads them into the warehouse table `bank_transactions`.

# How
Run from repo root:
  .venv/bin/python scripts_orchestrator/L3_load_bank_to_sqlite.py

Outputs (audits):
- data/reports/bank_transactions_audit.csv
- data/reports/bank_transactions_duplicates.csv   (within-batch duplicates)
- data/reports/bank_transactions_unparsed_rows.csv (rows we couldn't parse)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on path so python_packages can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from python_packages.layers.l3_bank_sqlite import run_l3_bank_sqlite

if __name__ == "__main__":
    raise SystemExit(run_l3_bank_sqlite())
