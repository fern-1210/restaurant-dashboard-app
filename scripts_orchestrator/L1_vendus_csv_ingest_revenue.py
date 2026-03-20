# What this is:
# - Layer 1 orchestration: reads Vendus revenue CSVs, produces clean daily dataset and audits.
#
# Why we are doing it:
# - Vendus exports match how the business operates ("source of truth").
# - This approach is easier to audit and perfect for a learning MVP.
#
# What it creates (local-only, gitignored):
# - data/warehouse/revenue_daily.csv
# - data/reports/revenue_daily_audit.csv
# - data/reports/revenue_daily_duplicates.csv
# - data/reports/revenue_daily_missing_days.csv
#
# How to run:
#   .venv/bin/python scripts_orchestrator/L1_vendus_csv_ingest_revenue.py
#
# Notes:
# - Vendus CSV uses ';' as separator and ',' as decimal separator.
# - We intentionally do NOT fill missing days with 0 (we output them for review).

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on path so python_packages can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from python_packages.layers.l1_revenue import run_l1_revenue

if __name__ == "__main__":
    raise SystemExit(run_l1_revenue())
