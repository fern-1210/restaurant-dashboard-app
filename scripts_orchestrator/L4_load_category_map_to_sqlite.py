"""
# BETTER WAY (for when you're ready)
Use a curated dimension table with surrogate keys and SCD logic (slowly changing
dimensions) so category history is versioned across time.

# SIMPLE VERSION (what we're building now)
Load a 2-level category map from Excel into `transaction_category_map` using
`description_norm` as the deterministic key and write coverage audits.

# How
Run from repo root:
  .venv/bin/python scripts_orchestrator/L4_load_category_map_to_sqlite.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on path so python_packages can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from python_packages.layers.l4_category_sqlite import run_l4_category_sqlite

if __name__ == "__main__":
    raise SystemExit(run_l4_category_sqlite())
