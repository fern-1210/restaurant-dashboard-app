"""
# What
Runnable script for Stage 0: generate the inflow audit workbook.

# Why
Provides a simple command that produces the partner review handoff file before
Stage 1 classification changes.

# How
Run from repo root:
  .venv/bin/python scripts_orchestrator/L0_generate_inflow_audit_workbook.py

Output:
  data/audit/stage0_inflow_audit.xlsx
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on path so scripts_pipeline can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts_pipeline.layers.l0_inflow_audit import run_l0_inflow_audit

if __name__ == "__main__":
    raise SystemExit(run_l0_inflow_audit())
