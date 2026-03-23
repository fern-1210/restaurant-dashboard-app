"""
# What
Runnable script: generate Stage 0 outflow audit workbook (2024–2025).

# Why
Thin orchestration entrypoint so the layer story stays obvious.

# How
Run from repo root:
  python3 scripts_orchestrator/L0_generate_outflow_audit_workbook.py
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts_pipeline.layers.l0_outflow_audit import run_l0_outflow_audit

if __name__ == "__main__":
    raise SystemExit(run_l0_outflow_audit())
