"""
# What
Runnable script for Stage 1 inflow classification.

# Why
Provides a single command that:
- optionally ingests reviewed decisions from Stage 0 workbook
- applies canonical inflow classification
- exports Stage 1 workbook outputs

# How
Run from repo root:
  .venv/bin/python scripts_orchestrator/S1_apply_inflow_classification.py

Optional:
  .venv/bin/python scripts_orchestrator/S1_apply_inflow_classification.py /path/to/reviewed.xlsx
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on path so scripts_pipeline can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts_pipeline.layers.stage1_inflow_classification import run_stage1_inflow_classification


def main(argv: list[str]) -> int:
    """
    # What
    Parse optional reviewed workbook path and run Stage 1.

    # Why
    Lets the same script work with default path or ad-hoc review files.

    # How
    Uses first CLI arg as workbook path when provided.
    """

    workbook_path = Path(argv[1]) if len(argv) > 1 else None
    return run_stage1_inflow_classification(workbook_path)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
