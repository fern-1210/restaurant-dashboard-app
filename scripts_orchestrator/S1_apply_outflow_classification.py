"""
# What
Runnable script: Stage 1 outflow — load partner decisions into transaction_category_map.

# Why
Thin orchestration entrypoint mirroring inflow Stage 1.

# How
Run from repo root:
  python3 scripts_orchestrator/S1_apply_outflow_classification.py

Optional extra reviewed file (e.g. after editing stage1 workbook):
  python3 scripts_orchestrator/S1_apply_outflow_classification.py \\
    data/audit/stage0_outflow_audit_reviewed.xlsx \\
    data/audit/stage1_outflow_classification.xlsx
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts_pipeline.layers.stage1_outflow_classification import run_stage1_outflow_classification


def main(argv: list[str]) -> int:
    reviewed = Path(argv[1]) if len(argv) > 1 else None
    extra = Path(argv[2]) if len(argv) > 2 else None
    return run_stage1_outflow_classification(reviewed, extra)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
