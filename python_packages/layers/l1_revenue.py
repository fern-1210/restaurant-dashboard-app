"""
# What
Layer 1 entrypoint: Vendus revenue CSVs → clean daily dataset + audits.

# Why
This wrapper makes the L1 data flow visible in one place. Orchestration scripts
import from here instead of multiple modules, so the "layer story" is obvious.

# How
Delegates to python_packages.revenue_ingest for load/transform/audit logic.
Writes outputs to warehouse and reports dirs, prints summary, returns exit code.
"""

from __future__ import annotations

from python_packages.paths import RAW_VENN_REVENUE_DIR, REPORTS_DIR, WAREHOUSE_DIR
from python_packages.revenue_ingest import (
    build_revenue_audits,
    build_revenue_daily,
    load_all_vendus_sources,
)


def run_l1_revenue() -> int:
    """
    # What
    Run Layer 1: load raw Vendus CSVs, transform, audit, write outputs.

    # Why
    Single entrypoint so L1 script stays thin and layer flow is obvious.

    # How
    - Load, transform, audit using revenue_ingest
    - Write revenue_daily.csv and audit CSVs
    - Print summary, return 0 on success

    Returns:
        0 on success.
    """
    # Ensure output directories exist.
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load, transform, and audit using reusable package logic.
    raw = load_all_vendus_sources(RAW_VENN_REVENUE_DIR)
    daily = build_revenue_daily(raw)
    audits = build_revenue_audits(daily)

    # Write outputs.
    out_daily = WAREHOUSE_DIR / "revenue_daily.csv"
    daily.to_csv(out_daily, index=False)

    audits.audit.to_csv(REPORTS_DIR / "revenue_daily_audit.csv", index=False)
    audits.duplicates.to_csv(REPORTS_DIR / "revenue_daily_duplicates.csv", index=False)
    audits.missing_days.to_csv(REPORTS_DIR / "revenue_daily_missing_days.csv", index=False)

    # Print run summary.
    print("OK: Vendus revenue CSV ingestion complete")
    print(f"- Wrote {out_daily} (rows={len(daily)})")
    print(f"- Wrote audits to {REPORTS_DIR}")
    if not audits.duplicates.empty:
        print(f"Warning: found duplicate dates (count={audits.duplicates['date'].nunique()})")
    if not audits.missing_days.empty:
        print(f"Note: missing days between min/max (count={len(audits.missing_days)})")
    return 0
