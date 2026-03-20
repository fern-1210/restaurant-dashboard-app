"""
# What
Reusable Layer 1 logic: read Vendus revenue CSVs, transform to daily dataset, build audits.

# Why
Keeping transformation logic in scripts_pipeline makes it:
- reusable by other scripts or notebooks
- easier to test in isolation
- cleaner for the L1 orchestration script (which only calls these and writes outputs)

# How
- load_all_vendus_sources: read all CSVs from a directory into one DataFrame
- build_revenue_daily: transform raw columns into a clean daily schema
- build_revenue_audits: produce duplicates, missing days, and summary metrics
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

# Required Vendus column names (as exported) for the minimal daily schema.
REQUIRED_VENN_COLUMNS = {
    "Day": "date",
    "Sales with VAT": "sales_gross",
    "Sales": "sales_net",
}


def normalize_header(col: str) -> str:
    """
    Normalize Vendus CSV headers (quotes, HTML, BOM) into a stable form.

    Parameters:
        col: Raw column name string.

    Returns:
        Cleaned column name.
    """
    s = str(col).strip()
    s = re.sub(r"<[^>]+>", "", s)  # remove HTML tags
    s = s.replace("\ufeff", "")  # BOM
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip('"').strip("'")
    return s


def read_vendus_csv(path: Path) -> pd.DataFrame:
    """
    Read a single Vendus daily revenue CSV (semicolon-separated, comma decimal).

    Parameters:
        path: Path to the CSV file.

    Returns:
        DataFrame with normalized column names.
    """
    df = pd.read_csv(
        path,
        sep=";",
        decimal=",",
        encoding="utf-8",
    )
    df.columns = [normalize_header(c) for c in df.columns]
    return df


def load_all_vendus_sources(raw_dir: Path) -> pd.DataFrame:
    """
    Load all Vendus revenue CSVs from a directory into one DataFrame.

    Parameters:
        raw_dir: Directory containing *.csv files.

    Returns:
        Combined DataFrame with source_file column for lineage.
    """
    csvs = sorted(raw_dir.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files found in: {raw_dir}")

    parts: list[pd.DataFrame] = []
    for p in csvs:
        df = read_vendus_csv(p)
        df["source_file"] = p.name
        parts.append(df)
    return pd.concat(parts, ignore_index=True)


def build_revenue_daily(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw Vendus DataFrame into a clean daily revenue schema.

    Parameters:
        raw: Combined raw DataFrame from load_all_vendus_sources.

    Returns:
        DataFrame with date, sales_gross, sales_net, optional extras, source_file.
    """
    missing = [c for c in REQUIRED_VENN_COLUMNS.keys() if c not in raw.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(raw.columns)}")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(raw["Day"], errors="coerce").dt.date
    out["sales_gross"] = pd.to_numeric(raw["Sales with VAT"], errors="coerce")
    out["sales_net"] = pd.to_numeric(raw["Sales"], errors="coerce")

    # Optional extras (keep if present, do not require).
    if "Costs" in raw.columns:
        out["costs"] = pd.to_numeric(raw["Costs"], errors="coerce")
    if "Profit" in raw.columns:
        out["profit"] = pd.to_numeric(raw["Profit"], errors="coerce")
    if "Number of Sales (1)" in raw.columns:
        out["num_sales"] = pd.to_numeric(raw["Number of Sales (1)"], errors="coerce")
    elif "Number of Sales" in raw.columns:
        out["num_sales"] = pd.to_numeric(raw["Number of Sales"], errors="coerce")
    if "Quantity" in raw.columns:
        out["quantity"] = pd.to_numeric(raw["Quantity"], errors="coerce")

    out["source_file"] = raw.get("source_file")

    out = out.dropna(subset=["date"]).copy()
    out = out.sort_values("date").reset_index(drop=True)
    return out


@dataclass(frozen=True)
class RevenueAuditOutputs:
    """
    Structured audit outputs for Layer 1 revenue ingestion.

    Attributes:
        audit: Summary metrics (rows_total, date_min, date_max, etc.).
        duplicates: Rows with duplicate dates.
        missing_days: Dates missing between min and max.
    """

    audit: pd.DataFrame
    duplicates: pd.DataFrame
    missing_days: pd.DataFrame


def build_revenue_audits(daily: pd.DataFrame) -> RevenueAuditOutputs:
    """
    Build audit outputs: duplicates, missing days, summary metrics.

    Parameters:
        daily: Clean daily revenue DataFrame from build_revenue_daily.

    Returns:
        RevenueAuditOutputs with audit, duplicates, missing_days.
    """
    dup_mask = daily.duplicated(subset=["date"], keep=False)
    duplicates = daily.loc[dup_mask].sort_values(["date", "source_file"]).copy()

    if daily.empty:
        missing_days = pd.DataFrame(columns=["date"])
    else:
        min_d = pd.to_datetime(daily["date"].min())
        max_d = pd.to_datetime(daily["date"].max())
        all_days = pd.date_range(min_d, max_d, freq="D").date
        present = set(daily["date"].tolist())
        missing = [d for d in all_days if d not in present]
        missing_days = pd.DataFrame({"date": missing})

    audit_rows: list[dict[str, object]] = []
    audit_rows.append({"metric": "rows_total", "value": int(len(daily))})
    audit_rows.append({"metric": "date_min", "value": str(daily["date"].min()) if not daily.empty else ""})
    audit_rows.append({"metric": "date_max", "value": str(daily["date"].max()) if not daily.empty else ""})
    audit_rows.append(
        {"metric": "duplicates_dates_count", "value": int(duplicates["date"].nunique()) if not duplicates.empty else 0}
    )
    audit_rows.append({"metric": "missing_days_count", "value": int(len(missing_days))})

    if not daily.empty:
        audit_rows.append({"metric": "sales_gross_sum", "value": float(daily["sales_gross"].fillna(0).sum())})
        audit_rows.append({"metric": "sales_net_sum", "value": float(daily["sales_net"].fillna(0).sum())})

    audit = pd.DataFrame(audit_rows)
    return RevenueAuditOutputs(audit=audit, duplicates=duplicates, missing_days=missing_days)
