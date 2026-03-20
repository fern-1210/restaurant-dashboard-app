"""
# What
Layer 4 entrypoint: category Excel → SQLite transaction_category_map + category_taxonomy.

# Why
This wrapper makes the L4 data flow visible in one place. Orchestration scripts
import from here instead of multiple modules, so the "layer story" is obvious.

# How
Normalizes Excel columns into mapping or taxonomy mode, loads into SQLite,
writes coverage audits. Uses ingest.common, db, paths, schema.
"""

from __future__ import annotations

import re

import pandas as pd

from ingest.common import normalize_description, now_utc_iso
from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import (
    DB_PATH,
    DEFAULT_CATEGORY_XLSM_PATH,
    DEFAULT_CATEGORY_XLSX_PATH,
    PARTNER_MAPPING_XLSM_PATH,
    PARTNER_MAPPING_XLSX_PATH,
    REPORTS_DIR,
)
from scripts_pipeline.schema import create_all_tables


def _normalize_excel_columns(df: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    """
    Normalize input Excel column names to expected semantic names.

    Returns:
        Tuple of (mode, standardized_df). Mode is 'mapping' or 'taxonomy'.
    """
    out = df.copy()
    col_map = {
        re.sub(r"[^a-z0-9]+", "_", str(c).strip().lower()).strip("_"): c
        for c in out.columns
    }

    description_candidates = [
        "description", "description_raw", "description_norm",
        "merchant", "supplier", "transaction_description",
    ]
    category_candidates = [
        "category", "level_1", "lvl1", "macro_category", "group", "group_level_1",
    ]
    subcategory_candidates = [
        "subcategory", "sub_category", "level_2", "lvl2", "micro_category", "category_level_2",
    ]
    level3_candidates = ["level_3", "lvl3", "detail", "subsubcategory", "subcategory_level_3"]

    source_description = next((col_map[c] for c in description_candidates if c in col_map), None)
    source_category = next((col_map[c] for c in category_candidates if c in col_map), None)
    source_subcategory = next((col_map[c] for c in subcategory_candidates if c in col_map), None)
    source_level3 = next((col_map[c] for c in level3_candidates if c in col_map), None)

    if source_description is not None and source_category is not None and source_subcategory is not None:
        out = out.rename(columns={
            source_description: "description_raw",
            source_category: "category",
            source_subcategory: "subcategory",
        })
        return "mapping", out[["description_raw", "category", "subcategory"]]

    if source_description is None and source_category is not None and source_subcategory is not None:
        rename_map = {source_category: "level_1", source_subcategory: "level_2"}
        if source_level3 is not None:
            rename_map[source_level3] = "level_3"
        out = out.rename(columns=rename_map)
        if "level_3" not in out.columns:
            out["level_3"] = None
        return "taxonomy", out[["level_1", "level_2", "level_3"]]

    if source_category is None or source_subcategory is None:
        raise ValueError(
            "Excel must include category/subcategory columns (and optionally description). "
            f"Found columns: {list(out.columns)}"
        )
    raise ValueError(f"Could not interpret Excel columns: {list(out.columns)}")


def _prepare_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate mapping rows before loading into SQLite."""
    out = df.copy()
    out["description_norm"] = out["description_raw"].apply(lambda x: normalize_description(str(x)))
    out["category"] = out["category"].astype(str).str.strip()
    out["subcategory"] = out["subcategory"].astype(str).str.strip()
    out = out.drop(columns=["description_raw"])
    out = out.dropna(subset=["description_norm", "category", "subcategory"])
    out = out[
        (out["description_norm"].str.len() > 0)
        & (out["category"].str.len() > 0)
        & (out["subcategory"].str.len() > 0)
    ]
    out = out.drop_duplicates(subset=["description_norm"], keep="last").reset_index(drop=True)
    out["source_label"] = "manual_excel"
    out["notes"] = ""
    out["updated_at"] = now_utc_iso()
    return out[["description_norm", "category", "subcategory", "source_label", "notes", "updated_at"]]


def _replace_mapping_table(conn, mapping_df: pd.DataFrame) -> tuple[int, int]:
    """Replace mapping table contents atomically. Returns (rows_before, rows_after)."""
    rows_before = conn.execute("SELECT COUNT(*) FROM transaction_category_map;").fetchone()[0]
    conn.execute("BEGIN;")
    conn.execute("DELETE FROM transaction_category_map;")
    conn.executemany(
        """
        INSERT INTO transaction_category_map (
            description_norm, category, subcategory, source_label, notes, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (row["description_norm"], row["category"], row["subcategory"],
             row["source_label"], row["notes"], row["updated_at"])
            for _, row in mapping_df.iterrows()
        ],
    )
    conn.commit()
    rows_after = conn.execute("SELECT COUNT(*) FROM transaction_category_map;").fetchone()[0]
    return rows_before, rows_after


def _replace_taxonomy_table(conn, taxonomy_df: pd.DataFrame) -> tuple[int, int]:
    """Replace taxonomy table contents atomically. Returns (rows_before, rows_after)."""
    rows_before = conn.execute("SELECT COUNT(*) FROM category_taxonomy;").fetchone()[0]
    conn.execute("BEGIN;")
    conn.execute("DELETE FROM category_taxonomy;")
    conn.executemany(
        """
        INSERT INTO category_taxonomy (level_1, level_2, level_3, source_label, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (row["level_1"], row["level_2"], row["level_3"], "manual_excel", now_utc_iso())
            for _, row in taxonomy_df.iterrows()
        ],
    )
    conn.commit()
    rows_after = conn.execute("SELECT COUNT(*) FROM category_taxonomy;").fetchone()[0]
    return rows_before, rows_after


def _write_coverage_audit(conn) -> Path:
    """Compute and store mapping coverage audit report. Returns audit path."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = REPORTS_DIR / "transaction_category_coverage_audit.csv"

    total_tx = conn.execute("SELECT COUNT(*) FROM bank_transactions;").fetchone()[0]
    mapped_tx = conn.execute(
        """
        SELECT COUNT(*)
        FROM bank_transactions b
        INNER JOIN transaction_category_map m ON b.description_norm = m.description_norm
        """
    ).fetchone()[0]
    unmapped_tx = total_tx - mapped_tx
    mapped_pct = (mapped_tx / total_tx * 100.0) if total_tx else 0.0

    audit_df = pd.DataFrame([
        {"metric": "bank_transactions_total", "value": int(total_tx)},
        {"metric": "bank_transactions_mapped", "value": int(mapped_tx)},
        {"metric": "bank_transactions_unmapped", "value": int(unmapped_tx)},
        {"metric": "bank_transactions_mapped_pct", "value": float(round(mapped_pct, 2))},
    ])
    audit_df.to_csv(audit_path, index=False)

    unmapped_path = REPORTS_DIR / "transaction_category_unmapped.csv"
    pd.read_sql_query(
        """
        SELECT b.posted_date, b.description_raw, b.description_norm, b.amount,
               b.bank, b.account_id, b.source_file
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE m.description_norm IS NULL
        ORDER BY b.posted_date DESC
        """,
        conn,
    ).to_csv(unmapped_path, index=False)

    return audit_path


def run_l4_category_sqlite() -> int:
    """
    # What
    Run Layer 4: load category map from Excel into SQLite.

    # Why
    Single entrypoint so L4 script stays thin and layer flow is obvious.

    # How
    - Resolve source Excel (partner mapping or fallback)
    - Normalize columns, detect mapping vs taxonomy mode
    - Load into transaction_category_map or category_taxonomy
    - Write coverage audits, print summary, return 0 on success

    Returns:
        0 on success.
    """
    if PARTNER_MAPPING_XLSX_PATH.exists():
        source_path = PARTNER_MAPPING_XLSX_PATH
    elif PARTNER_MAPPING_XLSM_PATH.exists():
        source_path = PARTNER_MAPPING_XLSM_PATH
    elif DEFAULT_CATEGORY_XLSX_PATH.exists():
        source_path = DEFAULT_CATEGORY_XLSX_PATH
    elif DEFAULT_CATEGORY_XLSM_PATH.exists():
        source_path = DEFAULT_CATEGORY_XLSM_PATH
    else:
        raise FileNotFoundError(
            "No category Excel found. Either:\n"
            f"  1. Run scripts_orchestrator/L4_generate_partner_mapping_template.py, have partner fill it,\n"
            f"     and save as one of:\n"
            f"     - {PARTNER_MAPPING_XLSX_PATH}\n"
            f"     - {PARTNER_MAPPING_XLSM_PATH}\n"
            f"  2. Or place hierarchy Excel at one of:\n"
            f"     - {DEFAULT_CATEGORY_XLSX_PATH}\n"
            f"     - {DEFAULT_CATEGORY_XLSM_PATH}"
        )

    raw_df = pd.read_excel(source_path, engine="openpyxl")
    mode, normalized_df = _normalize_excel_columns(raw_df)

    conn = connect_sqlite(DB_PATH)
    create_all_tables(conn)

    if mode == "mapping":
        mapping_df = _prepare_mapping(normalized_df)
        rows_before, rows_after = _replace_mapping_table(conn, mapping_df)
        loaded_entity = "transaction_category_map"
    else:
        taxonomy_df = normalized_df.copy()
        taxonomy_df["level_1"] = taxonomy_df["level_1"].astype(str).str.strip()
        taxonomy_df["level_2"] = taxonomy_df["level_2"].astype(str).str.strip()
        taxonomy_df["level_3"] = taxonomy_df["level_3"].fillna("").astype(str).str.strip()
        taxonomy_df = taxonomy_df.dropna(subset=["level_1", "level_2"])
        taxonomy_df = taxonomy_df[
            (taxonomy_df["level_1"].str.len() > 0) & (taxonomy_df["level_2"].str.len() > 0)
        ]
        taxonomy_df = taxonomy_df.drop_duplicates(
            subset=["level_1", "level_2", "level_3"], keep="last"
        ).reset_index(drop=True)
        rows_before, rows_after = _replace_taxonomy_table(conn, taxonomy_df)
        loaded_entity = "category_taxonomy"

    audit_path = _write_coverage_audit(conn)
    conn.close()

    print("OK: category data loaded into SQLite")
    print(f"- Source Excel: {source_path}")
    print(f"- Mode detected: {mode}")
    print(f"- Loaded table: {loaded_entity}")
    print(f"- DB: {DB_PATH}")
    print(f"- Rows before refresh: {rows_before}")
    print(f"- Rows after refresh: {rows_after}")
    print(f"- Coverage audit: {audit_path}")
    print(f"- Unmapped list: {REPORTS_DIR / 'transaction_category_unmapped.csv'}")

    return 0
