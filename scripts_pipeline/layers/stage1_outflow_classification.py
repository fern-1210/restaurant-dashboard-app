"""
# What
Stage 1 outflow: ingest partner-reviewed category decisions into transaction_category_map
and export a classified workbook.

# Why
Stage 0 produces the review queue. Stage 1 persists approved category/subcategory pairs
per description_norm so the dashboard and future loads stay consistent.

# How
Read reviewed Excel sheets, validate pairs against taxonomy (or map-derived pairs),
UPSERT into transaction_category_map, then export summary + full classified outflows.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl.worksheet.datavalidation import DataValidation

from ingest.common import now_utc_iso
from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import (
    AUDIT_DIR,
    DB_PATH,
    STAGE0_OUTFLOW_REVIEWED_PATH,
    STAGE1_OUTFLOW_CLASSIFIED_PATH,
)
from scripts_pipeline.layers.l0_outflow_audit import (
    OUTFLOW_REVIEW_DATE_END,
    OUTFLOW_REVIEW_DATE_START,
    _autofit_columns,
    _read_taxonomy_reference,
)
from scripts_pipeline.schema import create_all_tables

REVIEW_COLUMNS = [
    "final_category",
    "final_subcategory",
    "confidence",
    "decision_notes",
    "reviewed_by",
    "reviewed_at",
]

CONFIDENCE_OPTIONS = ["high", "medium", "low"]


def _norm_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _norm_optional(value) -> str | None:
    out = _norm_text(value)
    return out if out else None


def _pairs_from_taxonomy_df(tax_df: pd.DataFrame) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for _, row in tax_df.iterrows():
        c, s = _norm_text(row.get("category")), _norm_text(row.get("subcategory"))
        if c and s:
            pairs.add((c, s))
    return pairs


def _load_outflow_decisions(workbook_path: Path) -> pd.DataFrame:
    if not workbook_path.exists():
        return pd.DataFrame(
            columns=[
                "description_norm",
                "final_category",
                "final_subcategory",
                "confidence",
                "decision_notes",
                "reviewed_by",
                "reviewed_at",
                "source_workbook",
            ]
        )

    sheets = []
    for sheet in ["needs_review", "flagged_outflows", "stage1_needs_review"]:
        try:
            df = pd.read_excel(workbook_path, sheet_name=sheet, engine="openpyxl")
        except Exception:
            continue
        if df.empty:
            continue
        lower_map = {str(c).strip().lower(): c for c in df.columns}
        required = {"description_norm", "final_category", "final_subcategory"}
        if not required.issubset(set(lower_map.keys())):
            continue
        prepared = pd.DataFrame(
            {
                "description_norm": df[lower_map["description_norm"]].map(_norm_text),
                "final_category": df[lower_map["final_category"]].map(_norm_text),
                "final_subcategory": df[lower_map["final_subcategory"]].map(_norm_text),
                "confidence": df[lower_map["confidence"]].map(_norm_optional)
                if "confidence" in lower_map
                else None,
                "decision_notes": df[lower_map["decision_notes"]].map(_norm_optional)
                if "decision_notes" in lower_map
                else None,
                "reviewed_by": df[lower_map["reviewed_by"]].map(_norm_optional)
                if "reviewed_by" in lower_map
                else None,
                "reviewed_at": df[lower_map["reviewed_at"]].map(_norm_optional)
                if "reviewed_at" in lower_map
                else None,
            }
        )
        prepared["source_workbook"] = str(workbook_path)
        sheets.append(prepared)

    if not sheets:
        return pd.DataFrame(
            columns=[
                "description_norm",
                "final_category",
                "final_subcategory",
                "confidence",
                "decision_notes",
                "reviewed_by",
                "reviewed_at",
                "source_workbook",
            ]
        )

    out = pd.concat(sheets, ignore_index=True)
    out = out[
        out["description_norm"].str.len().gt(0)
        & out["final_category"].str.len().gt(0)
        & out["final_subcategory"].str.len().gt(0)
    ]
    return out.drop_duplicates(subset=["description_norm"], keep="last").reset_index(drop=True)


def _upsert_outflow_mappings(conn, decisions_df: pd.DataFrame, valid_pairs: set[tuple[str, str]]) -> tuple[int, list[str]]:
    if decisions_df.empty:
        return 0, []
    now = now_utc_iso()
    errors: list[str] = []
    rows_ok: list[tuple] = []
    for _, row in decisions_df.iterrows():
        key = (row["final_category"], row["final_subcategory"])
        if valid_pairs and key not in valid_pairs:
            errors.append(
                f"Invalid pair for '{row['description_norm']}': {key[0]} / {key[1]}"
            )
            continue
        rows_ok.append(
            (
                row["description_norm"],
                row["final_category"],
                row["final_subcategory"],
                "stage1_outflow_review",
                row.get("decision_notes") or "",
                now,
            )
        )
    if rows_ok:
        conn.executemany(
            """
            INSERT INTO transaction_category_map (
                description_norm, category, subcategory, source_label, notes, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(description_norm) DO UPDATE SET
                category=excluded.category,
                subcategory=excluded.subcategory,
                source_label=excluded.source_label,
                notes=excluded.notes,
                updated_at=excluded.updated_at
            """,
            rows_ok,
        )
        conn.commit()
    return len(rows_ok), errors


def _read_outflow_classified_window(conn) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            b.id,
            b.bank,
            b.account_id,
            b.posting_date,
            b.value_date,
            b.description_raw,
            b.description_norm,
            b.amount,
            b.source_file,
            b.source_row,
            COALESCE(m.category, 'UNMAPPED') AS mapped_category,
            COALESCE(m.subcategory, 'UNMAPPED') AS mapped_subcategory,
            m.source_label AS map_source_label
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE b.amount < 0
          AND b.value_date BETWEEN ? AND ?
        ORDER BY b.value_date, b.id
        """,
        conn,
        params=[OUTFLOW_REVIEW_DATE_START, OUTFLOW_REVIEW_DATE_END],
    )


def _append_review_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in REVIEW_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def _build_review_instructions_sheet() -> pd.DataFrame:
    rows = [
        ("Purpose", "Resolve remaining UNMAPPED outflows after Stage 1 map refresh."),
        ("Fill", "final_category + final_subcategory for each open row."),
        ("Save", "Save as data/audit/stage1_outflow_classification.xlsx and re-run this script."),
    ]
    return pd.DataFrame(rows, columns=["field", "guidance"])


def _apply_review_dropdowns(
    worksheet, headers: list[str], max_row: int, categories: list[str], subcategories: list[str]
) -> None:
    if max_row < 2:
        return
    hmap = {str(h): i + 1 for i, h in enumerate(headers) if h is not None}

    def _apply(col: str, allowed: list[str]) -> None:
        idx = hmap.get(col)
        if not idx or not allowed:
            return
        letter = worksheet.cell(row=1, column=idx).column_letter
        csv_list = ",".join(allowed)
        dv = DataValidation(
            type="list",
            formula1=f'"{csv_list}"',
            allow_blank=True,
            showErrorMessage=True,
            errorTitle="Invalid value",
            error="Use taxonomy lists.",
        )
        worksheet.add_data_validation(dv)
        dv.add(f"{letter}2:{letter}{max_row}")

    _apply("final_category", categories)
    _apply("final_subcategory", subcategories)
    _apply("confidence", CONFIDENCE_OPTIONS)


def run_stage1_outflow_classification(
    reviewed_workbook_path: Path | None = None,
    extra_workbook_path: Path | None = None,
) -> int:
    """
    # What
    Apply outflow review decisions and write Stage 1 workbook.

    Parameters:
        reviewed_workbook_path: Partner-reviewed Stage 0 file (default stage0_outflow_audit_reviewed.xlsx).
        extra_workbook_path: Optional second file (e.g. edited stage1_outflow_classification.xlsx).

    Returns:
        0 on success (prints warnings for invalid pairs).
    """

    primary = reviewed_workbook_path or STAGE0_OUTFLOW_REVIEWED_PATH
    conn = connect_sqlite(DB_PATH)
    create_all_tables(conn)
    tax_ref = _read_taxonomy_reference(conn)
    valid_pairs = _pairs_from_taxonomy_df(tax_ref)
    cats = sorted({str(x).strip() for x in tax_ref["category"].dropna() if str(x).strip()})
    subs = sorted({str(x).strip() for x in tax_ref["subcategory"].dropna() if str(x).strip()})

    decisions_a = _load_outflow_decisions(primary)
    decisions_b = _load_outflow_decisions(extra_workbook_path) if extra_workbook_path else pd.DataFrame()
    if not decisions_b.empty:
        decisions = pd.concat([decisions_a, decisions_b], ignore_index=True)
        decisions = decisions.drop_duplicates(subset=["description_norm"], keep="last")
    else:
        decisions = decisions_a

    inserted, errors = _upsert_outflow_mappings(conn, decisions, valid_pairs)
    classified_df = _read_outflow_classified_window(conn)
    conn.close()

    for err in errors:
        print(f"WARNING: {err}")

    summary_df = (
        classified_df.groupby("mapped_category", dropna=False)
        .agg(row_count=("id", "count"), total_value=("amount", lambda s: s.abs().sum()))
        .reset_index()
        .sort_values("total_value", ascending=False)
    )
    summary_df["total_value"] = summary_df["total_value"].round(2)

    needs = classified_df[classified_df["mapped_category"].eq("UNMAPPED")].copy()
    needs = _append_review_columns(needs)
    instructions = _build_review_instructions_sheet()

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(STAGE1_OUTFLOW_CLASSIFIED_PATH, engine="openpyxl") as writer:
        instructions.to_excel(writer, sheet_name="review_instructions", index=False)
        summary_df.to_excel(writer, sheet_name="stage1_bucket_summary", index=False)
        needs.to_excel(writer, sheet_name="stage1_needs_review", index=False)
        classified_df.to_excel(writer, sheet_name="stage1_full_classified", index=False)
        decisions.to_excel(writer, sheet_name="loaded_manual_decisions", index=False)

        for name, sdf in [
            ("review_instructions", instructions),
            ("stage1_bucket_summary", summary_df),
            ("stage1_needs_review", needs),
            ("stage1_full_classified", classified_df),
            ("loaded_manual_decisions", decisions),
        ]:
            ws = writer.sheets[name]
            _autofit_columns(sdf, ws)
            if name == "stage1_needs_review":
                _apply_review_dropdowns(ws, list(sdf.columns), len(sdf) + 1, cats, subs)

    print("OK: Stage 1 outflow classification completed")
    print(f"- Reviewed workbook: {primary}")
    print(f"- Map rows upserted: {inserted}")
    print(f"- Classified outflow rows: {len(classified_df)}")
    print(f"- Remaining UNMAPPED: {len(needs)}")
    print(f"- Output: {STAGE1_OUTFLOW_CLASSIFIED_PATH}")
    return 0
