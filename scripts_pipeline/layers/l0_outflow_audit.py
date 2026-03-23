"""
# What
Stage 0 entrypoint: read-only outflow audit workbook for partner review (2024–2025).

# Why
Outflows need the same traceable triage as inflows: unmatched rows, interbank flags,
and a handoff file with taxonomy-aligned dropdowns before updating the live map.

# How
Pull debits from bank_transactions in the review window, join transaction_category_map,
add pandas flags, attach taxonomy reference from category_taxonomy (or map fallback),
and write data/audit/stage0_outflow_audit.xlsx.
"""

from __future__ import annotations

import pandas as pd
from openpyxl.worksheet.datavalidation import DataValidation

from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import AUDIT_DIR, DB_PATH, STAGE0_OUTFLOW_AUDIT_PATH

# ----------------------------------
# Review window (full 2024–2025 pass)
# ----------------------------------
OUTFLOW_REVIEW_DATE_START = "2024-01-01"
OUTFLOW_REVIEW_DATE_END = "2025-12-31"

REVIEW_COLUMNS = [
    "final_category",
    "final_subcategory",
    "confidence",
    "decision_notes",
    "reviewed_by",
    "reviewed_at",
]

CONFIDENCE_OPTIONS = ["high", "medium", "low"]


def _read_taxonomy_reference(conn) -> pd.DataFrame:
    """
    # What
    Load approved category pairs for Excel reference and dropdown source lists.

    # Why
    Partner review should align with existing taxonomy when present.

    # How
    Prefer category_taxonomy; if empty, derive distinct pairs from transaction_category_map.
    """

    try:
        tax = pd.read_sql_query(
            """
            SELECT DISTINCT level_1 AS category, level_2 AS subcategory, level_3
            FROM category_taxonomy
            WHERE level_1 IS NOT NULL AND TRIM(level_1) != ''
              AND level_2 IS NOT NULL AND TRIM(level_2) != ''
            ORDER BY level_1, level_2, level_3
            """,
            conn,
        )
    except Exception:
        tax = pd.DataFrame(columns=["category", "subcategory", "level_3"])
    if tax.empty:
        tax = pd.read_sql_query(
            """
            SELECT DISTINCT category, subcategory, NULL AS level_3
            FROM transaction_category_map
            WHERE category IS NOT NULL AND TRIM(category) != ''
              AND subcategory IS NOT NULL AND TRIM(subcategory) != ''
            ORDER BY category, subcategory
            """,
            conn,
        )
    return tax


def _read_outflow_base(conn) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            b.id,
            b.bank,
            b.account_id,
            b.posting_date,
            b.value_date,
            strftime('%Y-%m', b.posting_date) AS month,
            b.description_raw,
            b.description_norm,
            b.amount,
            b.balance,
            b.currency,
            b.source_file,
            b.source_row,
            m.category,
            m.subcategory,
            m.source_label
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m
            ON b.description_norm = m.description_norm
        WHERE b.amount < 0
          AND b.value_date BETWEEN ? AND ?
        ORDER BY b.value_date, b.id
        """,
        conn,
        params=[OUTFLOW_REVIEW_DATE_START, OUTFLOW_REVIEW_DATE_END],
    )


def _add_stage0_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_unmatched"] = out["category"].isna()
    out["is_interbank_out"] = out["category"].eq("INTER COMPANY TRANSFERS")
    out["category_display"] = out["category"].fillna("NeedsReview")
    return out


def _build_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("category_display", dropna=False)
        .agg(
            row_count=("id", "count"),
            total_value=("amount", lambda s: s.abs().sum()),
            interbank_flagged_count=("is_interbank_out", "sum"),
        )
        .reset_index()
        .sort_values("total_value", ascending=False)
    )
    summary["total_value"] = summary["total_value"].round(2)
    summary["interbank_flagged_count"] = summary["interbank_flagged_count"].astype(int)
    return summary


def _autofit_columns(df: pd.DataFrame, worksheet) -> None:
    for idx, col in enumerate(df.columns, start=1):
        values = []
        if len(df):
            for cell_value in df[col].tolist():
                if isinstance(cell_value, bytes):
                    values.append(cell_value.decode("utf-8", errors="replace"))
                elif cell_value is None:
                    values.append("")
                else:
                    values.append(str(cell_value))
        max_len = max([len(str(col))] + [len(v) for v in values]) if values else len(str(col))
        worksheet.column_dimensions[worksheet.cell(row=1, column=idx).column_letter].width = min(
            max_len + 2, 80
        )


def _append_review_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in REVIEW_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def _category_lists(tax_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    cats = sorted({str(x).strip() for x in tax_df["category"].dropna() if str(x).strip()})
    subs = sorted({str(x).strip() for x in tax_df["subcategory"].dropna() if str(x).strip()})
    return cats, subs


def _build_review_instructions_sheet(cats: list[str], subs: list[str]) -> pd.DataFrame:
    rows = [
        ("Purpose", "Assign expense category/subcategory on `needs_review` and `flagged_outflows`."),
        ("Do not edit", "id, source_file, source_row, description_norm, amount (traceability)."),
        ("Required when filling", "final_category AND final_subcategory (must be a valid taxonomy pair)."),
        ("confidence", ", ".join(CONFIDENCE_OPTIONS)),
        ("reviewed_at", "YYYY-MM-DD"),
        ("After review", "Save as data/audit/stage0_outflow_audit_reviewed.xlsx then run outflow Stage 1 script."),
        ("Category count", str(len(cats))),
        ("Subcategory count", str(len(subs))),
    ]
    return pd.DataFrame(rows, columns=["field", "guidance"])


def _apply_outflow_review_dropdowns(
    worksheet,
    header_row_values: list[str],
    max_row: int,
    categories: list[str],
    subcategories: list[str],
) -> None:
    if max_row < 2:
        return
    header_to_col = {
        str(value): idx + 1 for idx, value in enumerate(header_row_values) if value is not None
    }

    def _apply_list_validation(column_name: str, allowed: list[str]) -> None:
        col_idx = header_to_col.get(column_name)
        if col_idx is None or not allowed:
            return
        col_letter = worksheet.cell(row=1, column=col_idx).column_letter
        csv_list = ",".join(allowed)
        validation = DataValidation(
            type="list",
            formula1=f'"{csv_list}"',
            allow_blank=True,
            showErrorMessage=True,
            errorTitle="Invalid value",
            error="Pick from the approved taxonomy lists.",
        )
        worksheet.add_data_validation(validation)
        validation.add(f"{col_letter}2:{col_letter}{max_row}")

    _apply_list_validation("final_category", categories)
    _apply_list_validation("final_subcategory", subcategories)
    _apply_list_validation("confidence", CONFIDENCE_OPTIONS)


def run_l0_outflow_audit() -> int:
    """
    # What
    Generate Stage 0 outflow audit workbook for 2024–2025.

    Returns:
        0 on success.
    """

    conn = connect_sqlite(DB_PATH)
    tax_ref = _read_taxonomy_reference(conn)
    cats, subs = _category_lists(tax_ref)
    base_df = _read_outflow_base(conn)
    conn.close()

    outflow_df = _add_stage0_flags(base_df)
    category_summary_df = _build_category_summary(outflow_df)

    detail_columns = [
        "posting_date",
        "value_date",
        "bank",
        "account_id",
        "description_raw",
        "description_norm",
        "amount",
        "category",
        "subcategory",
        "source_file",
        "source_row",
    ]
    flagged_df = (
        outflow_df.loc[outflow_df["is_interbank_out"], detail_columns]
        .sort_values(["value_date", "amount"], ascending=[False, True])
        .reset_index(drop=True)
    )
    needs_review_df = (
        outflow_df.loc[outflow_df["is_unmatched"], detail_columns]
        .assign(abs_amount=lambda d: d["amount"].abs())
        .sort_values(["abs_amount", "value_date"], ascending=[False, False])
        .drop(columns=["abs_amount"])
        .reset_index(drop=True)
    )
    flagged_df = _append_review_columns(flagged_df)
    needs_review_df = _append_review_columns(needs_review_df)
    instructions_df = _build_review_instructions_sheet(cats, subs)

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(STAGE0_OUTFLOW_AUDIT_PATH, engine="openpyxl") as writer:
        instructions_df.to_excel(writer, sheet_name="review_instructions", index=False)
        tax_ref.to_excel(writer, sheet_name="taxonomy_reference", index=False)
        category_summary_df.to_excel(writer, sheet_name="category_summary", index=False)
        flagged_df.to_excel(writer, sheet_name="flagged_outflows", index=False)
        needs_review_df.to_excel(writer, sheet_name="needs_review", index=False)
        outflow_df.to_excel(writer, sheet_name="full_outflow_base", index=False)

        for sheet_name, sheet_df in [
            ("review_instructions", instructions_df),
            ("taxonomy_reference", tax_ref),
            ("category_summary", category_summary_df),
            ("flagged_outflows", flagged_df),
            ("needs_review", needs_review_df),
            ("full_outflow_base", outflow_df),
        ]:
            ws = writer.sheets[sheet_name]
            _autofit_columns(sheet_df, ws)
            if sheet_name in {"flagged_outflows", "needs_review"}:
                _apply_outflow_review_dropdowns(
                    ws, list(sheet_df.columns), len(sheet_df) + 1, cats, subs
                )

    print("OK: Stage 0 outflow audit workbook generated")
    print(f"- Workbook: {STAGE0_OUTFLOW_AUDIT_PATH}")
    print(f"- Outflow rows (2024–2025): {len(outflow_df)}")
    print(f"- NeedsReview rows: {len(needs_review_df)}")
    print(f"- Flagged interbank rows: {len(flagged_df)}")
    return 0
