"""
# What
Stage 0 entrypoint: create a read-only inflow audit workbook from SQLite data.

# Why
Before introducing new inflow taxonomy/rules, we need a trustworthy baseline that:
- keeps full transaction traceability
- surfaces unmapped and suspicious inflows
- gives business owners a clean review handoff

# How
Reads inflows from bank_transactions (amount > 0), left-joins current mapping,
adds pandas-only diagnostic flags, builds summary/review tables, and writes:
data/audit/stage0_inflow_audit.xlsx.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl.worksheet.datavalidation import DataValidation

from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import AUDIT_DIR, DB_PATH


# ----------------------------------
# Known expense labels on inflows
# Keep this explicit for first review cycle
# ----------------------------------
EXPENSE_LABELS_ON_INFLOW = [
    "SALARIES AND FREELANCE",
    "FOOD AND DRINKS",
    "Fresh Produce",
    "OTHER EXPENSES",
    "OVERHEADS",
    "Insurance (Employees)",
]

REVIEW_COLUMNS = [
    "final_bucket",
    "final_subbucket",
    "owner_tag",
    "confidence",
    "decision_notes",
    "reviewed_by",
    "reviewed_at",
]

CANONICAL_BUCKETS = [
    "PartnerInvestment",
    "InterbankTransfer",
    "OperatingRevenue",
    "GrantIncome",
    "NeedsReview",
]

OWNER_TAG_OPTIONS = ["SNIDER", "JULIAN", "BOTH", "N-A"]
CONFIDENCE_OPTIONS = ["high", "medium", "low"]


def _read_inflow_base(conn) -> pd.DataFrame:
    """
    # What
    Pull all positive-amount bank transactions with mapping fields.

    # Why
    This is the immutable Stage 0 base table for diagnosis and partner review.

    # How
    SQL LEFT JOIN keeps unmapped rows visible; includes traceability fields.
    """

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
        WHERE b.amount > 0
        ORDER BY b.posting_date, b.id
        """,
        conn,
    )


def _add_stage0_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    # What
    Add pandas-only Stage 0 flags for inflow diagnosis.

    # Why
    We need review signals without touching production DB tables.

    # How
    Detect unmatched, expense-labeled, and interbank rows; add display category.
    """

    out = df.copy()
    out["is_unmatched"] = out["category"].isna()
    out["is_expense_label"] = out["category"].isin(EXPENSE_LABELS_ON_INFLOW)
    out["is_interbank"] = out["category"].eq("INTER COMPANY TRANSFERS")
    out["category_display"] = out["category"].fillna("NeedsReview")
    return out


def _build_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    # What
    Build high-level inflow diagnostics by category_display.

    # Why
    Gives a fast "where money comes from" picture and risk markers by bucket.

    # How
    Group by category_display and aggregate count, sum, and flagged-row counts.
    """

    summary = (
        df.groupby("category_display", dropna=False)
        .agg(
            row_count=("id", "count"),
            total_value=("amount", "sum"),
            expense_flagged_count=("is_expense_label", "sum"),
            interbank_flagged_count=("is_interbank", "sum"),
        )
        .reset_index()
        .sort_values("total_value", ascending=False)
    )
    summary["total_value"] = summary["total_value"].round(2)
    summary["expense_flagged_count"] = summary["expense_flagged_count"].astype(int)
    summary["interbank_flagged_count"] = summary["interbank_flagged_count"].astype(int)
    return summary


def _autofit_columns(df: pd.DataFrame, worksheet) -> None:
    """
    # What
    Auto-size Excel columns based on content length.

    # Why
    Improves readability for business review without manual resizing.

    # How
    Compute max len(header, cell values) and apply a capped width.
    """

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
        worksheet.column_dimensions[worksheet.cell(row=1, column=idx).column_letter].width = min(max_len + 2, 80)


def _append_review_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    # What
    Add empty business-review columns to an output DataFrame.

    # Why
    Ensures the workbook is directly editable as a review template without
    manual setup before partner review.

    # How
    Appends standardized columns used by Stage 1 decision loader.
    """

    out = df.copy()
    for col in REVIEW_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def _build_review_instructions_sheet() -> pd.DataFrame:
    """
    # What
    Build an instructions sheet for consistent manual review.

    # Why
    Makes the handoff self-explanatory for SNIDER/JULIAN and reduces formatting
    mistakes in decision columns.

    # How
    Writes key rules and accepted values into a simple two-column table.
    """

    rows = [
        ("Purpose", "Fill decisions on `needs_review` and `expense_flagged` sheets."),
        ("Do not edit", "id/source_file/source_row/description_norm/amount columns."),
        ("Required field", "final_bucket (must be one of canonical buckets)."),
        ("Canonical buckets", ", ".join(CANONICAL_BUCKETS)),
        ("owner_tag options", "SNIDER, JULIAN, BOTH, N-A"),
        ("confidence options", "high, medium, low"),
        ("reviewed_at format", "YYYY-MM-DD"),
        ("Notes", "Use decision_notes to explain rationale or evidence."),
    ]
    return pd.DataFrame(rows, columns=["field", "guidance"])


def _apply_review_dropdowns(worksheet, header_row_values: list[str], max_row: int) -> None:
    """
    # What
    Apply Excel dropdown validation to review columns.

    # Why
    Prevents invalid values that would later fail Stage 1 ingestion logic.

    # How
    Add data validation lists for final_bucket, owner_tag, and confidence.
    """

    if max_row < 2:
        return

    header_to_col = {
        str(value): idx + 1
        for idx, value in enumerate(header_row_values)
        if value is not None
    }

    def _apply_list_validation(column_name: str, allowed_values: list[str]) -> None:
        col_idx = header_to_col.get(column_name)
        if col_idx is None:
            return
        col_letter = worksheet.cell(row=1, column=col_idx).column_letter
        csv_list = ",".join(allowed_values)
        validation = DataValidation(
            type="list",
            formula1=f'"{csv_list}"',
            allow_blank=True,
            showErrorMessage=True,
            errorTitle="Invalid value",
            error=f"Use one of: {csv_list}",
        )
        worksheet.add_data_validation(validation)
        validation.add(f"{col_letter}2:{col_letter}{max_row}")

    _apply_list_validation("final_bucket", CANONICAL_BUCKETS)
    _apply_list_validation("owner_tag", OWNER_TAG_OPTIONS)
    _apply_list_validation("confidence", CONFIDENCE_OPTIONS)


def run_l0_inflow_audit() -> int:
    """
    # What
    Run Stage 0 inflow audit and export Excel handoff workbook.

    # Why
    Produces a concrete review artifact for SNIDER/JULIAN before Stage 1 rules.

    # How
    - Read base inflow rows from SQLite (read-only query)
    - Add Stage 0 flags in pandas
    - Build category summary + review queues
    - Export workbook with 4 sheets to data/audit/stage0_inflow_audit.xlsx

    Returns:
        0 on success.
    """

    conn = connect_sqlite(DB_PATH)
    base_df = _read_inflow_base(conn)
    conn.close()

    inflow_df = _add_stage0_flags(base_df)
    category_summary_df = _build_category_summary(inflow_df)

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
    expense_flagged_df = (
        inflow_df.loc[inflow_df["is_expense_label"], detail_columns]
        .sort_values(["posting_date", "amount"], ascending=[False, False])
        .reset_index(drop=True)
    )
    needs_review_df = (
        inflow_df.loc[inflow_df["is_unmatched"], detail_columns]
        .sort_values(["amount", "posting_date"], ascending=[False, False])
        .reset_index(drop=True)
    )
    expense_flagged_df = _append_review_columns(expense_flagged_df)
    needs_review_df = _append_review_columns(needs_review_df)
    instructions_df = _build_review_instructions_sheet()

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = AUDIT_DIR / "stage0_inflow_audit.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        instructions_df.to_excel(writer, sheet_name="review_instructions", index=False)
        category_summary_df.to_excel(writer, sheet_name="category_summary", index=False)
        expense_flagged_df.to_excel(writer, sheet_name="expense_flagged", index=False)
        needs_review_df.to_excel(writer, sheet_name="needs_review", index=False)
        inflow_df.to_excel(writer, sheet_name="full_inflow_base", index=False)

        for sheet_name, sheet_df in [
            ("review_instructions", instructions_df),
            ("category_summary", category_summary_df),
            ("expense_flagged", expense_flagged_df),
            ("needs_review", needs_review_df),
            ("full_inflow_base", inflow_df),
        ]:
            worksheet = writer.sheets[sheet_name]
            _autofit_columns(sheet_df, worksheet)

            if sheet_name in {"expense_flagged", "needs_review"}:
                headers = list(sheet_df.columns)
                max_row = len(sheet_df) + 1
                _apply_review_dropdowns(worksheet, headers, max_row)

    print("OK: Stage 0 inflow audit workbook generated")
    print(f"- Workbook: {out_path}")
    print(f"- Full inflow rows: {len(inflow_df)}")
    print(f"- Expense-flagged rows: {len(expense_flagged_df)}")
    print(f"- NeedsReview rows: {len(needs_review_df)}")
    print(f"- Category buckets: {len(category_summary_df)}")

    return 0
