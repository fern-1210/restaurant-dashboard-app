"""
# What
Stage 1 inflow classification: apply canonical buckets and export business outputs.

# Why
Stage 0 gave visibility. Stage 1 converts that into a repeatable classification
system by combining reviewed decisions with deterministic fallback rules.

# How
- Optionally load reviewed workbook decisions into inflow_classification_map
- Build inflow dataset from SQLite + existing map + decision map
- Apply canonical bucket logic with strict fallback to NeedsReview
- Exclude unresolved risky rows from revenue metrics
- Export Stage 1 workbook for business and audit follow-up
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl.worksheet.datavalidation import DataValidation

from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import (
    AUDIT_DIR,
    DB_PATH,
    STAGE0_REVIEWED_WORKBOOK_PATH,
    STAGE1_CLASSIFIED_WORKBOOK_PATH,
)
from scripts_pipeline.schema import create_all_tables

CANONICAL_BUCKETS = {
    "PartnerInvestment",
    "InterbankTransfer",
    "OperatingRevenue",
    "GrantIncome",
    "NeedsReview",
}

CANONICAL_BUCKETS_LIST = [
    "PartnerInvestment",
    "InterbankTransfer",
    "OperatingRevenue",
    "GrantIncome",
    "NeedsReview",
]

OWNER_TAG_OPTIONS = ["SNIDER", "JULIAN", "BOTH", "N-A"]
CONFIDENCE_OPTIONS = ["high", "medium", "low"]

REVIEW_COLUMNS = [
    "final_bucket",
    "final_subbucket",
    "owner_tag",
    "confidence",
    "decision_notes",
    "reviewed_by",
    "reviewed_at",
]

EXPENSE_LABELS_ON_INFLOW = {
    "SALARIES AND FREELANCE",
    "FOOD AND DRINKS",
    "Fresh Produce",
    "OTHER EXPENSES",
    "OVERHEADS",
    "Insurance (Employees)",
}


# ----------------------------------
# Safe value normalization helpers
# Keep workbook ingestion robust and predictable
# ----------------------------------
def _norm_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _norm_optional(value) -> str | None:
    out = _norm_text(value)
    return out if out else None


def _load_review_decisions(reviewed_workbook_path: Path) -> pd.DataFrame:
    """
    # What
    Read reviewed decisions from Stage 0 workbook.

    # Why
    Business-reviewed decisions should be first-priority source for Stage 1.

    # How
    Parse `needs_review` and `expense_flagged`, keep rows with valid final_bucket.
    """

    if not reviewed_workbook_path.exists():
        return pd.DataFrame(
            columns=[
                "description_norm",
                "final_bucket",
                "final_subbucket",
                "owner_tag",
                "confidence",
                "decision_notes",
                "reviewed_by",
                "reviewed_at",
                "source_workbook",
            ]
        )

    sheets = []
    # Accept both Stage 0 and Stage 1 review sheet names so the same loader
    # can ingest iterative review passes without manual file reshaping.
    for sheet in ["needs_review", "expense_flagged", "stage1_needs_review"]:
        try:
            df = pd.read_excel(reviewed_workbook_path, sheet_name=sheet, engine="openpyxl")
        except Exception:
            continue
        if df.empty:
            continue
        lower_map = {str(c).strip().lower(): c for c in df.columns}
        required = {"description_norm", "final_bucket"}
        if not required.issubset(set(lower_map.keys())):
            continue
        prepared = pd.DataFrame(
            {
                "description_norm": df[lower_map["description_norm"]].map(_norm_text),
                "final_bucket": df[lower_map["final_bucket"]].map(_norm_text),
                "final_subbucket": df[lower_map["final_subbucket"]].map(_norm_optional)
                if "final_subbucket" in lower_map
                else None,
                "owner_tag": df[lower_map["owner_tag"]].map(_norm_optional)
                if "owner_tag" in lower_map
                else None,
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
        prepared["source_workbook"] = str(reviewed_workbook_path)
        sheets.append(prepared)

    if not sheets:
        return pd.DataFrame(
            columns=[
                "description_norm",
                "final_bucket",
                "final_subbucket",
                "owner_tag",
                "confidence",
                "decision_notes",
                "reviewed_by",
                "reviewed_at",
                "source_workbook",
            ]
        )

    decisions = pd.concat(sheets, ignore_index=True)
    decisions = decisions[
        decisions["description_norm"].str.len().gt(0)
        & decisions["final_bucket"].isin(CANONICAL_BUCKETS)
    ]
    return decisions.drop_duplicates(subset=["description_norm"], keep="last").reset_index(drop=True)


def _upsert_decisions(conn, decisions_df: pd.DataFrame) -> int:
    """
    # What
    Upsert reviewed decisions into inflow_classification_map.

    # Why
    Makes review outcomes persistent and reusable.

    # How
    INSERT ON CONFLICT updates existing key by description_norm.
    """

    if decisions_df.empty:
        return 0
    now_iso = pd.Timestamp.utcnow().isoformat()
    rows = [
        (
            row["description_norm"],
            row["final_bucket"],
            row["final_subbucket"],
            row["owner_tag"],
            row["confidence"],
            row["decision_notes"],
            row["reviewed_by"],
            row["reviewed_at"],
            row["source_workbook"],
            now_iso,
        )
        for _, row in decisions_df.iterrows()
    ]
    conn.executemany(
        """
        INSERT INTO inflow_classification_map (
            description_norm, final_bucket, final_subbucket, owner_tag, confidence,
            decision_notes, reviewed_by, reviewed_at, source_workbook, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(description_norm) DO UPDATE SET
            final_bucket=excluded.final_bucket,
            final_subbucket=excluded.final_subbucket,
            owner_tag=excluded.owner_tag,
            confidence=excluded.confidence,
            decision_notes=excluded.decision_notes,
            reviewed_by=excluded.reviewed_by,
            reviewed_at=excluded.reviewed_at,
            source_workbook=excluded.source_workbook,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def _read_stage1_base(conn) -> pd.DataFrame:
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
            m.source_label,
            icm.final_bucket AS reviewed_bucket,
            icm.final_subbucket AS reviewed_subbucket,
            icm.owner_tag,
            icm.confidence,
            icm.decision_notes
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m
            ON b.description_norm = m.description_norm
        LEFT JOIN inflow_classification_map icm
            ON b.description_norm = icm.description_norm
        WHERE b.amount > 0
        ORDER BY b.posting_date, b.id
        """,
        conn,
    )


def _rule_based_bucket(row: pd.Series) -> str:
    desc = _norm_text(row.get("description_norm")).lower()
    cat = _norm_text(row.get("category"))

    if cat == "INTER COMPANY TRANSFERS":
        return "InterbankTransfer"
    if any(token in desc for token in ["transfer", "trf", "inter company"]):
        return "InterbankTransfer"
    if any(token in desc for token in ["snider", "julian", "owner loan", "owner deposit", "partner"]):
        return "PartnerInvestment"
    if any(token in desc for token in ["grant", "eu commission", "eu", "commission"]):
        return "GrantIncome"
    if cat in {"DEPOSIT FROM CARD MACHINE", "BANK DEPOSIT IN CASH"}:
        return "OperatingRevenue"
    if _norm_text(row.get("category")) == "":
        return "NeedsReview"
    return "NeedsReview"


def _apply_stage1_classification(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_unmatched"] = out["category"].isna()
    out["is_expense_label"] = out["category"].isin(EXPENSE_LABELS_ON_INFLOW)
    out["is_interbank"] = out["category"].eq("INTER COMPANY TRANSFERS")

    out["proposed_bucket"] = out.apply(_rule_based_bucket, axis=1)
    out["final_bucket"] = out["reviewed_bucket"].fillna(out["proposed_bucket"])
    out["final_subbucket"] = out["reviewed_subbucket"]
    out["decision_source"] = out["reviewed_bucket"].notna().map(
        {True: "manual_reviewed_workbook", False: "rule_based"}
    )

    # Stage 1 locked policy: unresolved expense-labeled inflows are excluded.
    out["exclude_from_revenue_metric"] = False
    out.loc[out["is_expense_label"] & out["reviewed_bucket"].isna(), "exclude_from_revenue_metric"] = True
    out.loc[out["final_bucket"].isin(["InterbankTransfer", "NeedsReview", "PartnerInvestment"]), "exclude_from_revenue_metric"] = True

    # Stage 1 business-revenue numerator.
    out["include_in_revenue_metric"] = ~out["exclude_from_revenue_metric"] & out["final_bucket"].eq("OperatingRevenue")
    return out


def persist_bank_inflow_stage1(conn, classified_df: pd.DataFrame) -> int:
    """
    # What
    Replace `bank_inflow_stage1` contents from a classified inflow DataFrame.

    # Why
    Streamlit reads Stage 1 buckets via SQL join; this table is the dashboard source of truth.

    # How
    DELETE all rows, then INSERT one row per classified inflow with flags as 0/1 integers.
    """

    conn.execute("DELETE FROM bank_inflow_stage1;")
    if classified_df.empty:
        conn.commit()
        return 0
    now_iso = pd.Timestamp.now("UTC").isoformat()
    rows = []
    for _, row in classified_df.iterrows():
        rows.append(
            (
                int(row["id"]),
                str(row["final_bucket"]),
                _norm_optional(row.get("final_subbucket")),
                _norm_optional(row.get("owner_tag")),
                str(row.get("decision_source") or ""),
                1 if bool(row.get("exclude_from_revenue_metric")) else 0,
                1 if bool(row.get("include_in_revenue_metric")) else 0,
                now_iso,
            )
        )
    conn.executemany(
        """
        INSERT INTO bank_inflow_stage1 (
            bank_transaction_id, final_bucket, final_subbucket, owner_tag,
            decision_source, exclude_from_revenue_metric, include_in_revenue_metric, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


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
        worksheet.column_dimensions[worksheet.cell(row=1, column=idx).column_letter].width = min(max_len + 2, 80)


def _append_review_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    # What
    Add editable review columns for Stage 1 follow-up.

    # Why
    Stage 1 workbook should remain an actionable review handoff, not just output.

    # How
    Ensure all standardized review columns exist (without overwriting existing).
    """

    out = df.copy()
    for col in REVIEW_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def _build_review_instructions_sheet() -> pd.DataFrame:
    """
    # What
    Build review guidance sheet for Stage 1 workbook.

    # Why
    Keeps partner workflow consistent with Stage 0 and avoids invalid values.

    # How
    Store rules and accepted values in a two-column table.
    """

    rows = [
        ("Purpose", "Use `stage1_needs_review` for unresolved inflows after Stage 1 rules."),
        ("Do not edit", "id/source_file/source_row/description_norm/amount columns."),
        ("Required field", "final_bucket (must be one of canonical buckets)."),
        ("Canonical buckets", ", ".join(CANONICAL_BUCKETS_LIST)),
        ("owner_tag options", "SNIDER, JULIAN, BOTH, N-A"),
        ("confidence options", "high, medium, low"),
        ("reviewed_at format", "YYYY-MM-DD"),
        ("Notes", "Use decision_notes to explain evidence for each decision."),
    ]
    return pd.DataFrame(rows, columns=["field", "guidance"])


def _apply_review_dropdowns(worksheet, header_row_values: list[str], max_row: int) -> None:
    """
    # What
    Apply Excel dropdown validation to Stage 1 review columns.

    # Why
    Prevents typos and ensures values are compatible with Stage 1 ingestion.

    # How
    Add list validation rules to selected columns.
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

    _apply_list_validation("final_bucket", CANONICAL_BUCKETS_LIST)
    _apply_list_validation("owner_tag", OWNER_TAG_OPTIONS)
    _apply_list_validation("confidence", CONFIDENCE_OPTIONS)


def run_stage1_inflow_classification(reviewed_workbook_path: Path | None = None) -> int:
    """
    # What
    Run Stage 1 inflow classification and export workbook.

    # Why
    Produces canonical bucket outputs and a focused queue for ongoing review.

    # How
    - Ingest reviewed decisions (if workbook exists)
    - Classify all inflows
    - Export summary + unresolved queues + full classified rows

    Returns:
        0 on success.
    """

    reviewed_path = reviewed_workbook_path or STAGE0_REVIEWED_WORKBOOK_PATH

    conn = connect_sqlite(DB_PATH)
    create_all_tables(conn)

    decisions_df = _load_review_decisions(reviewed_path)
    decision_rows = _upsert_decisions(conn, decisions_df)
    classified_df = _apply_stage1_classification(_read_stage1_base(conn))
    persisted = persist_bank_inflow_stage1(conn, classified_df)
    conn.close()

    summary_df = (
        classified_df.groupby("final_bucket", dropna=False)
        .agg(
            row_count=("id", "count"),
            total_value=("amount", "sum"),
            excluded_from_revenue=("exclude_from_revenue_metric", "sum"),
            included_in_revenue=("include_in_revenue_metric", "sum"),
        )
        .reset_index()
        .sort_values("total_value", ascending=False)
    )
    summary_df["total_value"] = summary_df["total_value"].round(2)

    stage1_needs_review_df = (
        classified_df.loc[classified_df["final_bucket"].eq("NeedsReview")]
        .sort_values(["amount", "posting_date"], ascending=[False, False])
        .reset_index(drop=True)
    )
    stage1_needs_review_df = _append_review_columns(stage1_needs_review_df)
    instructions_df = _build_review_instructions_sheet()

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(STAGE1_CLASSIFIED_WORKBOOK_PATH, engine="openpyxl") as writer:
        instructions_df.to_excel(writer, sheet_name="review_instructions", index=False)
        summary_df.to_excel(writer, sheet_name="stage1_bucket_summary", index=False)
        stage1_needs_review_df.to_excel(writer, sheet_name="stage1_needs_review", index=False)
        classified_df.to_excel(writer, sheet_name="stage1_full_classified", index=False)
        decisions_df.to_excel(writer, sheet_name="loaded_manual_decisions", index=False)

        for sheet_name, sheet_df in [
            ("review_instructions", instructions_df),
            ("stage1_bucket_summary", summary_df),
            ("stage1_needs_review", stage1_needs_review_df),
            ("stage1_full_classified", classified_df),
            ("loaded_manual_decisions", decisions_df),
        ]:
            worksheet = writer.sheets[sheet_name]
            _autofit_columns(sheet_df, worksheet)
            if sheet_name == "stage1_needs_review":
                headers = list(sheet_df.columns)
                max_row = len(sheet_df) + 1
                _apply_review_dropdowns(worksheet, headers, max_row)

    print("OK: Stage 1 inflow classification completed")
    print(f"- Reviewed workbook used: {reviewed_path}")
    print(f"- Manual decisions loaded: {decision_rows}")
    print(f"- Total inflow rows classified: {len(classified_df)}")
    print(f"- Stage 1 NeedsReview rows: {len(stage1_needs_review_df)}")
    print(f"- bank_inflow_stage1 rows persisted: {persisted}")
    print(f"- Output workbook: {STAGE1_CLASSIFIED_WORKBOOK_PATH}")
    return 0
