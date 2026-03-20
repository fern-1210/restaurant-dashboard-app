"""
# What
Generate an Excel template for your partner to map bank transaction descriptions
to categories.

# Why
Your partner needs a clear list of every unique transaction description in your
data, plus the approved category hierarchy, so they can fill in category and
subcategory without guessing.

# How
Run from repo root:
  .venv/bin/python scripts_orchestrator/L4_generate_partner_mapping_template.py

Output:
  data/partner_input/venn_category_mapping_TEMPLATE.xlsx

Partner instructions:
  1. Open the template.
  2. Fill in the "category" and "subcategory" columns for each row.
  3. Use the "Taxonomy Reference" sheet to pick valid values.
  4. Save as: data/partner_input/venn_category_mapping.xlsx
  5. Run: .venv/bin/python scripts_orchestrator/L4_load_category_map_to_sqlite.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import (
    DB_PATH,
    PARTNER_INPUT_DIR,
    PARTNER_MAPPING_TEMPLATE_PATH,
)


def main() -> int:
    """
    Generate partner mapping template from warehouse data.

    Returns:
        Exit code (0 on success).
    """
    # Ensure output directory exists.
    PARTNER_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = connect_sqlite(DB_PATH)

    # ---- Sheet 1: Mapping (one row per unique description_norm) ----
    # Pull all unique normalized descriptions from bank transactions.
    mapping_df = pd.read_sql_query(
        """
        SELECT DISTINCT
            description_norm,
            MIN(description_raw) AS description_raw_sample
        FROM bank_transactions
        WHERE description_norm IS NOT NULL
          AND TRIM(description_norm) != ''
        GROUP BY description_norm
        ORDER BY description_norm
        """,
        conn,
    )

    # Add empty columns for partner to fill.
    mapping_df["category"] = ""
    mapping_df["subcategory"] = ""
    mapping_df["notes"] = ""

    # Reorder so partner sees: key columns first, then fill-in columns.
    mapping_df = mapping_df[
        ["description_norm", "description_raw_sample", "category", "subcategory", "notes"]
    ]

    # ---- Sheet 2: Taxonomy Reference (approved hierarchy) ----
    # Pull category hierarchy for reference (if taxonomy table exists and has data).
    try:
        taxonomy_df = pd.read_sql_query(
            """
            SELECT level_1 AS category, level_2 AS subcategory, level_3
            FROM category_taxonomy
            ORDER BY level_1, level_2, level_3
            """,
            conn,
        )
    except Exception:
        # Table might not exist or be empty; use placeholder.
        taxonomy_df = pd.DataFrame(
            columns=["category", "subcategory", "level_3"],
            data=[
                ["(No taxonomy loaded yet)", "(Run load_category_map with hierarchy Excel first)", ""],
            ],
        )

    conn.close()

    # ---- Write Excel with two sheets ----
    with pd.ExcelWriter(PARTNER_MAPPING_TEMPLATE_PATH, engine="openpyxl") as writer:
        # Sheet 1: the mapping template (partner fills category, subcategory).
        mapping_df.to_excel(writer, sheet_name="Mapping", index=False)
        # Sheet 2: reference only (partner uses this to pick valid values).
        taxonomy_df.to_excel(writer, sheet_name="Taxonomy Reference", index=False)

    # Print clear next steps.
    print("OK: partner mapping template generated")
    print(f"- Template: {PARTNER_MAPPING_TEMPLATE_PATH}")
    print(f"- Rows to map: {len(mapping_df)}")
    print()
    print("Partner instructions:")
    print("  1. Open the template in Excel or Google Sheets.")
    print("  2. Fill in 'category' and 'subcategory' for each row.")
    print("  3. Use 'Taxonomy Reference' sheet for valid values.")
    print("  4. Save as: data/partner_input/venn_category_mapping.xlsx")
    print("  5. Run: .venv/bin/python scripts_orchestrator/L4_load_category_map_to_sqlite.py")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
