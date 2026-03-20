"""
# What
Central place for "where things live" on disk.

# Why
Hardcoding paths across multiple scripts becomes error-prone as the project grows.
Putting them here makes it obvious and consistent:
- where the SQLite database file is
- where we read warehouse CSVs from
- where we write audit reports

# How
We use pathlib.Path so paths work cross-platform and are easy to join.


#Julian notes
- allows for easy changing of directories and files; avoids OS path confusion between backslash and slash style paths.


"""

from __future__ import annotations

from pathlib import Path

# ---- Project directories (relative to repo root) ----

# Raw Vendus revenue CSVs (Layer 1 input).
RAW_VENN_REVENUE_DIR = Path("raw_docs") / "venn_revenue"

# Raw bank statement files (Layer 3 input).
RAW_BANK_DIR = Path("raw_docs") / "bank_statements"

# "Warehouse" directory: clean tables, including the SQLite DB file.
WAREHOUSE_DIR = Path("data") / "warehouse"

# "Reports" directory: audit outputs, duplicates lists, etc.
REPORTS_DIR = Path("data") / "reports"

# SQLite database file (single file, many tables).
DB_PATH = WAREHOUSE_DIR / "venn.db"

# Existing trusted revenue CSV that you already generate (Layer 1 output).
REVENUE_DAILY_CSV_PATH = WAREHOUSE_DIR / "revenue_daily.csv"

# Partner input: where mapping templates and filled mapping files live.
# (data/ is gitignored so sensitive business data stays local.)
PARTNER_INPUT_DIR = Path("data") / "partner_input"

# Filled mapping files: partner can save as .xlsx or .xlsm.
# Load script will prefer these before fallback files.
PARTNER_MAPPING_XLSX_PATH = PARTNER_INPUT_DIR / "venn_category_mapping.xlsx"
PARTNER_MAPPING_XLSM_PATH = PARTNER_INPUT_DIR / "venn_category_mapping.xlsm"

# Generated template file (run script to create).
PARTNER_MAPPING_TEMPLATE_PATH = PARTNER_INPUT_DIR / "venn_category_mapping_TEMPLATE.xlsx"

# Fallback paths for hierarchy-only Excel exports (L4).
DEFAULT_CATEGORY_XLSX_PATH = Path.home() / "Downloads" / "venn_category_structure.xlsx"
DEFAULT_CATEGORY_XLSM_PATH = Path.home() / "Downloads" / "venn_category_structure.xlsm"

