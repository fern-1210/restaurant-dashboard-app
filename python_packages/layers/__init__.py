"""
# What
Layer-aligned entrypoints for the ETL pipeline (L1 → L2 → L3 → L4).

# Why
Reading code in layer order makes the data flow obvious. Each module wraps
the underlying parsing/loading logic so scripts stay small and the "layer story"
is visible at the top of each orchestration script.

# How
- l1_revenue: Vendus CSVs → revenue_daily.csv + audits
- l2_revenue_sqlite: revenue_daily.csv → SQLite revenue_daily table
- l3_bank_sqlite: bank statement files → SQLite bank_transactions table
- l4_category_sqlite: category Excel → SQLite transaction_category_map + taxonomy
"""
