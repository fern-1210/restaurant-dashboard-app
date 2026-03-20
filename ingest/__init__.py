"""
Ingestion package for source-specific parsing and normalization.

We keep "how to read each raw document format" here, and keep database logic
in `scripts_pipeline/`. That separation makes it easier to learn and debug:
- ingest = turn raw docs into clean DataFrames
- warehouse = store/query those clean tables in SQLite
"""

