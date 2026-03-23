# Outflow partner review (Stage 0 → Stage 1)

## What this is for
You and your partner classify **2024–2025 bank outflows** (debits) using the same workbook pattern as inflows. Stage 0 is read-only triage; Stage 1 writes your decisions into `transaction_category_map` so the dashboard stays aligned.

## Files
- **Generated (do not hand-edit as “source of truth”)**: `data/audit/stage0_outflow_audit.xlsx`
- **Save your work as**: `data/audit/stage0_outflow_audit_reviewed.xlsx`
- **After Stage 1 runs**: `data/audit/stage1_outflow_classification.xlsx` (for iterative `UNMAPPED` cleanup)

## Generate Stage 0
From the repo root:

```bash
python3 scripts_orchestrator/L0_generate_outflow_audit_workbook.py
```

## Which sheets to edit
1. Read `review_instructions` and `taxonomy_reference`.
2. Work in:
   - `needs_review` — no category in the current map
   - `flagged_outflows` — intercompany transfer outflows (sanity check)

## Columns you fill (review only)
- `final_category` (dropdown — must match taxonomy level 1 / partner structure)
- `final_subcategory` (dropdown — must match taxonomy level 2)
- `confidence` — `high`, `medium`, `low`
- `decision_notes` — short rationale
- `reviewed_by` — `JULIAN`, `SNIDER`, etc.
- `reviewed_at` — `YYYY-MM-DD`

## Do not change
Keep traceability intact:
- `description_norm`, `amount`, `source_file`, `source_row`, dates, `id` (if present on exports)

## Apply Stage 1 (writes to SQLite)
After saving the reviewed file:

```bash
python3 scripts_orchestrator/S1_apply_outflow_classification.py
```

This upserts rows into `transaction_category_map` with `source_label = stage1_outflow_review`.

## If UNMAPPED remains
Open `data/audit/stage1_outflow_classification.xlsx` → sheet `stage1_needs_review`, fill the same review columns, save, then run:

```bash
python3 scripts_orchestrator/S1_apply_outflow_classification.py \
  data/audit/stage0_outflow_audit_reviewed.xlsx \
  data/audit/stage1_outflow_classification.xlsx
```

## Hand back to engineering
Say: **“outflow reviewed + Stage 1 applied”** and note any `WARNING: Invalid pair` lines from the terminal (means category/subcategory combo was not in the taxonomy reference).

## Why Stage 0 vs Stage 1
- **Stage 0**: safe audit + partner workflow; **no database writes**.
- **Stage 1**: persist decisions so classification is **repeatable** next month and the **dashboard** reflects your mapping.
