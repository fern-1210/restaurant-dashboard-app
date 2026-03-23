# Stage 0 Partner Review Guide

## What this file is for
This guide explains exactly how you and your business partner should review the Stage 0 inflow workbook, what to edit, and how to hand it back for Stage 1 processing.

## Files involved
- Input workbook to review: `data/audit/stage0_inflow_audit.xlsx`
- Reviewed workbook to save: `data/audit/stage0_inflow_audit_reviewed.xlsx`
- Stage 1 output workbook (generated after processing): `data/audit/stage1_inflow_classification.xlsx`

## Review workflow (SNIDER + JULIAN)
- Open `stage0_inflow_audit.xlsx`.
- Read `review_instructions` sheet first.
- Work mainly in:
  - `needs_review` (unmatched inflows)
  - `expense_flagged` (positive inflows that currently look like expense labels)
- Fill only the review columns:
  - `final_bucket` (required)
  - `final_subbucket` (optional)
  - `owner_tag` (optional)
  - `confidence` (optional)
  - `decision_notes` (recommended)
  - `reviewed_by` (recommended)
  - `reviewed_at` (recommended, `YYYY-MM-DD`)

## Allowed values (dropdown-enabled)
- `final_bucket`:
  - `PartnerInvestment`
  - `InterbankTransfer`
  - `OperatingRevenue`
  - `GrantIncome`
  - `NeedsReview`
- `owner_tag`:
  - `SNIDER`, `JULIAN`, `BOTH`, `N-A`
- `confidence`:
  - `high`, `medium`, `low`

## What NOT to change
Do not edit traceability fields. These identify the exact source transaction:
- `description_norm`
- `amount`
- `source_file`
- `source_row`
- `posting_date`
- `value_date`

## Decision rules to follow
- If not sure, keep `final_bucket = NeedsReview` and explain why in `decision_notes`.
- Interbank movements between your own accounts should be `InterbankTransfer`.
- Owner/partner injections should be `PartnerInvestment`.
- EU Commission or grant-type inflows should be `GrantIncome`.
- Only assign `OperatingRevenue` when clearly business revenue (for example POS/card/cash operations).

## Save and hand back
- Save the reviewed file as:
  - `data/audit/stage0_inflow_audit_reviewed.xlsx`
- Keep the sheet names unchanged.
- Keep column names unchanged.

## Apply your reviewed decisions
Run:

```bash
python3 scripts_orchestrator/S1_apply_inflow_classification.py
```

This will:
- load your reviewed decisions into the decision map,
- reclassify all inflows,
- output `data/audit/stage1_inflow_classification.xlsx`.

## If you want to amend decisions later
- Re-open `data/audit/stage0_inflow_audit_reviewed.xlsx`.
- Update any review columns.
- Re-run Stage 1 command again.
- Latest decisions overwrite previous ones by `description_norm`.

## How to share it back with me
- Tell me you have updated `data/audit/stage0_inflow_audit_reviewed.xlsx`.
- I will run Stage 1 and summarize:
  - what changed by bucket,
  - what is still in `NeedsReview`,
  - what should be prioritized next.
