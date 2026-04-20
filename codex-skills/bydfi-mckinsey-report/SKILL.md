---
name: bydfi-mckinsey-report
description: Run the BYDFI one-line McKinsey-grade CEO report workflow. Use when the user says things like 跑今天增量 CEO 报告, 跑今天麦肯锡版管理报告, 先补抓再重写终版 PDF, 跑本周周日报告, or asks for the one-click daily/weekly management report flow without restating the whole context.
---

# BYDFI McKinsey Report

## Purpose

Turn a one-line user request into the fixed BYDFI report workflow:

1. Refresh coverage and controlled backfill.
2. Generate the latest daily or weekly digest.
3. Render the automated CEO draft.
4. Update the final management markdown when a final PDF is requested.
5. Validate and render the fixed desktop PDF.

## First Step

Run the repo orchestrator first:

- Daily: `python run_mckinsey_ceo_cycle.py --period daily`
- Weekly: `python run_mckinsey_ceo_cycle.py --period weekly`

If the final CEO markdown has already been revised and checked, rerun with:

- `python run_mckinsey_ceo_cycle.py --period daily --render-final-pdf`

Or use the PowerShell wrapper:

- `powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run_mckinsey_ceo_cycle.ps1 -Period daily`

## What To Read After Running

- `output/mckinsey/*_context_latest.md`
- `output/scheduled/*_ops_digest_latest.md`
- `output/scheduled/*_ceo_brief_latest.md`
- `data/reports/ceo_brief_final_send.md`

## Final PDF Rules

- If the user asks for the final CEO-facing PDF, revise `data/reports/ceo_brief_final_send.md`, not the automated digest draft.
- Keep the report management-safe and CEO-safe.
- Always run the validator and then scan the rendered PDF text.
- Final file name stays fixed as `高层决策报告_终版.pdf`.

## Guardrails

- Do not forward the automated draft as the final management version without review.
- Do not leak crawler residue, system residue, database keys, writer inner monologue, or user requirement voice.
- Use recency weighting. If 4月9日 evidence arrives after a 4月8日 version, the 4月9日 evidence wins.
- Separate system blockers from personal low performance judgments.

## Skill Chaining

- For management writing and PDF cleanup, also use `bydfi-ceo-brief`.
- For repo map, coverage, or MCP reuse, also use `bydfi-exchange-ops`.
