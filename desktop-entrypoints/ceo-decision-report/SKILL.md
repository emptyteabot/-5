---
name: ceo-decision-report
description: Generate BYDFI daily or weekly CEO decision report PDFs from the latest Lark messages, meeting summaries, weekly reports, and project docs. Use when the user says 跑今天CEO日报, 跑本周CEO周报, 强刷8群后重跑, 检查Lark覆盖, or wants the reusable SOP and final PDF flow on this machine.
---

# CEO Decision Report

## Load Order

- Read [references/source-map.md](references/source-map.md) first.
- Read [references/report-rules.md](references/report-rules.md) before editing any management-facing markdown.
- Read [references/core-groups.md](references/core-groups.md) when the user asks whether Lark coverage is complete or asks for `不重不漏`.

## Default Workflow

1. If the user asks whether the current evidence is complete, run `scripts/check-coverage.ps1` first.
2. If the user asks for the latest possible daily or weekly report, run `scripts/run-daily.ps1` or `scripts/run-weekly.ps1`.
3. If freshness is challenged or the user explicitly wants a hard refresh, run `scripts/force-refresh-8-groups.ps1` before rerunning the weekly flow.
4. If the user wants the final CEO-facing PDF, revise the repo source file `data/reports/ceo_brief_final_send.md`, not the automated draft.
5. After the final markdown is revised, run `scripts/render-final-pdf.ps1`.

## Guardrails

- Final PDF must stay management-safe: no crawler residue, no database fields, no bot narration, no writer inner monologue.
- Use exact dates when timing matters.
- Keep the translation line neutral unless the user explicitly asks for a translation-specific diagnosis.
- Compress people judgments to `可以继续放权` and `需要管理纠偏`; do not dump a full roster unless asked.
- Prefer raw SQLite evidence and source documents over garbled auto-generated markdown summaries.
