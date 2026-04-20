---
name: bydfi-ceo-brief
description: Create or update BYDFI management-facing CEO brief PDFs from Lark-derived chats, meeting summaries, weekly reports, and cloud documents. Use when asked to produce a 高层决策报告 / CEO brief / management PDF, to fold in latest Lark messages with recency weighting, to convert noisy analyst output into CEO-safe language, to add meeting follow-up tracking or Ella project progress tables, or to judge whether the current output satisfies Kater's reporting requirements.
---

# BYDFI CEO Brief

## Workflow

### 1. Build the evidence boundary

- Prefer the newest business-bearing Lark records and source documents, but downweight late backfills, system noise, join/rename messages, and bot second-pass chatter.
- Use cached report artifacts under `data/reports/` when they already contain structured extracts for the same day.
- Use `tmp_desktop_audit_records.sqlite3` to verify raw requirements or specific claims when the user asks whether something truly satisfies Kater.
- Treat internal evidence-heavy drafts as analyst material only; never forward them directly to management.

### 2. Load the operating rules

- Read [references/kater-requirements.md](references/kater-requirements.md) when the user asks whether the current report satisfies Kater.
- Read [references/output-structure.md](references/output-structure.md) before drafting or revising the CEO brief.
- Read [references/forbidden-language.md](references/forbidden-language.md) before shipping anything management-facing.

### 3. Draft the management brief

- Keep the final management brief in Chinese.
- Lead with one management judgment, then today’s decisions, then the meeting follow-up page.
- Weight evidence by recency and operating relevance. A fresh weekly report beats an old backfilled PRD unless the PRD is needed to explain the project line.
- Use exact dates, not vague phrases like “today” or “latest,” when the timing matters.
- Convert source noise into management language. Remove database residue, collector residue, robot residue, and report-writer inner monologue.
- Default to an explicit meeting follow-up tracker:
  `会议 / 待办进展 / 结果缺口 / 今日新增待办`
- When Ella or perpetual-project coordination matters, default to an explicit project progress table:
  `事项 / 当前阶段 / 跨角色串联 / 主责任人 / 下一里程碑`
- If the user wants “same logic as last report,” preserve the management cadence rather than copying prior wording.

### 4. Render and verify

- Update the markdown source first. In this repo the current target is usually `data/reports/ceo_brief_YYYYMMDD.md`.
- Render the PDF with the repo script if it exists:
  `python generate_ceo_brief_pdf.py`
- Run the skill validator before final delivery:
  `python <codex-home>/skills/bydfi-ceo-brief/scripts/verify_ceo_brief.py <path-to-md>`
- Confirm the desktop PDF copy exists and its timestamp changed after rendering.
- If the renderer supports extraction, spot-check that required headings and table labels are present in the PDF text.

### 5. Answer Kater compliance questions truthfully

- Separate `内容是否覆盖` from `流程是否闭环`.
- A report can satisfy most analysis requirements while still failing the automation/timeliness bar.
- Do not say “fully satisfied” unless all three are true:
  1. Meeting conclusions, department issues, and actionable suggestions are present.
  2. Weekly reports and project lines are actually chained into management judgment.
  3. Stable pre-meeting automatic delivery and manual fact-check control are verified, not assumed.

## Repo Notes

- Management markdown source: usually `data/reports/ceo_brief_YYYYMMDD.md`
- Internal evidence draft: usually `data/reports/management_report_YYYYMMDD.md`
- Management PDF renderer in this repo: `generate_ceo_brief_pdf.py`
- Internal PDF renderer in this repo: `generate_management_report_pdf.py`

## Exit Criteria

- The PDF reads like a real executive brief, not a crawler dump.
- The report includes a meeting follow-up tracker and, when relevant, an Ella progress table.
- The brief contains no forbidden language from [references/forbidden-language.md](references/forbidden-language.md).
- If asked about Kater coverage, the answer distinguishes `已满足 / 部分满足 / 未满足` with evidence-based reasoning.
