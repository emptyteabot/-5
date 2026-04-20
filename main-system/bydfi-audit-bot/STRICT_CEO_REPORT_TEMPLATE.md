# Strict CEO Report Template

Use this template only after the evidence guard passes. If the evidence guard status is `fail`, replace the whole report with a failure notice and the blocking reasons.

## 1. Data Boundary

- Report generated at:
- Evidence window:
- Latest first-order business evidence timestamp:
- Evidence sources included:
- Evidence sources excluded:
- Confidence level:

Required rule:
If the latest evidence is stale, incomplete, or mixed with AI summary artifacts, say so explicitly at the top.

## 2. Top 3 Management Issues

For each issue, use this structure:

1. Issue:
2. Why it matters:
3. Evidence:
4. Owner:
5. ETA / missing ETA:
6. Current blocker:
7. Confidence:

Hard rule:
No issue may be listed without direct evidence and an owner or a clearly stated "owner missing".

## 3. Delay Risk

For each item:

- Item:
- Baseline date:
- Current status:
- Delay signal:
- Evidence:
- Confidence:

Hard rule:
Do not say "延期风险高" unless there is a baseline date or explicit expected milestone. If no baseline exists, say "无法判断延期，只能判断排期治理不足".

## 4. Personal Execution Signal

For each person:

- Name:
- Verified delivered items:
- Verified in-progress items:
- Missing evidence:
- Risk signal:
- Confidence:

Hard rule:
This is execution signal, not HR evaluation.
Do not write personality judgment.
Do not infer efficiency from attendance, volume of messages, or duplicated weekly reports.

## 5. Department Feedback

For each department:

- Department:
- Current situation:
- Main issue:
- Evidence:
- Suggested action:
- Confidence:

Hard rule:
If department history is incomplete or low-quality, say "历史材料不足，结论仅基于当前周期".

## 6. Alignment With BYDFI Direction

For each major function or project:

- Initiative:
- Claimed business goal:
- Observed direction fit:
- Evidence:
- Main gap:

Hard rule:
Direction alignment must be tied to business goal, not just technical completion.

## 7. Unknowns And Blockers

List everything the system cannot responsibly conclude.

Examples:

- Missing owner
- Missing ETA
- Missing acceptance metric
- Only AI summary available
- Only noisy / incomplete history available
- Pipeline stale or degraded

This section is mandatory.

## 8. Solution Landing

This section is mandatory for every final management PDF.

For each major issue, use this structure:

- Problem:
- Fix:
- Owner:
- Checkpoint:
- Acceptance metric:

Hard rule:
Do not end the report with diagnosis alone.
Every top issue must be pushed into a concrete management action.
Acceptance must be observable in business terms, not just "continue follow-up" or "speed up communication".

## 9. Forbidden Output Patterns

Never write these unless supported by cited evidence:

- "明显空转"
- "没有任何部门"
- "团队普遍"
- "已经形成共识"
- "确定延期"
- "个人效能较差"
- "方向错误"

Replace them with evidence-bound phrasing:

- "当前证据显示"
- "本周期可见"
- "基于已有材料"
- "尚无法确认"
- "缺少 ETA / owner / 验收标准"

## 10. Hard Failure Notice

If evidence guard fails, output only:

1. Guard status
2. Blocking reasons
3. What can still be said safely
4. What must not be concluded yet
