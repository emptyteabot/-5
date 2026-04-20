# Forbidden Language

Never let these leak into the management-facing PDF.

## Raw System And Database Residue

- `audit_records`
- `audit record`
- `document_key`
- `chat_id`
- `message_id`
- `run_id`
- `source_url`
- `source_content_hash`
- collector internals, crawl timing logs, backfill logs, or schema keys

## Robot And Tool Residue

- `Claude分析机器人`
- `审计机器人`
- any “AI second conclusion” wording
- bot routing notes
- “this PDF should not say ...”
- “automatic daily report may not be stable yet”

## Report-Writer Inner Monologue

- any caveat that breaks the fourth wall
- any line that explains how the report was generated instead of what management should decide
- any note about the writer’s own drafting concerns

## Self-Referential Or Misframed Commentary

- third-person self commentary such as `Yohan：...` inside a CEO brief
- “4 月 7 日凌晨补充进来的历史资料 ...”
- any sentence that tells the CEO about crawl timing rather than business timing

## Safer Replacements

- Replace source residue with management judgment.
- Replace implementation logs with status language like `已上线 / 待上线 / 测试中 / 验收未统一`.
- Replace tool narration with business implication and next action.
