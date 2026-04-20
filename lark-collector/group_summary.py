from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from auditor import run_group_summary
from config import BOT_DISPLAY_NAME, DB_PATH
from feishu import get_document_raw_content, list_chat_messages, send_text_to_chat
from message_sources import build_source_payload
from group_report_artifacts import write_group_report_text
from storage import list_records_for_chat
from timeline_aggregator import build_execution_snapshot
from version_plan import build_plan_context, fetch_version_plan_items

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

MAX_ITEM_CHARS = 1200
MAX_PERSON_CHARS = 4200

HARD_RISK_KEYWORDS = [
    "404",
    "邀请码",
    "奖励派发",
    "奖励",
    "ttfb",
    "gsc",
    "索引",
    "抓取",
    "站点地图",
    "内链",
    "活动",
    "爆仓",
    "卡点",
]

EVIDENCE_ACCEPTANCE_TOKENS = ("已上线", "已发布", "已交付", "已验收", "上线完成", "修复完成")


@dataclass
class PersonBucket:
    reporter_id: str
    reporter_name: str
    items: list[dict[str, Any]]


def _anchor_id_for_record(record: dict[str, Any]) -> str:
    message_id = str(record.get("message_id") or "").strip()
    if message_id:
        return f"msg:{message_id}"
    document_key = str(record.get("document_key") or "").strip()
    if document_key:
        return f"doc:{document_key}"
    source_url = str(record.get("source_url") or "").strip()
    return f"url:{source_url}" if source_url else ""


def infer_reporter_name(record: dict[str, Any]) -> str:
    reporter_name = str(record.get("reporter_name") or "").strip()
    if reporter_name:
        return reporter_name

    raw_message = record.get("raw_message_json")
    if isinstance(raw_message, dict):
        sender_candidates = raw_message.get("node", {}).get("senderCandidates", [])
        for candidate in sender_candidates:
            text = str(candidate or "").strip()
            if text:
                return text

    title = str(record.get("source_title") or "").strip()
    if title:
        # e.g. "3.24 会议总结-Ken"
        suffix = re.search(r"[-_/](?P<name>[A-Za-z][A-Za-z0-9_-]{1,24})$", title)
        if suffix:
            return suffix.group("name")
        # e.g. "会议总结（Ken）"
        bracket = re.search(r"[（(](?P<name>[^()（）]{1,24})[)）]$", title)
        if bracket:
            return bracket.group("name").strip()

    text = str(record.get("parsed_text") or "")
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    if first_line and len(first_line) <= 24 and not first_line.startswith("http"):
        return first_line

    raw_event = record.get("raw_event_json")
    if isinstance(raw_event, dict):
        sender_id = raw_event.get("event", {}).get("sender", {}).get("sender_id", {})
        for key in ("user_id", "open_id", "union_id"):
            value = sender_id.get(key)
            if value:
                return str(value)[-8:]

    sender = str(record.get("sender_id") or "").strip()
    return sender[-8:] if sender else "unknown"


def _compact_text(text: str, *, limit: int = MAX_ITEM_CHARS) -> str:
    normalized = "\n".join(line.strip() for line in text.splitlines() if line.strip()).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip() + "\n[内容过长，已截断。完整正文已入库]"


def normalize_item(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "message_id": str(record.get("message_id") or ""),
        "created_at": str(record.get("message_timestamp") or record.get("created_at") or ""),
        "source_type": str(record.get("source_type") or ""),
        "title": str(record.get("source_title") or ""),
        "content": _compact_text(str(record.get("parsed_text") or "")),
        "audit_result": str(record.get("audit_result") or ""),
        "message_type": str(record.get("message_type") or ""),
        "document_key": str(record.get("document_key") or ""),
        "source_url": str(record.get("source_url") or ""),
        "anchor_id": _anchor_id_for_record(record),
        "status": str(record.get("status") or ""),
        "error_message": str(record.get("error_message") or ""),
    }


def group_records(records: list[dict[str, Any]]) -> list[PersonBucket]:
    grouped: dict[str, PersonBucket] = {}
    for record in records:
        reporter_id = str(record.get("sender_id") or "").strip() or "unknown"
        bucket = grouped.get(reporter_id)
        if bucket is None:
            bucket = PersonBucket(
                reporter_id=reporter_id,
                reporter_name=infer_reporter_name(record),
                items=[],
            )
            grouped[reporter_id] = bucket
        bucket.items.append(normalize_item(record))
    return list(grouped.values())


def _materialize_person_items(bucket: PersonBucket) -> list[str]:
    lines: list[str] = [f"## {bucket.reporter_name} ({bucket.reporter_id})"]
    used_chars = len(lines[0])
    for idx, item in enumerate(bucket.items, 1):
        block = [
            f"[{idx}] 时间: {item['created_at']}",
            f"来源: {item['source_type']} / {item['message_type']}",
        ]
        if item["title"]:
            block.append(f"标题: {item['title']}")
        if item["document_key"]:
            block.append(f"文档键: {item['document_key']}")
        if item["content"]:
            block.append(item["content"])
        block.append("")

        block_text = "\n".join(block)
        if used_chars + len(block_text) > MAX_PERSON_CHARS:
            lines.append("[其余内容省略。完整正文已入库，可用于后续追溯]")
            break

        lines.extend(block)
        used_chars += len(block_text)
    return lines


def build_grouped_prompt_text(
    buckets: list[PersonBucket],
    *,
    start_at: datetime,
    end_at: datetime,
) -> str:
    lines = [
        f"开始时间: {start_at.isoformat()}",
        f"结束时间: {end_at.isoformat()}",
        f"汇报人数: {len(buckets)}",
        "",
    ]
    for bucket in buckets:
        lines.extend(_materialize_person_items(bucket))
    return "\n".join(lines).strip()


def _extract_issue_candidates(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    seen: set[str] = set()

    for record in records:
        title = str(record.get("source_title") or "").strip()
        content = str(record.get("parsed_text") or "").strip()
        source_type = str(record.get("source_type") or "").strip()
        if not content:
            continue

        for line in (line.strip() for line in content.splitlines() if line.strip()):
            lowered = line.lower()
            hit = next((kw for kw in HARD_RISK_KEYWORDS if kw in lowered or kw in line), "")
            if not hit:
                continue
            issue = line[:120]
            dedupe_key = issue.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            issues.append(
                {
                    "issue": issue,
                    "business_evidence": f"{source_type or 'unknown'} | {title or '无标题'} | {line[:180]}",
                    "business_anchor_ids": [_anchor_id_for_record(record)] if _anchor_id_for_record(record) else [],
                }
            )
            if len(issues) >= 12:
                return issues
    return issues


def _find_plan_evidence(issue: str, plan_items: list[dict[str, Any]]) -> tuple[str, list[str]]:
    issue_low = issue.lower()
    tokens = [
        token
        for token in re.split(r"[^0-9a-zA-Z\u4e00-\u9fff]+", issue_low)
        if len(token) >= 2
    ]

    for item in plan_items:
        title = str(item.get("title") or "").strip()
        owner = str(item.get("owner") or "").strip() or "未提供"
        status = str(item.get("status") or "").strip() or "未提供"
        due = str(item.get("due_date") or "").strip() or "未提供"
        if not title:
            continue

        lowered_title = title.lower()
        if issue_low[:8] in lowered_title or any(token in lowered_title for token in tokens):
            record_id = str(item.get("record_id") or "")
            anchor_ids = [f"plan:{record_id}"] if record_id else []
            return f"版本计划匹配：{title} | 负责人: {owner} | 状态: {status} | 截止: {due}", anchor_ids

    return "研发/计划侧证据不足（待核对）", []


def _rule_from_evidence(rd_plan_evidence: str) -> str:
    has_owner = ("负责人:" in rd_plan_evidence) and ("未提供" not in rd_plan_evidence)
    has_eta = ("截止:" in rd_plan_evidence or "ETA" in rd_plan_evidence) and ("未提供" not in rd_plan_evidence)
    has_acceptance = any(token in rd_plan_evidence for token in EVIDENCE_ACCEPTANCE_TOKENS)

    if has_owner and has_eta and has_acceptance:
        return "可追踪推进"
    if has_owner or has_eta:
        return "状态冲突-假性推进"
    return "工单断链/空转"


def _build_crosscheck_items(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    issues = _extract_issue_candidates(records)
    if not issues:
        return []

    try:
        plan_items = fetch_version_plan_items()
    except Exception:
        plan_items = []

    items: list[dict[str, str]] = []
    for issue_item in issues:
        issue = issue_item["issue"]
        business_evidence = issue_item["business_evidence"]
        rd_plan_evidence, rd_plan_anchor_ids = _find_plan_evidence(issue, plan_items)
        rule = _rule_from_evidence(rd_plan_evidence)
        items.append(
            {
                "issue": issue,
                "business_evidence": business_evidence,
                "rd_plan_evidence": rd_plan_evidence,
                "business_anchor_ids": list(issue_item.get("business_anchor_ids") or []),
                "rd_plan_anchor_ids": rd_plan_anchor_ids,
                "rule": rule,
            }
        )
    return items


def _derive_root_cause(rule: str, rd_plan_evidence: str) -> str:
    if "证据不足" in rd_plan_evidence:
        return "研发侧缺可验证排期，问题停留在业务描述层。"
    if rule == "状态冲突-假性推进":
        return "存在对接痕迹，但缺 owner、ETA、验收闭环中的关键字段。"
    if rule == "工单断链/空转":
        return "问题未转化为可执行研发任务，产品到技术交接断裂。"
    return "存在可追踪执行链路。"


def _derive_action(rule: str, issue: str) -> str:
    if rule == "状态冲突-假性推进":
        return f"请直接@技术负责人和产品负责人，今日内给出“{issue[:24]}”的YYYY-MM-DD上线日期、验收人、验收口径。"
    if rule == "工单断链/空转":
        return f"请直接@产品负责人，本周内把“{issue[:24]}”写入需求池与版本进度表并锁定研发排期。"
    return f"保持跟踪“{issue[:24]}”，按已承诺ETA验收回执。"


def _render_crosscheck_report(items: list[dict[str, str]]) -> str:
    if not items:
        return "BYDFi 产研跨源对账单 (Cross-Check Report)\n一、高危断链项（AI双向核查确权）\n无。"

    high_risk_items = [item for item in items if item["rule"] in {"状态冲突-假性推进", "工单断链/空转"}]
    pending_items = [item for item in high_risk_items if "证据不足" in item["rd_plan_evidence"]]
    confirmed_high_items = [item for item in high_risk_items if "证据不足" not in item["rd_plan_evidence"]]

    lines: list[str] = ["BYDFi 产研跨源对账单 (Cross-Check Report)", ""]

    lines.append("一、高危断链项（AI双向核查确权）")
    if not confirmed_high_items:
        lines.append("当前未发现可直接确权的高危断链项。")
    else:
        for idx, item in enumerate(confirmed_high_items, 1):
            issue = item["issue"]
            business = item["business_evidence"]
            rd = item["rd_plan_evidence"]
            rule = item["rule"]
            if rule == "状态冲突-假性推进":
                conclusion = "研发侧存在处理话术或局部排期，但缺验收闭环。"
            else:
                conclusion = "未检索到可执行研发排期，属于断链。"

            lines.extend(
                [
                    f"事件{idx}：{issue}",
                    f"业务侧诉求源：{business}",
                    f"业务侧锚点：{', '.join(item.get('business_anchor_ids') or ['无'])}",
                    f"研发侧核对源：{rd}",
                    f"研发侧锚点：{', '.join(item.get('rd_plan_anchor_ids') or ['无'])}",
                    f"AI核对结论：【{rule}】{conclusion}",
                    f"归因：{_derive_root_cause(rule, rd)}",
                    f"追责建议：{_derive_action(rule, issue)}",
                    "",
                ]
            )

    lines.append("二、待核对项（证据不足，不下重判）")
    if not pending_items:
        lines.append("无。")
    else:
        for idx, item in enumerate(pending_items, 1):
            lines.extend(
                [
                    f"事件{idx}：{item['issue']}",
                    f"缺失证据：{item['rd_plan_evidence']}",
                    f"已知业务锚点：{', '.join(item.get('business_anchor_ids') or ['无'])}",
                    "需要谁补：产品负责人、技术负责人、项目经理（补 owner、ETA、验收口径）。",
                    "",
                ]
            )

    lines.append("三、本周必须落地动作（最多3条）")
    must_actions = [
        _derive_action(item["rule"], item["issue"]) for item in (confirmed_high_items + pending_items)
    ][:3]
    while len(must_actions) < 3:
        must_actions.append("补齐本周新增问题的 owner、ETA、验收口径，并同步到版本进度表。")
    lines.extend([f"{idx}. {action}" for idx, action in enumerate(must_actions, 1)])
    return "\n".join(lines).strip()


def _build_crosscheck_material(records: list[dict[str, Any]]) -> str:
    items = _build_crosscheck_items(records)
    if not items:
        return ""

    lines = ["跨源对账素材："]
    for idx, item in enumerate(items, 1):
        issue = item["issue"]
        business_evidence = item["business_evidence"]
        rd_plan_evidence = item["rd_plan_evidence"]
        rule = item["rule"]
        lines.extend(
            [
                f"[{idx}] 事件: {issue}",
                f"业务侧证据: {business_evidence}",
                f"业务侧锚点: {', '.join(item.get('business_anchor_ids') or ['无'])}",
                f"研发/计划侧证据: {rd_plan_evidence}",
                f"研发/计划侧锚点: {', '.join(item.get('rd_plan_anchor_ids') or ['无'])}",
                f"规则判定: {rule}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def _load_history_backfill(
    chat_id: str,
    *,
    start_at: datetime,
    end_at: datetime,
    existing_ids: set[str],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    try:
        history_items = list_chat_messages(
            chat_id,
            start_at=start_at.isoformat(),
            end_at=end_at.isoformat(),
            page_size=50,
        )
    except Exception:
        return items

    for item in history_items:
        message_id = str(item.get("message_id") or "")
        if not message_id or message_id in existing_ids:
            continue

        payload = build_source_payload(
            {
                "message_id": message_id,
                "chat_id": str(item.get("chat_id") or chat_id),
                "message_type": str(item.get("msg_type") or ""),
                "content": str(item.get("body", {}).get("content") or ""),
            },
            fetch_document_text=get_document_raw_content,
        )

        body = str(payload.get("body") or "").strip()
        if not body:
            continue

        try:
            created_at = datetime.fromtimestamp(
                int(item.get("create_time", "0")) / 1000,
                tz=timezone.utc,
            ).isoformat()
        except Exception:
            created_at = end_at.isoformat()

        items.append(
            {
                "message_id": message_id,
                "sender_id": str(item.get("sender", {}).get("id") or ""),
                "reporter_name": "",
                "source_type": str(payload.get("source_type") or ""),
                "source_title": str(payload.get("title") or ""),
                "source_url": "",
                "document_key": "",
                "parsed_text": body,
                "message_type": str(item.get("msg_type") or ""),
                "created_at": created_at,
                "message_timestamp": created_at,
                "status": "parsed",
                "error_message": "",
                "raw_event_json": {},
                "raw_message_json": {},
            }
        )
        existing_ids.add(message_id)

    return items


def _load_group_records(
    chat_id: str,
    *,
    start_at: datetime,
    end_at: datetime,
    statuses: list[str] | None = None,
) -> list[dict[str, Any]]:
    db_records = list_records_for_chat(
        chat_id,
        start_at=start_at.isoformat(),
        end_at=end_at.isoformat(),
        statuses=statuses or ["parsed", "queued", "auditing", "audited", "replied", "error"],
        db_path=DB_PATH,
    )

    useful = [
        record
        for record in db_records
        if str(record.get("parsed_text") or "").strip()
        and str(record.get("source_type") or "") not in {"unsupported_message"}
    ]

    existing_ids = {str(record.get("message_id") or "") for record in useful}
    useful.extend(
        _load_history_backfill(
            chat_id,
            start_at=start_at,
            end_at=end_at,
            existing_ids=existing_ids,
        )
    )
    useful.sort(key=lambda item: str(item.get("message_timestamp") or item.get("created_at") or ""))
    return useful


def _serialize_buckets(buckets: list[PersonBucket]) -> list[dict[str, Any]]:
    return [asdict(bucket) for bucket in buckets]


def build_group_summary_materials(
    chat_id: str,
    *,
    start_at: datetime,
    end_at: datetime,
    statuses: list[str] | None = None,
) -> dict[str, Any]:
    records = _load_group_records(chat_id, start_at=start_at, end_at=end_at, statuses=statuses)
    buckets = group_records(records)
    grouped_text = build_grouped_prompt_text(buckets, start_at=start_at, end_at=end_at) if buckets else ""
    crosscheck_items = _build_crosscheck_items(records)
    crosscheck_text = _build_crosscheck_material(records)
    window_label = f"{start_at.strftime('%Y-%m-%d %H:%M')} ~ {end_at.strftime('%Y-%m-%d %H:%M')}"

    return {
        "chat_id": chat_id,
        "records": records,
        "buckets": buckets,
        "serialized_buckets": _serialize_buckets(buckets),
        "grouped_text": grouped_text,
        "crosscheck_items": crosscheck_items,
        "crosscheck_text": crosscheck_text,
        "window_label": window_label,
        "plan_context": build_plan_context(),
    }


def generate_group_summary(
    chat_id: str,
    *,
    start_at: datetime,
    end_at: datetime,
    statuses: list[str] | None = None,
) -> tuple[str, list[PersonBucket], list[dict[str, Any]]]:
    materials = build_group_summary_materials(
        chat_id,
        start_at=start_at,
        end_at=end_at,
        statuses=statuses,
    )
    records = materials["records"]
    buckets = materials["buckets"]
    if not buckets:
        return "在指定时间窗口内未找到可分析的汇报内容。", [], []

    crosscheck_items = list(materials.get("crosscheck_items") or [])
    if crosscheck_items:
        return _render_crosscheck_report(crosscheck_items), buckets, records

    audit_input = materials["grouped_text"]
    if materials["crosscheck_text"]:
        audit_input = f"{materials['crosscheck_text']}\n\n{audit_input}".strip()

    summary = run_group_summary(
        audit_input,
        window_label=materials["window_label"],
        plan_context=materials["plan_context"],
    )
    return summary, buckets, records


def build_group_summary_report(
    chat_id: str,
    *,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, Any]:
    summary, buckets, records = generate_group_summary(chat_id, start_at=start_at, end_at=end_at)
    title = f"{BOT_DISPLAY_NAME}：群汇总分析（{len(buckets)}人）"
    serialized_buckets = _serialize_buckets(buckets)

    snapshot = build_execution_snapshot(
        title=title,
        chat_id=chat_id,
        start_at=start_at.isoformat(),
        end_at=end_at.isoformat(),
        people_count=len(serialized_buckets),
        summary_text=summary,
        records=records,
        buckets=serialized_buckets,
    )
    report_id, text_path, json_path, payload = write_group_report_text(
        title=title,
        chat_id=chat_id,
        start_at=start_at.isoformat(),
        end_at=end_at.isoformat(),
        people_count=len(serialized_buckets),
        summary_text=summary,
        records=records,
        buckets=serialized_buckets,
    )
    snapshot["textPath"] = str(text_path)
    snapshot["jsonPath"] = str(json_path)
    snapshot["evidenceIndex"] = payload.get("evidenceIndex") or {}
    return {
        "title": title,
        "summary": summary,
        "snapshot": snapshot,
        "people_count": len(serialized_buckets),
        "buckets": buckets,
        "records": records,
        "report_id": report_id,
        "text_path": text_path,
        "json_path": json_path,
    }


def send_group_summary_to_chat(
    chat_id: str,
    *,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, Any]:
    report = build_group_summary_report(chat_id, start_at=start_at, end_at=end_at)
    text = (
        f"{report['title']}\n"
        f"时间窗口：{start_at.strftime('%Y-%m-%d %H:%M')} ~ {end_at.strftime('%Y-%m-%d %H:%M')}\n"
        f"{report['summary']}"
    )
    return send_text_to_chat(chat_id, text)


def parse_summary_command(text: str) -> int | None:
    raw = text.strip()
    normalized = raw.lower()

    if "汇总" not in raw and "summary" not in normalized:
        return None
    if "今天" in raw:
        return 24
    if "本周" in raw:
        return 24 * 7

    match = re.search(r"(\d+)\s*(h|hour|hours|小时)", normalized)
    if match:
        return max(1, int(match.group(1)))
    return 24


def default_window(hours: int) -> tuple[datetime, datetime]:
    end_at = datetime.now(timezone.utc)
    start_at = end_at - timedelta(hours=hours)
    return start_at, end_at


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate group summary from stored Lark reports.")
    parser.add_argument("--chat-id", required=True)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--send", action="store_true")
    args = parser.parse_args()

    start_at, end_at = default_window(args.hours)
    report = build_group_summary_report(args.chat_id, start_at=start_at, end_at=end_at)
    print(report["summary"])
    print(f"\nPeople grouped: {report['people_count']}")
    print(f"Report text path: {report['text_path']}")
    print(f"Report json path: {report['json_path']}")
    if args.send:
        result = send_group_summary_to_chat(args.chat_id, start_at=start_at, end_at=end_at)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
