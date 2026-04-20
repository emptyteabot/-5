from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from config import REPORTS_DIR

STATUS_KEYWORDS = {
    "delivered": ["已上线", "已发布", "已完成", "已交付", "已验收", "上线完成"],
    "testing": ["转测试", "测试中", "联调中", "联调", "待测试"],
    "review": ["待评审", "评审中", "待确认"],
    "developing": ["开发中", "进行中", "优化中", "跟进中"],
    "blocked": ["阻塞", "卡住", "依赖", "等待", "延期", "风险"],
}

SOURCE_COLORS = {
    "external_document_message": "#00f5c4",
    "document_message": "#00f5c4",
    "document_link_message": "#27c6ff",
    "external_text_message": "#7cff5f",
    "text_message": "#7cff5f",
    "post_message": "#ff9f4a",
    "interactive_message": "#f66cff",
}


def _parse_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _source_label(value: str) -> str:
    mapping = {
        "external_document_message": "文档全文",
        "document_message": "文档全文",
        "document_link_message": "文档链接",
        "external_text_message": "文字汇报",
        "text_message": "文字汇报",
        "post_message": "富文本",
        "interactive_message": "卡片",
    }
    return mapping.get(value, value or "未知来源")


def _source_color(value: str) -> str:
    return SOURCE_COLORS.get(value, "#5b6b8a")


def _status_counts(text: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for key, keywords in STATUS_KEYWORDS.items():
        out[key] = sum(text.count(keyword) for keyword in keywords)
    return out


def _anchor_id_for_record(record: dict[str, object]) -> str:
    message_id = str(record.get("message_id") or "").strip()
    if message_id:
        return f"msg:{message_id}"
    document_key = str(record.get("document_key") or "").strip()
    if document_key:
        return f"doc:{document_key}"
    source_url = str(record.get("source_url") or "").strip()
    return f"url:{source_url}" if source_url else ""


def _build_anchor_capsule(record: dict[str, object]) -> dict[str, object] | None:
    anchor_id = _anchor_id_for_record(record)
    if not anchor_id:
        return None
    preview = " ".join(str(record.get("parsed_text") or "").split())
    if len(preview) > 220:
        preview = preview[:220].rstrip() + "..."
    return {
        "anchorId": anchor_id,
        "messageId": str(record.get("message_id") or ""),
        "chatId": str(record.get("chat_id") or ""),
        "time": str(record.get("message_timestamp") or record.get("created_at") or ""),
        "reporterName": str(record.get("reporter_name") or record.get("sender_id") or "unknown"),
        "senderId": str(record.get("sender_id") or ""),
        "sourceType": str(record.get("source_type") or ""),
        "messageType": str(record.get("message_type") or ""),
        "title": str(record.get("source_title") or ""),
        "sourceUrl": str(record.get("source_url") or ""),
        "documentKey": str(record.get("document_key") or ""),
        "status": str(record.get("status") or ""),
        "preview": preview,
    }


def build_group_report_payload(
    *,
    title: str,
    chat_id: str,
    start_at: str,
    end_at: str,
    people_count: int,
    summary_text: str,
    records: list[dict],
    buckets: list[dict],
) -> dict:
    event_rows = []
    source_counter: Counter[str] = Counter()
    risk_counter: Counter[str] = Counter()
    daily_counter: Counter[str] = Counter()
    document_count = 0
    evidence_index: dict[str, dict[str, object]] = {}

    for record in records:
        time_value = str(record.get("message_timestamp") or record.get("created_at") or "")
        dt = _parse_time(time_value)
        day_key = dt.strftime("%m-%d") if dt else "unknown"
        daily_counter[day_key] += 1

        source_type = str(record.get("source_type") or "")
        source_counter[source_type] += 1
        if str(record.get("document_key") or ""):
            document_count += 1

        counts = _status_counts(str(record.get("parsed_text") or ""))
        for key, value in counts.items():
            risk_counter[key] += value
        anchor = _build_anchor_capsule(record)
        anchor_id = ""
        if anchor:
            anchor_id = str(anchor["anchorId"])
            evidence_index[anchor_id] = anchor

        event_rows.append(
            {
                "time": time_value,
                "reporter": str(record.get("reporter_name") or record.get("sender_id") or "unknown"),
                "sourceType": source_type,
                "sourceLabel": _source_label(source_type),
                "sourceColor": _source_color(source_type),
                "title": str(record.get("source_title") or ""),
                "documentKey": str(record.get("document_key") or ""),
                "anchorId": anchor_id,
                "status": str(record.get("status") or ""),
                "error": str(record.get("error_message") or ""),
            }
        )

    people_cards = []
    for bucket in buckets:
        items = list(bucket.get("items") or [])
        sorted_items = sorted(items, key=lambda item: str(item.get("created_at") or ""))
        source_mix = Counter(str(item.get("source_type") or "") for item in sorted_items)
        doc_count = sum(1 for item in sorted_items if str(item.get("document_key") or ""))
        latest = sorted_items[-1] if sorted_items else {}
        statuses = Counter()
        for item in sorted_items:
            for key, value in _status_counts(str(item.get("content") or "")).items():
                statuses[key] += value

        people_cards.append(
            {
                "reporterId": str(bucket.get("reporter_id") or ""),
                "reporterName": str(bucket.get("reporter_name") or "unknown"),
                "reportCount": len(sorted_items),
                "documentCount": doc_count,
                "latestTime": str(latest.get("created_at") or ""),
                "latestTitle": str(latest.get("title") or ""),
                "latestAnchorId": str(latest.get("anchor_id") or ""),
                "sourceMix": [
                    {"label": _source_label(key), "value": value, "color": _source_color(key)}
                    for key, value in source_mix.items()
                ],
                "statusMix": dict(statuses),
                "events": [
                    {
                        "time": str(item.get("created_at") or ""),
                        "title": str(item.get("title") or ""),
                        "sourceLabel": _source_label(str(item.get("source_type") or "")),
                        "sourceColor": _source_color(str(item.get("source_type") or "")),
                        "documentKey": str(item.get("document_key") or ""),
                        "anchorId": str(item.get("anchor_id") or ""),
                    }
                    for item in sorted_items
                ],
            }
        )

    return {
        "title": title,
        "chatId": chat_id,
        "startAt": start_at,
        "endAt": end_at,
        "peopleCount": people_count,
        "reportCount": len(records),
        "documentCount": document_count,
        "sourceDistribution": [
            {"key": key, "label": _source_label(key), "value": value, "color": _source_color(key)}
            for key, value in source_counter.items()
        ],
        "riskStats": dict(risk_counter),
        "dailyActivity": [{"day": day, "value": value} for day, value in sorted(daily_counter.items())],
        "events": sorted(event_rows, key=lambda item: item["time"]),
        "peopleCards": people_cards,
        "evidenceIndex": evidence_index,
        "summaryText": summary_text,
    }


def render_group_report_text(
    *,
    title: str,
    chat_id: str,
    start_at: str,
    end_at: str,
    people_count: int,
    summary_text: str,
    records: list[dict],
    buckets: list[dict],
) -> tuple[str, dict]:
    payload = build_group_report_payload(
        title=title,
        chat_id=chat_id,
        start_at=start_at,
        end_at=end_at,
        people_count=people_count,
        summary_text=summary_text,
        records=records,
        buckets=buckets,
    )
    lines = [
        f"# {title}",
        "",
        f"- chat_id: {chat_id}",
        f"- 时间窗口: {start_at} ~ {end_at}",
        f"- 人数: {people_count}",
        f"- 记录数: {payload['reportCount']}",
        f"- 文档数: {payload['documentCount']}",
        "",
        "## 群汇总结论",
        summary_text or "无",
        "",
        "## 按人摘要",
    ]
    for person in payload.get("peopleCards") or []:
        lines.extend(
            [
                f"### {person.get('reporterName') or 'unknown'}",
                f"- 汇报次数: {person.get('reportCount')}",
                f"- 文档数: {person.get('documentCount')}",
                f"- 最近更新时间: {person.get('latestTime')}",
                f"- 最近标题: {person.get('latestTitle')}",
                f"- 锚点: {person.get('latestAnchorId') or '无'}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n", payload


def write_group_report_text(
    *,
    title: str,
    chat_id: str,
    start_at: str,
    end_at: str,
    people_count: int,
    summary_text: str,
    records: list[dict],
    buckets: list[dict],
) -> tuple[str, Path, Path, dict]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_id = uuid4().hex
    text_body, payload = render_group_report_text(
        title=title,
        chat_id=chat_id,
        start_at=start_at,
        end_at=end_at,
        people_count=people_count,
        summary_text=summary_text,
        records=records,
        buckets=buckets,
    )
    text_path = REPORTS_DIR / f"{report_id}.md"
    json_path = REPORTS_DIR / f"{report_id}.json"
    text_path.write_text(text_body, encoding="utf-8")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_id, text_path, json_path, payload
