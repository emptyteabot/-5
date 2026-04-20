from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from entity_mapper import (
    build_entity_bundle,
    build_person_key,
    extract_people_signals,
    is_operational_person,
    is_system_reporter,
    normalize_reporter_name,
)

STATUS_KEYWORDS = {
    "delivered": ["已上线", "已发布", "已完成", "已交付", "已验收", "上线完成", "修复完成"],
    "testing": ["测试中", "提测", "待测试", "联调中", "联调", "待验收", "验收中", "转测试"],
    "review": ["待评审", "评审中", "待审核", "待确认"],
    "developing": ["开发中", "进行中", "推进中", "优化中", "调研中", "排期中"],
    "blocked": ["阻塞", "卡住", "依赖", "等待", "延期", "风险", "待处理", "缺资源", "未开始"],
}

SOURCE_LABELS = {
    "external_document_message": "文档全文",
    "document_message": "文档全文",
    "document_link_message": "文档链接",
    "external_text_message": "文字汇报",
    "text_message": "文字汇报",
    "post_message": "富文本",
    "interactive_message": "卡片",
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

TEAM_MARKERS = {
    "backend": ["后端", "backend", "接口", "服务", "网关", "数据库", "引擎", "java", "sparkdb"],
    "frontend": ["前端", "web", "app", "ui", "页面", "客户端", "h5"],
    "product": ["产品", "prd", "原型", "需求", "方案", "流程"],
    "test": ["测试", "用例", "提测", "验收", "qa", "联调", "压测"],
    "ops": ["运维", "监控", "事故", "报警", "节点", "巡检", "漏洞", "vpn"],
    "opsbiz": ["运营", "活动", "招商", "seo", "增长", "代理", "投放", "埋点"],
    "support": ["客服", "工单", "用户中心", "vip", "投诉"],
    "risk": ["风控", "审核", "kyc", "koic", "限额", "反洗钱"],
}

NOISE_LINES = {"Lark云文档", "外部", "搜索", "与我分享", "分享", "你可阅读", "主要参会人", "BYDFi", "会议纪要补录"}
ACTION_KEYWORDS = ("推进", "开发", "联调", "排查", "优化", "修复", "处理", "对接", "提测", "评审", "梳理", "调研", "跟进", "输出", "搭建")
RESULT_KEYWORDS = ("已上线", "已发布", "已完成", "已交付", "已验收", "修复", "恢复", "提升", "下降", "新增", "完成")
BLOCKER_KEYWORDS = ("阻塞", "卡住", "依赖", "等待", "延期", "风险", "缺", "未开始", "待处理", "待确认", "报错", "异常", "投诉", "404", "空白")
ETA_RE = re.compile(r"(今天|明天|后天|本周|下周|月底|月初|周[一二三四五六日天]|4月底前|最晚明天|\d{1,2}\s*月\s*\d{1,2}\s*日|\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?)")
OWNER_RE = re.compile(r"(?:owner|负责人)[:：]?\s*([A-Za-z\u4e00-\u9fff][A-Za-z0-9\u4e00-\u9fff /、，]{1,30})", re.IGNORECASE)
MENTION_RE = re.compile(r"@([A-Za-z\u4e00-\u9fff][A-Za-z0-9_\u4e00-\u9fff-]{0,20})")
LEADING_ACTOR_RE = re.compile(r"^(?:[-•]\s*)?([A-Za-z][A-Za-z0-9_-]{1,20}|[\u4e00-\u9fff]{2,10})[:：]\s*(.+)$")
RESPONSIBLE_RE = re.compile(r"([A-Za-z][A-Za-z0-9_-]{1,20}|[\u4e00-\u9fff]{2,10})\s*负责")
STRIP_BULLET_RE = re.compile(r"^[\s•◦·▪■□◆◇▶▷>*`#\-—–]+")
MARKDOWN_RE = re.compile(r"^\s*(?:[#>*`~-]+|\d+\.\s+|[-+*]\s+)")
BOILERPLATE_RE = re.compile(r"^(?:https?://|最近修改[:：]|\d+\s*条回复|Lark云文档|[A-Z]$)")
DIRECT_REPORT_SOURCE_TYPES = {"external_text_message", "text_message", "post_message", "interactive_message"}
NON_OPERATIONAL_NAMES = {"Kater", "Yohan"}
MEETING_NOTE_HINTS = ("会议", "纪要", "总结", "周会", "复盘", "参会")


def parse_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def source_label(value: str) -> str:
    return SOURCE_LABELS.get(value, value or "未知来源")


def source_color(value: str) -> str:
    return SOURCE_COLORS.get(value, "#5b6b8a")


def status_counts(text: str) -> dict[str, int]:
    return {key: sum(text.count(word) for word in words) for key, words in STATUS_KEYWORDS.items()}


def _plain(value: str) -> str:
    line = STRIP_BULLET_RE.sub("", (value or "").replace("\u200b", "").strip())
    return re.sub(r"\s+", " ", line).strip(" -:：")


def _clean_lines(text: str) -> list[str]:
    lines = []
    for raw in (text or "").splitlines():
        line = _plain(raw)
        if not line or line in NOISE_LINES or BOILERPLATE_RE.match(line):
            continue
        lines.append(line)
    return lines


def _sanitize_plain_text(text: str) -> str:
    rows = []
    for raw in (text or "").splitlines():
        line = _plain(MARKDOWN_RE.sub("", raw)).replace("`", "").replace("*", "").replace("#", "")
        if line:
            rows.append(line)
    return "\n".join(rows)


def _pick(items: list[str], limit: int = 3) -> list[str]:
    out: list[str] = []
    for item in items:
        value = _plain(item)
        if value and value not in out:
            out.append(value)
        if len(out) >= limit:
            break
    return out


def _weight(dt: datetime | None, end_dt: datetime | None) -> float:
    if dt is None or end_dt is None:
        return 1.0
    days = max(0.0, (end_dt - dt).total_seconds() / 86400)
    if days <= 1:
        return 3.0
    if days <= 3:
        return 2.6
    if days <= 7:
        return 2.2
    if days <= 14:
        return 1.8
    if days <= 30:
        return 1.4
    return 1.0


def _extract_owner_candidates(body: str) -> list[str]:
    owners: list[str] = []
    for item in OWNER_RE.findall(body):
        normalized = normalize_reporter_name(item)
        if is_operational_person(normalized) and normalized not in owners:
            owners.append(normalized)
    for signal in extract_people_signals(body):
        if signal.get("kind") not in {"owner", "speaker"}:
            continue
        name = normalize_reporter_name(str(signal.get("name") or ""))
        if is_operational_person(name) and name not in owners:
            owners.append(name)
    return owners[:6]


def _valid_actor_name(name: str) -> str | None:
    normalized = normalize_reporter_name(name)
    if normalized == "unknown" or not is_operational_person(normalized):
        return None
    if len(normalized) <= 1:
        return None
    return normalized


def _is_meeting_note(title: str, content: str) -> bool:
    """Return True if this record looks like a meeting summary / minutes."""
    combined = f"{title}\n{content[:200]}"
    return any(marker in combined for marker in MEETING_NOTE_HINTS)


def _extract_actor_entries(content: str, lines: list[str], reporter: str, source_type: str, title: str) -> list[dict[str, Any]]:
    actor_map: dict[str, dict[str, Any]] = {}
    # Only treat as meeting note if the title itself is a proper meeting title
    # (not a forwarder name like "Jenny" or "Miles").
    # This prevents Jenny's old docs from promoting bullet-point phrases as speakers.
    title_is_real = is_operational_person(title) and title.lower() not in {"unknown"}
    is_meeting = title_is_real and _is_meeting_note(title, content)

    def ensure_actor(name: str, *, source: str, line: str = "", line_items: list[str] | None = None) -> None:
        actor = _valid_actor_name(name)
        if not actor:
            return
        bucket = actor_map.setdefault(
            actor,
            {
                "name": actor,
                "key": build_person_key(actor),
                "sources": set(),
                "lines": [],
                "etaHints": [],
            },
        )
        bucket["sources"].add(source)
        candidates = line_items if line_items is not None else ([line] if line else [])
        for candidate in candidates:
            cleaned = _plain(candidate)
            if not cleaned or cleaned in bucket["lines"]:
                continue
            bucket["lines"].append(cleaned)
            bucket["etaHints"].extend(ETA_RE.findall(cleaned))

    # Collect attendee names first (for meeting notes we will promote them to actors)
    attendee_names: list[str] = []
    for signal in extract_people_signals(content):
        kind = str(signal.get("kind") or "mention")
        name = str(signal.get("name") or "")
        line = str(signal.get("line") or "")
        if kind == "attendee":
            attendee_names.append(name)
            # For meeting notes: @mention attendees under 主要参会人 are real participants
            # Only promote if they came from an @mention (Latin name or short Chinese name)
            # to avoid body bullet-point phrases being mistaken for people
            if is_meeting and "@" in line:
                ensure_actor(name, source="speaker", line=line)
        elif kind == "speaker":
            # Only treat as speaker if it's a Latin name (English names are unambiguous)
            # Chinese "speaker" matches are often bullet-point section headers, not people
            normalized = _valid_actor_name(name)
            if normalized and re.match(r'^[A-Za-z][A-Za-z0-9_-]{1,20}$', normalized):
                ensure_actor(name, source=kind, line=line)
            elif normalized and len(normalized) <= 4 and not any(kw in line for kw in ("任务", "业务", "需求", "方案", "问题", "规范", "管理", "工作", "进度", "指标", "完成", "优化", "配置", "处理")):
                ensure_actor(name, source=kind, line=line)
        else:
            ensure_actor(name, source=kind, line=line)

    if not actor_map and source_type in DIRECT_REPORT_SOURCE_TYPES:
        fallback_lines = _pick(
            [line for line in lines if any(word in line for word in ACTION_KEYWORDS + RESULT_KEYWORDS + BLOCKER_KEYWORDS)],
            4,
        )
        if not fallback_lines:
            fallback_lines = _pick(lines, 4)
        if fallback_lines:
            ensure_actor(reporter, source="reporter", line_items=fallback_lines)

    results = []
    for bucket in actor_map.values():
        person_text = "\n".join(bucket["lines"]) or title
        bucket["sources"] = sorted(bucket["sources"])
        bucket["etaHints"] = _pick(bucket["etaHints"], 4)
        bucket["actions"] = _pick([line for line in bucket["lines"] if any(word in line for word in ACTION_KEYWORDS)], 4)
        bucket["results"] = _pick([line for line in bucket["lines"] if any(word in line for word in RESULT_KEYWORDS)], 4)
        bucket["blockers"] = _pick([line for line in bucket["lines"] if any(word in line for word in BLOCKER_KEYWORDS)], 4)
        bucket["statusMix"] = status_counts(f"{title}\n{person_text}")
        bucket["isOwner"] = any(source in {"owner", "speaker", "reporter"} for source in bucket["sources"])
        results.append(bucket)
    return results


def resolve_reporter_name(record: dict[str, Any]) -> str:
    explicit = normalize_reporter_name(str(record.get("reporter_name") or ""))
    # Note-forwarders (Jenny / Lona / Miles) are just message carriers, not workers.
    # Treat them as unknown so speaker extraction reads the real actors from content.
    if explicit != "unknown" and is_operational_person(explicit):
        return explicit
    raw_message = record.get("raw_message_json")
    if isinstance(raw_message, dict):
        node = raw_message.get("node", {})
        for candidate in node.get("senderCandidates", []):
            name = normalize_reporter_name(str(candidate or ""))
            if name != "unknown" and is_operational_person(name):
                return name
    text = str(record.get("parsed_text") or "")
    first = next((line.strip() for line in text.splitlines() if line.strip()), "")
    first = normalize_reporter_name(first)
    if first != "unknown" and is_operational_person(first) and len(first) <= 24 and not first.startswith("http") and not any(keyword in first for keyword in ("会议总结", "会议纪要", "会议回顾", "智能纪要", "周报", "月报", "周会", "月会", "用例会")):
        return first
    sender_fallback = normalize_reporter_name(str(record.get("sender_id") or "").strip())
    if sender_fallback != "unknown" and is_operational_person(sender_fallback) and not re.fullmatch(r"[a-f0-9]{8,}", sender_fallback.lower()):
        return sender_fallback
    return "unknown"


def _preview_text(text: str, limit: int = 220) -> str:
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(compact) <= limit:
        return compact
    return compact[:limit].rstrip() + "..."


def _build_anchor_capsule(record: dict[str, Any], *, title: str, body: str, time_value: str) -> dict[str, Any]:
    message_id = str(record.get("message_id") or "").strip()
    document_key = str(record.get("document_key") or "").strip()
    source_type = str(record.get("source_type") or "").strip()
    source_url = str(record.get("source_url") or "").strip()
    chat_id = str(record.get("chat_id") or "").strip()
    if message_id:
        anchor_id = f"msg:{message_id}"
    elif document_key:
        anchor_id = f"doc:{document_key}"
    else:
        anchor_id = f"src:{hashlib.sha1(f'{title}|{time_value}|{source_type}'.encode('utf-8')).hexdigest()[:16]}"
    return {
        "anchorId": anchor_id,
        "messageId": message_id,
        "chatId": chat_id,
        "time": time_value,
        "reporterName": str(record.get("reporter_name") or "").strip(),
        "senderId": str(record.get("sender_id") or "").strip(),
        "sourceType": source_type,
        "messageType": str(record.get("message_type") or "").strip(),
        "title": title,
        "sourceUrl": source_url,
        "documentKey": document_key,
        "sourceContentHash": str(record.get("source_content_hash") or "").strip(),
        "status": str(record.get("status") or "").strip(),
        "preview": _preview_text(body),
        "hasRawMessage": bool(record.get("raw_message_json")),
        "hasRawEvent": bool(record.get("raw_event_json")),
    }


def _fact(record: dict[str, Any], end_dt: datetime | None) -> dict[str, Any]:
    text = str(record.get("parsed_text") or "")
    lines = _clean_lines(text)
    title = _plain(str(record.get("source_title") or ""))
    if not title or not is_operational_person(title) or title.lower() == "unknown" or title.lower().startswith("reply "):
        title = lines[0] if lines else "未命名记录"
    reporter = resolve_reporter_name(record)
    entity = build_entity_bundle(
        reporter_name=reporter,
        sender_id=str(record.get("sender_id") or ""),
        title=title,
        content=text,
    )
    time_value = str(record.get("message_timestamp") or record.get("created_at") or "")
    dt = parse_time(time_value)
    body = "\n".join(lines[:80])
    source_type = str(record.get("source_type") or "")
    anchor = _build_anchor_capsule(record, title=title, body=body or text, time_value=time_value)
    teams = [
        team
        for team, words in TEAM_MARKERS.items()
        if any(word.lower() in f"{title}\n{body}".lower() for word in words)
    ]
    actions = _pick([line for line in lines if any(word in line for word in ACTION_KEYWORDS)], 4)
    results = _pick([line for line in lines if any(word in line for word in RESULT_KEYWORDS)], 4)
    blockers = _pick([line for line in lines if any(word in line for word in BLOCKER_KEYWORDS)], 4)
    etas = _pick(ETA_RE.findall(body), 4)
    owners = _extract_owner_candidates(body)
    actor_entries = _extract_actor_entries(text, lines, reporter, source_type, title)
    signature_seed = str(record.get("document_key") or "") or f"{title}|{' '.join(lines[:8])}"
    return {
        "signature": hashlib.sha1(signature_seed.encode("utf-8")).hexdigest(),
        "time": time_value,
        "dt": dt,
        "title": title,
        "personName": reporter,
        "personKey": entity["person_key"],
        "projectKey": entity["project_key"],
        "projectCandidates": entity["project_candidates"],
        "progressState": entity["progress_state"],
        "sourceType": source_type,
        "documentKey": str(record.get("document_key") or ""),
        "weight": _weight(dt, end_dt),
        "statusMix": status_counts(f"{title}\n{body}"),
        "teams": teams,
        "owners": owners,
        "actors": actor_entries,
        "etas": etas,
        "actions": actions,
        "results": results,
        "blockers": blockers,
        "anchor": anchor,
        "record": record,
    }


def _dedupe_facts(records: list[dict[str, Any]], end_dt: datetime | None) -> list[dict[str, Any]]:
    facts_raw = [_fact(record, end_dt) for record in records]
    deduped = {}
    for item in sorted(
        facts_raw,
        key=lambda row: (
            len(row["results"]) + len(row["blockers"]) + len(row["actions"]),
            row["time"],
        ),
    ):
        deduped[item["signature"]] = item
    return list(deduped.values())


def _build_progress_tracks_from_facts(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in facts:
        grouped[(item["personKey"], item["projectKey"])].append(
            {
                "personKey": item["personKey"],
                "personName": item["personName"],
                "projectKey": item["projectKey"],
                "progressState": item["progressState"],
                "title": item["title"],
                "time": item["time"],
                "etaHints": item["etas"],
                "anchorId": str((item.get("anchor") or {}).get("anchorId") or ""),
            }
        )
    return [
        {
            "personKey": values[0]["personKey"],
            "personName": values[0]["personName"],
            "projectKey": values[0]["projectKey"],
            "events": sorted(values, key=lambda row: row["time"]),
        }
        for values in grouped.values()
    ]


def build_progress_tracks(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _build_progress_tracks_from_facts(_dedupe_facts(records, None))


def detect_pseudo_progress_sequences(records: list[dict[str, Any]], min_repeats: int = 3) -> list[dict[str, Any]]:
    findings = []
    for track in build_progress_tracks(records):
        seq: list[dict[str, Any]] = []
        for event in track["events"]:
            if event["progressState"] == "delivered":
                seq = []
                continue
            if event["progressState"] in {"testing", "review", "developing"}:
                seq.append(event)
            elif len(seq) >= min_repeats:
                findings.append(
                    {
                        "personKey": track["personKey"],
                        "personName": track["personName"],
                        "projectKey": track["projectKey"],
                        "repeatCount": len(seq),
                        "states": [row["progressState"] for row in seq],
                        "startAt": seq[0]["time"],
                        "endAt": seq[-1]["time"],
                        "latestTitle": seq[-1]["title"],
                        "latestEtaHints": seq[-1].get("etaHints") or [],
                        "anchorIds": [row.get("anchorId") or "" for row in seq if row.get("anchorId")],
                        "signal": "repeated_pseudo_progress",
                    }
                )
                seq = []
            else:
                seq = []
        if len(seq) >= min_repeats:
            findings.append(
                {
                    "personKey": track["personKey"],
                    "personName": track["personName"],
                    "projectKey": track["projectKey"],
                    "repeatCount": len(seq),
                    "states": [row["progressState"] for row in seq],
                    "startAt": seq[0]["time"],
                    "endAt": seq[-1]["time"],
                    "latestTitle": seq[-1]["title"],
                    "latestEtaHints": seq[-1].get("etaHints") or [],
                    "anchorIds": [row.get("anchorId") or "" for row in seq if row.get("anchorId")],
                    "signal": "repeated_pseudo_progress",
                }
            )
    return sorted(findings, key=lambda row: (row["repeatCount"], row["endAt"]), reverse=True)


def detect_repeated_non_delivery(records: list[dict[str, Any]], min_mentions: int = 3) -> list[dict[str, Any]]:
    findings = []
    for track in build_progress_tracks(records):
        events = track["events"]
        if len(events) < min_mentions or any(event["progressState"] == "delivered" for event in events):
            continue
        findings.append(
            {
                "personKey": track["personKey"],
                "personName": track["personName"],
                "projectKey": track["projectKey"],
                "repeatCount": len(events),
                "states": [row["progressState"] for row in events],
                "startAt": events[0]["time"],
                "endAt": events[-1]["time"],
                "latestTitle": events[-1]["title"],
                "latestEtaHints": events[-1].get("etaHints") or [],
                "anchorIds": [row.get("anchorId") or "" for row in events if row.get("anchorId")],
                "signal": "repeated_non_delivery",
            }
        )
    return sorted(findings, key=lambda row: (row["repeatCount"], row["endAt"]), reverse=True)


def build_execution_snapshot(
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
    del buckets
    end_dt = parse_time(end_at)
    facts = _dedupe_facts(records, end_dt)
    evidence_index = {
        str((item.get("anchor") or {}).get("anchorId") or ""): item.get("anchor")
        for item in facts
        if str((item.get("anchor") or {}).get("anchorId") or "")
    }

    source_counter: Counter[str] = Counter()
    risk_counter: Counter[str] = Counter()
    daily_counter: Counter[str] = Counter()
    event_rows = []
    document_count = 0

    for item in facts:
        dt = item["dt"]
        daily_counter[dt.strftime("%m-%d") if dt else "unknown"] += 1
        source_type = item["sourceType"]
        source_counter[source_type] += 1
        if item["documentKey"]:
            document_count += 1
        for key, value in item["statusMix"].items():
            risk_counter[key] += value
        event_rows.append(
            {
                "time": item["time"],
                "reporter": item["personName"],
                "sourceType": source_type,
                "sourceLabel": source_label(source_type),
                "sourceColor": source_color(source_type),
                "title": item["title"],
                "documentKey": item["documentKey"],
                "anchorId": str((item.get("anchor") or {}).get("anchorId") or ""),
                "projectKey": item["projectKey"],
                "teams": item["teams"],
                "owners": item["owners"],
                "etas": item["etas"],
                "status": str(item["record"].get("status") or ""),
                "error": str(item["record"].get("error_message") or ""),
            }
        )

    project_map: dict[str, dict[str, Any]] = {}
    for item in facts:
        project_key = (item["projectCandidates"][0] if item["projectCandidates"] else item["projectKey"]).strip() or "unmapped"
        rollup = project_map.setdefault(
            project_key,
            {
                "projectKey": project_key,
                "reportCount": 0,
                "latestTime": "",
                "latestTitle": "",
                "delivered": 0,
                "testing": 0,
                "review": 0,
                "developing": 0,
                "blocked": 0,
                "teams": set(),
                "owners": set(),
                "etas": [],
                "evidence": [],
                "evidenceAnchorIds": [],
                "deliveryScore": 0.0,
                "riskScore": 0.0,
            },
        )
        rollup["reportCount"] += 1
        if item["time"] >= rollup["latestTime"]:
            rollup["latestTime"] = item["time"]
            rollup["latestTitle"] = item["title"]
        for key, value in item["statusMix"].items():
            rollup[key] += value
        rollup["teams"].update(item["teams"])
        rollup["owners"].update(item["owners"])
        rollup["etas"].extend(item["etas"])
        rollup["evidence"].extend(item["results"] + item["blockers"] + item["actions"])
        anchor_id = str((item.get("anchor") or {}).get("anchorId") or "")
        if anchor_id and anchor_id not in rollup["evidenceAnchorIds"]:
            rollup["evidenceAnchorIds"].append(anchor_id)
        rollup["deliveryScore"] += item["statusMix"].get("delivered", 0) * item["weight"] * 3 - item["statusMix"].get("blocked", 0) * item["weight"]
        rollup["riskScore"] += item["statusMix"].get("blocked", 0) * item["weight"] * 4
        rollup["riskScore"] += (
            item["statusMix"].get("testing", 0)
            + item["statusMix"].get("review", 0)
            + item["statusMix"].get("developing", 0)
        ) * item["weight"] * 1.4

    project_rollups = []
    for rollup in project_map.values():
        rollup["teams"] = sorted(rollup["teams"])
        rollup["owners"] = sorted(rollup["owners"])
        rollup["etas"] = _pick(rollup["etas"], 3)
        rollup["evidence"] = _pick(rollup["evidence"], 4)
        rollup["riskScore"] += max(0, len(rollup["teams"]) - 1) * 1.2 + (1.5 if not rollup["owners"] else 0) + (1.0 if not rollup["etas"] else 0)
        project_rollups.append(rollup)
    project_rollups.sort(key=lambda row: (row["riskScore"], row["latestTime"]), reverse=True)

    fractures = []
    for item in project_rollups:
        if len(item["teams"]) < 2:
            continue
        if item["blocked"] == 0 and item["delivered"] > 0 and item["riskScore"] < 6:
            continue
        eta_text = "缺 ETA。" if not item["etas"] else f"ETA 线索 {'、'.join(item['etas'])}。"
        fractures.append(
            {
                "projectKey": item["projectKey"],
                "latestTitle": item["latestTitle"],
                "latestTime": item["latestTime"],
                "teams": item["teams"],
                "riskLevel": "high" if item["riskScore"] >= 8 or item["blocked"] > 0 else "medium",
                "score": round(item["riskScore"], 2),
                "ownerGap": not bool(item["owners"]),
                "etaGap": not bool(item["etas"]),
                "evidence": item["evidence"],
                "evidenceAnchorIds": item["evidenceAnchorIds"][:4],
                "summary": (
                    f"{item['projectKey']} 同时牵动 {' / '.join(item['teams'])}，最近记录仍以 "
                    f"{(item['evidence'][0] if item['evidence'] else '待处理事项')} 为主，"
                    f"{'缺 owner，' if not item['owners'] else ''}"
                    f"{eta_text}"
                ),
            }
        )
    fractures = sorted(fractures, key=lambda row: (row["score"], row["latestTime"]), reverse=True)[:8]

    person_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in facts:
        actor_entries = item.get("actors") or []
        for actor in actor_entries:
            actor_name = actor.get("name") or ""
            actor_key = actor.get("key") or build_person_key(actor_name)
            if not actor_name or not actor_key or not is_operational_person(actor_name):
                continue
            cloned = dict(item)
            cloned["personName"] = actor_name
            cloned["personKey"] = actor_key
            cloned["actorLines"] = actor.get("lines") or []
            cloned["actorEtaHints"] = actor.get("etaHints") or []
            cloned["actorActions"] = actor.get("actions") or []
            cloned["actorResults"] = actor.get("results") or []
            cloned["actorBlockers"] = actor.get("blockers") or []
            cloned["actorStatusMix"] = actor.get("statusMix") or {}
            cloned["actorSources"] = actor.get("sources") or []
            cloned["actorIsOwner"] = bool(actor.get("isOwner"))
            person_map[cloned["personKey"]].append(cloned)
    people_cards = []
    for values in person_map.values():
        values = sorted(values, key=lambda row: row["time"])
        latest = values[-1]
        name = next((row["personName"] for row in values if row["personName"] != "unknown"), "unknown")
        status_mix = Counter()
        actor_sources = Counter()
        for row in values:
            for key, value in (row.get("actorStatusMix") or row["statusMix"]).items():
                status_mix[key] += value
            for source in row.get("actorSources") or []:
                actor_sources[source] += 1
        reported = _pick(
            [line for row in reversed(values) for line in (row.get("actorLines") or [])]
            + [row["projectKey"] for row in reversed(values) if row["projectKey"] not in {"unmapped", "linked-doc-sequence"}],
            4,
        )
        owned = _pick(
            [row["projectKey"] for row in values if row.get("actorIsOwner") and row["projectKey"] not in {"unmapped", "linked-doc-sequence"}],
            4,
        )
        doing = _pick(
            [
                line
                for row in reversed(values[-6:])
                for line in ((row.get("actorResults") or []) + (row.get("actorActions") or []) + (row.get("actorBlockers") or []))
            ],
            4,
        )
        etas = _pick([eta for row in reversed(values[-6:]) for eta in (row.get("actorEtaHints") or [])], 4)
        if not (reported or owned or doing or etas or sum(status_mix.values())):
            continue
        document_count = sum(1 for row in values if row["documentKey"])
        explicit_signal_count = actor_sources["owner"] + actor_sources["speaker"] + actor_sources["reporter"]
        evidence_strength = "高" if document_count >= 2 or explicit_signal_count >= 3 else ("中" if document_count or explicit_signal_count else "低")
        rail_text = (
            f"{name} 最近被明确绑定到 {('、'.join(reported) or '零散事项')}。"
            f"{'名下事项有 ' + '、'.join(owned) + '。' if owned else '暂时没有拿到明确 owner 口径。'}"
            f"{'当前动作：' + '；'.join(doing) + '。' if doing else '当前动作仍然模糊。'}"
            f"{'ETA：' + '、'.join(etas) + '。' if etas else '没有抽到明确 ETA。'}"
            f"证据强度 {evidence_strength}。"
        )
        people_cards.append(
            {
                "reporterId": latest["personKey"],
                "reporterName": name,
                "reportCount": len(values),
                "uniqueReportCount": len({row["signature"] for row in values}),
                "documentCount": document_count,
                "latestTime": latest["time"],
                "latestTitle": latest["title"],
                "sourceMix": [
                    {"label": source_label(key), "value": value, "color": source_color(key)}
                    for key, value in Counter(row["sourceType"] for row in values).items()
                ],
                "statusMix": dict(status_mix),
                "reportedWhat": reported,
                "ownedWhat": owned,
                "doingNow": doing,
                "etaHints": etas,
                "ownershipStatus": "explicit" if (owned or explicit_signal_count) else "weak",
                "evidenceStrength": evidence_strength,
                "railText": rail_text,
                "latestAnchorId": str((latest.get("anchor") or {}).get("anchorId") or ""),
                "impactScore": sum(row["weight"] for row in values) + status_mix.get("blocked", 0) * 1.5 + status_mix.get("delivered", 0) + explicit_signal_count * 0.4,
                "events": [
                    {
                        "time": row["time"],
                        "title": row["title"],
                        "sourceLabel": source_label(row["sourceType"]),
                        "sourceColor": source_color(row["sourceType"]),
                        "documentKey": row["documentKey"],
                        "anchorId": str((row.get("anchor") or {}).get("anchorId") or ""),
                        "projectKey": row["projectKey"],
                        "progressState": row["progressState"],
                        "etaHints": row.get("actorEtaHints") or row["etas"],
                    }
                    for row in values
                ],
            }
        )
    people_cards.sort(key=lambda row: (row["impactScore"], row["latestTime"]), reverse=True)

    meeting_speaker_scores: dict[str, float] = defaultdict(float)
    meeting_speaker_counts: dict[str, int] = defaultdict(int)
    for row in facts:
        row_title = str(row.get('title') or '')
        # Skip records whose title is a forwarder name or non-operational
        # (e.g. Jenny's old docs with title="Jenny" or "3.17 会议总结" sent by Miles)
        if not is_operational_person(row_title):
            continue
        text_blob = f"{row_title}\n{' '.join(row.get('results') or [])}\n{' '.join(row.get('actions') or [])}\n{' '.join(row.get('blockers') or [])}"
        if not any(marker in text_blob for marker in MEETING_NOTE_HINTS):
            continue
        for actor in row.get("actors") or []:
            actor_name = str(actor.get("name") or "")
            if not is_operational_person(actor_name):
                continue
            actor_key = build_person_key(actor_name)
            actor_sources = set(actor.get("sources") or [])
            if "speaker" not in actor_sources and "owner" not in actor_sources:
                continue
            lines = actor.get("lines") or []
            if not lines:
                continue
            speech_weight = row["weight"] + (0.6 if "owner" in actor_sources else 0.0)
            meeting_speaker_scores[actor_key] += speech_weight
            meeting_speaker_counts[actor_key] += 1

    for person in people_cards:
        person_key = build_person_key(person.get("reporterName") or "")
        person["meetingSpeakerScore"] = round(meeting_speaker_scores.get(person_key, 0.0), 3)
        person["meetingSpeakerCount"] = meeting_speaker_counts.get(person_key, 0)

    pseudo_tracks = detect_pseudo_progress_sequences(records)
    repeated_non_delivery = detect_repeated_non_delivery(records)
    red_projects = sorted(project_rollups, key=lambda row: (row["deliveryScore"], row["latestTime"]), reverse=True)[:5]
    black_projects = sorted(project_rollups, key=lambda row: (row["riskScore"], row["latestTime"]), reverse=True)[:5]

    ranking_pool = [
        row
        for row in people_cards
        if is_operational_person(str(row.get("reporterName") or ""))
    ]
    meeting_ranking_pool = [
        row
        for row in ranking_pool
        if int(row.get("meetingSpeakerCount") or 0) > 0
    ]
    ranking_base = meeting_ranking_pool

    black_people = sorted(
        ranking_base,
        key=lambda row: (
            (row["statusMix"].get("blocked", 0) * 3.2)
            + (row["statusMix"].get("testing", 0) * 1.6)
            + (row["statusMix"].get("review", 0) * 1.4)
            + (2 if row["ownershipStatus"] == "weak" and not row["etaHints"] else 0)
            + row.get("meetingSpeakerScore", 0.0) * 0.6,
            row.get("meetingSpeakerCount", 0),
            row["latestTime"],
        ),
        reverse=True,
    )[:5]

    black_ids = {row["reporterId"] for row in black_people}
    red_people = sorted(
        [row for row in ranking_base if row["reporterId"] not in black_ids],
        key=lambda row: (
            row.get("meetingSpeakerScore", 0.0) * 2.6
            + row.get("meetingSpeakerCount", 0) * 1.2
            + row["statusMix"].get("delivered", 0) * 2.4
            + row["documentCount"] * 0.7
            - row["statusMix"].get("blocked", 0) * 2.8,
            row["latestTime"],
        ),
        reverse=True,
    )[:5]

    latest_signals = _pick(
        [
            f"{row['title']}里明确提到{(row['results'] or row['blockers'] or row['actions'])[0]}"
            for row in sorted(facts, key=lambda item: ((item["dt"] or datetime.min), item["weight"]), reverse=True)
            if row["results"] or row["blockers"] or row["actions"]
        ],
        4,
    )
    deliveries = _pick(
        [
            f"{row['projectKey']}目前最像真实交付，证据是{(row['evidence'][0] if row['evidence'] else '缺少直接交付描写')}"
            for row in sorted(project_rollups, key=lambda item: (item["deliveryScore"], item["latestTime"]), reverse=True)
            if row["deliveryScore"] > 0 and row["delivered"] > 0
        ],
        4,
    )
    gaps = _pick(
        [
            f"{row.get('personName', '未知人员')}在{row.get('projectKey', '未映射项目')}上连续{row.get('repeatCount', 0)}次停留在测试或开发态，从{row.get('startAt', '')}拖到{row.get('endAt', '')}，中间没有交付信号"
            for row in pseudo_tracks[:4]
        ]
        + [
            f"{row.get('personName', '未知人员')}多次提到{row.get('projectKey', '未映射项目')}，但从{row.get('startAt', '')}到{row.get('endAt', '')}始终没有出现已上线或已交付"
            for row in repeated_non_delivery[:4]
        ],
        4,
    )
    fracture_lines = _pick([f"{row['projectKey']}涉及{'、'.join(row['teams'])}，{row['summary']}" for row in fractures[:3]], 3)
    person_lines = _pick([row["railText"] for row in people_cards[:4]], 4)
    model_summary = _sanitize_plain_text(summary_text)
    latest_bias_count = sum(1 for row in facts if row["weight"] >= 1.8)

    summary_lines = [
        (
            f"执行判断：本窗口覆盖 {start_at} 到 {end_at} 的 {len(facts)} 条去重后事实记录，其中 {document_count} 条带全文证据。"
            f"最近会议权重被刻意放大，近 14 天内的 {latest_bias_count} 条材料主导这次裁决。当前真正的问题不是没人汇报，"
            f"而是很多汇报还没有落到 owner、ETA 和验收结果。"
        ),
        f"最新会议信号：{'；'.join(latest_signals) if latest_signals else '近会议信息仍以状态同步为主，缺少硬结果。'}",
        f"真实交付：{'；'.join(deliveries) if deliveries else '当前没有足够多可以直接盖章为真实交付的事项，证据不足时不强行乐观。'}",
        f"伪进展与缺口：{'；'.join(gaps) if gaps else '没有抓到特别长的空转序列，但 owner、ETA 和验收口径仍然偏缺。'}",
        f"跨团队断裂：{'；'.join(fracture_lines) if fracture_lines else '当前没有高置信跨部门断裂条目，但这更可能意味着记录不完整，而不是协同已经顺畅。'}",
        f"个人责任轨道：{'；'.join(person_lines) if person_lines else '当前人物轨道证据很薄，很多人只有转发或会议痕迹，没有独立责任描述。'}",
    ]
    if model_summary:
        summary_lines.append(f"模型补充视角：{model_summary.splitlines()[0]}")

    top_track = repeated_non_delivery[0] if repeated_non_delivery else (pseudo_tracks[0] if pseudo_tracks else None)
    if top_track:
        top_verdict_title = f"{top_track['projectKey']} 已连续 {top_track['repeatCount']} 次停留在非交付状态"
        top_verdict_desc = (
            f"{top_track['personName']} 从 {top_track['startAt']} 到 {top_track['endAt']} 连续汇报，"
            f"但没有出现 delivered 信号，当前更像状态表演而非真实推进。"
        )
        top_verdict_anchor_ids = list(top_track.get("anchorIds") or [])[:4]
    elif fractures:
        top_verdict_title = f"{fractures[0]['projectKey']} 存在跨部门断裂"
        top_verdict_desc = fractures[0]["summary"]
        top_verdict_anchor_ids = list(fractures[0].get("evidenceAnchorIds") or [])[:4]
    else:
        top_verdict_title = "当前没有足够高置信的跨时间非交付链路"
        top_verdict_desc = "这意味着目前系统没有证据支持重判，而不是没有问题。请结合下方红黑榜和跨部门断裂区域一起看。"
        top_verdict_anchor_ids = []

    return {
        "title": title,
        "chatId": chat_id,
        "startAt": start_at,
        "endAt": end_at,
        "peopleCount": people_count,
        "reportCount": len(facts),
        "rawRecordCount": len(records),
        "documentCount": document_count,
        "sourceDistribution": [{"key": key, "label": source_label(key), "value": value, "color": source_color(key)} for key, value in source_counter.items()],
        "riskStats": dict(risk_counter),
        "dailyActivity": [{"day": day, "value": value} for day, value in sorted(daily_counter.items())],
        "events": sorted(event_rows, key=lambda row: row["time"]),
        "peopleCards": people_cards,
        "personRails": [
            {
                "reporterId": row["reporterId"],
                "reporterName": row["reporterName"],
                "reportCount": row["reportCount"],
                "uniqueReportCount": row["uniqueReportCount"],
                "documentCount": row["documentCount"],
                "latestTime": row["latestTime"],
                "latestTitle": row["latestTitle"],
                "statusMix": row["statusMix"],
                "reportedWhat": row["reportedWhat"],
                "ownedWhat": row["ownedWhat"],
                "doingNow": row["doingNow"],
                "etaHints": row["etaHints"],
                "ownershipStatus": row["ownershipStatus"],
                "evidenceStrength": row["evidenceStrength"],
                "meetingSpeakerScore": row.get("meetingSpeakerScore", 0),
                "meetingSpeakerCount": row.get("meetingSpeakerCount", 0),
                "railText": row["railText"],
            }
            for row in people_cards
        ],
        "projectRollups": project_rollups,
        "pseudoProgressTracks": pseudo_tracks,
        "repeatedNonDeliveryTracks": repeated_non_delivery,
        "crossTeamFractures": fractures,
        "redListCandidates": {"projects": red_projects, "people": red_people},
        "blackListCandidates": {"projects": black_projects, "people": black_people},
        "topVerdictTitle": top_verdict_title,
        "topVerdictDesc": top_verdict_desc,
        "topVerdictAnchorIds": top_verdict_anchor_ids,
        "evidenceIndex": evidence_index,
        "summaryText": "\n".join(summary_lines),
        "modelSummaryText": model_summary,
    }
