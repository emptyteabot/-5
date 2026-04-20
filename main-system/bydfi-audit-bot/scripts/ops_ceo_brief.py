from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "scheduled"
REPORTS_DIR = ROOT / "data" / "reports"
VERIFY_SCRIPT = Path.home() / ".codex" / "skills" / "bydfi-ceo-brief" / "scripts" / "verify_ceo_brief.py"
RENDER_SCRIPT = ROOT / "generate_ceo_brief_pdf.py"

LEADERSHIP_NAMES = {"Kater", "Miles"}
PROBLEM_SECTION_PREFIXES = [
    "头号问题：",
    "第二问题：",
    "第三问题：",
    "最清晰交付线：",
    "次级但重要的支撑线：",
]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_latest_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def parse_iso(raw: str | None) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone()


def date_key(raw: str | None) -> str:
    dt = parse_iso(raw)
    if dt is None:
        dt = datetime.now().astimezone()
    return dt.strftime("%Y%m%d")


def cn_date(raw: str | None) -> str:
    dt = parse_iso(raw)
    if dt is None:
        dt = datetime.now().astimezone()
    return f"{dt.year}年{dt.month}月{dt.day}日"


def short_time(raw: str | None) -> str:
    dt = parse_iso(raw)
    if dt is None:
        return "-"
    return dt.strftime("%m月%d日 %H:%M")


def load_payload(period: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if payload is not None:
        return payload
    target = OUTPUT_DIR / f"{period}_ops_digest_latest.json"
    if not target.exists():
        raise FileNotFoundError(f"Digest payload not found: {target}")
    return read_json(target)


def latest_manual_ceo_md() -> Path | None:
    files = sorted(REPORTS_DIR.glob("ceo_brief_*.md"), key=lambda item: item.stat().st_mtime, reverse=True)
    return files[0] if files else None


def latest_manual_ceo_text() -> tuple[Path | None, str]:
    path = latest_manual_ceo_md()
    if path and path.exists():
        return path, path.read_text(encoding="utf-8")
    return None, ""


def manual_matches_report_date(path: Path | None, report_date_key: str) -> bool:
    if not path:
        return False
    match = re.search(r"(\d{8})", path.stem)
    return bool(match and match.group(1) == report_date_key)


def extract_section(text: str, heading: str) -> str:
    pattern = rf"(?ms)^## {re.escape(heading)}\n(.*?)(?=^## |\Z)"
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


def extract_section_by_prefix(text: str, prefix: str) -> tuple[str, str] | None:
    pattern = rf"(?ms)^## ({re.escape(prefix)}[^\n]*)\n(.*?)(?=^## |\Z)"
    match = re.search(pattern, text)
    if not match:
        return None
    return match.group(1).strip(), match.group(2).strip()


def clean_group_title(title: str) -> str:
    text = str(title or "").strip()
    if not text:
        return "未命名业务线"
    replacements = {
        "BYDFi·MoonX业务大群": "MoonX 业务线",
        "BYDFI·MoonX业务大群": "MoonX 业务线",
        "BYDFi路MoonX业务大群": "MoonX 业务线",
        "产研周报发送群": "产研周报闭环",
        "中心研发任务": "中心研发任务线",
        "永续研发任务": "永续研发任务线",
        "三部门效能优化需求": "三部门效能线",
        "AI翻译交流": "翻译协同线",
        "血战到底": "经营协调会",
    }
    return replacements.get(text, text)


def action_hint(title: str) -> str:
    label = clean_group_title(title)
    if "MoonX" in label:
        return "冻结尾项之外的新插单，只盯验收口径、上线和销号结果。"
    if "周报" in label:
        return "把周报彻底改成闭环表，只保留负责人、截止时间、验收人、未收口原因。"
    if "研发任务" in label:
        return "新需求必须先并入统一版本表，再排优先级，不允许旧任务未收口继续插队。"
    if "效能" in label:
        return "先锁 owner、ETA、验收口径，再谈资源扩张和方案优化。"
    if "翻译" in label:
        return "把术语审批权和法务签核时限收敛到固定责任人，减少多人来回确认。"
    if "经营协调会" in label:
        return "会议纪要发出时必须同步待办进展、结果缺口和新增待办。"
    return "先把 owner、ETA、验收人写实，再推进下一轮动作。"


def select_problem_groups(payload: dict[str, Any]) -> list[dict[str, Any]]:
    groups = payload.get("delay_risk_groups") or payload.get("focus_groups") or []
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    preferred_keywords = ("MoonX", "周报", "研发任务", "效能", "翻译", "经营协调会")
    for keyword in preferred_keywords:
        for item in groups:
            title = clean_group_title(str(item.get("group_title", "")))
            if keyword in title and title not in seen:
                selected.append(item)
                seen.add(title)
                break
    for item in groups:
        title = clean_group_title(str(item.get("group_title", "")))
        if title in seen:
            continue
        selected.append(item)
        seen.add(title)
        if len(selected) >= 5:
            break
    return selected[:5]


def progress_line(item: dict[str, Any]) -> str:
    risk = item.get("risk_stats", {}) or {}
    latest = short_time(item.get("latest_time"))
    delivered = int(risk.get("delivered", 0))
    developing = int(risk.get("developing", 0))
    testing = int(risk.get("testing", 0))
    recent_count = int(item.get("recent_event_count", 0))
    return f"最近更新 {latest}；已交付 {delivered}，在研/测试 {developing + testing}，近两天新增信号 {recent_count} 条。"


def results_gap(item: dict[str, Any]) -> str:
    risk = item.get("risk_stats", {}) or {}
    blocked = int(risk.get("blocked", 0))
    delivered = int(risk.get("delivered", 0))
    developing = int(risk.get("developing", 0))
    testing = int(risk.get("testing", 0))
    if blocked >= 12:
        return f"阻塞 {blocked} 项，管理注意力被持续占用，收口慢于问题暴露速度。"
    if developing + testing > max(3, delivered):
        return f"在研/测试 {developing + testing} 项高于已交付 {delivered} 项，验收闭环偏弱。"
    if delivered == 0:
        return "还没有形成足够的结果回收，当前更像输入堆积而不是结果输出。"
    return "已有结果回收，但统一验收口径仍不够硬。"


def normalize_name(name: str) -> str:
    text = str(name or "").strip()
    if re.search(r"\d{1,2}:\d{2}$", text):
        text = re.sub(r"\d{1,2}:\d{2}$", "", text).strip()
    return text


def load_report_details(payload: dict[str, Any]) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for file_name in payload.get("report_files_used", []):
        path = REPORTS_DIR / str(file_name)
        if not path.exists():
            continue
        data = read_json(path)
        reports.append(data)
    return reports


def aggregate_people(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    people: dict[str, dict[str, Any]] = {}
    for report in reports:
        group_title = clean_group_title(str(report.get("title") or report.get("chatId") or ""))
        for card in report.get("peopleCards", []):
            name = normalize_name(str(card.get("reporterName") or ""))
            if not name:
                continue
            item = people.setdefault(
                name,
                {
                    "name": name,
                    "report_count": 0,
                    "document_count": 0,
                    "delivered": 0,
                    "testing": 0,
                    "developing": 0,
                    "review": 0,
                    "blocked": 0,
                    "groups": set(),
                    "latest_time": "",
                },
            )
            item["report_count"] += int(card.get("reportCount") or 0)
            item["document_count"] += int(card.get("documentCount") or 0)
            status_mix = card.get("statusMix") or {}
            for key in ("delivered", "testing", "developing", "review", "blocked"):
                item[key] += int(status_mix.get(key) or 0)
            item["groups"].add(group_title)
            latest_time = str(card.get("latestTime") or "")
            if latest_time and latest_time > item["latest_time"]:
                item["latest_time"] = latest_time

    rows: list[dict[str, Any]] = []
    for item in people.values():
        item["groups"] = sorted(item["groups"])
        item["group_count"] = len(item["groups"])
        item["workload"] = item["delivered"] + item["testing"] + item["developing"] + item["blocked"] + item["document_count"]
        item["positive_score"] = round(
            item["delivered"] * 3.0
            + item["document_count"] * 2.2
            + item["group_count"] * 1.5
            + min(item["report_count"], 20) * 0.2
            - item["blocked"] * 1.6
            - item["developing"] * 0.5
            - item["testing"] * 0.35,
            1,
        )
        item["risk_score"] = round(
            item["blocked"] * 2.8
            + max(0, item["developing"] + item["testing"] - item["delivered"]) * 1.2
            + item["group_count"] * 0.4
            - item["delivered"] * 0.2,
            1,
        )
        rows.append(item)
    return rows


def company_metrics(payload: dict[str, Any], reports: list[dict[str, Any]], people_rows: list[dict[str, Any]]) -> dict[str, Any]:
    delivered = testing = developing = blocked = 0
    people_total: set[str] = set()
    for report in reports:
        stats = report.get("riskStats") or {}
        delivered += int(stats.get("delivered") or 0)
        testing += int(stats.get("testing") or 0)
        developing += int(stats.get("developing") or 0)
        blocked += int(stats.get("blocked") or 0)
        for card in report.get("peopleCards", []):
            name = normalize_name(str(card.get("reporterName") or ""))
            if name:
                people_total.add(name)
    total_work = delivered + testing + developing + blocked
    result_conversion = round((delivered / total_work) * 100, 1) if total_work else 0.0
    closure_pressure = round((testing + developing + blocked) / max(delivered, 1), 2)
    red_lines = sum(1 for item in payload.get("delay_risk_groups", []) if float(item.get("delay_score", 0)) >= 150)
    yellow_lines = sum(1 for item in payload.get("delay_risk_groups", []) if 50 <= float(item.get("delay_score", 0)) < 150)
    risk_people = sum(1 for row in people_rows if row["risk_score"] >= 30 and row["workload"] >= 5 and row["name"] not in LEADERSHIP_NAMES)
    attention_debt = blocked + red_lines * 8 + yellow_lines * 4 + risk_people * 3
    active_people = sum(1 for row in people_rows if row["workload"] >= 5 and row["name"] not in LEADERSHIP_NAMES)
    per_capita_delivery = round(delivered / max(active_people, 1), 1)
    return {
        "delivered": delivered,
        "testing": testing,
        "developing": developing,
        "blocked": blocked,
        "people_total": len(people_total),
        "active_people": active_people,
        "result_conversion": result_conversion,
        "closure_pressure": closure_pressure,
        "red_lines": red_lines,
        "yellow_lines": yellow_lines,
        "attention_debt": attention_debt,
        "per_capita_delivery": per_capita_delivery,
    }


def metric_summary_section(metrics: dict[str, Any]) -> list[str]:
    debt_level = "高" if metrics["attention_debt"] >= 80 else "中" if metrics["attention_debt"] >= 40 else "低"
    pressure_level = "高压" if metrics["closure_pressure"] >= 1.8 else "偏高" if metrics["closure_pressure"] >= 1.0 else "可控"
    lines = [
        "## 公司效能量化总览",
        "",
        "| 指标 | 当前值 | 管理含义 | CEO 动作 |",
        "| --- | --- | --- | --- |",
        f"| 结果转化率 | {metrics['result_conversion']}% | 已交付占全部工作状态的比例；越高，说明团队越不是只报过程。 | 低于 45% 时，停止接受“已提测/待回归”式乐观汇报。 |",
        f"| 闭环压力值 | {metrics['closure_pressure']} | 在研、测试、阻塞相对已交付的压力倍数；越高，越说明收口被堆积拖慢。 | 当前为 {pressure_level}，必须压缩尾项和统一验收口径。 |",
        f"| 管理注意力负债 | {metrics['attention_debt']} | 把阻塞、红黄灯和高风险人员折算成一个管理负债分；越高，CEO 越容易被追人和追口径绑住。 | 当前为 {debt_level} 负债，管理动作应优先减少追问链路。 |",
        f"| 延期红黄灯 | 红 {metrics['red_lines']} / 黄 {metrics['yellow_lines']} | 红灯看业务线已失去自然收口能力，黄灯看继续恶化风险。 | 红灯项直接进入日盯，黄灯项进入周盯。 |",
        f"| 人均有效交付 | {metrics['per_capita_delivery']} | 不是算工时，而是看活跃样本里，每人真实交付的密度。 | 用它判断该继续放权，还是该立刻纠偏。 |",
        "",
    ]
    return lines


def delay_level(score: float) -> str:
    if score >= 150:
        return "红灯"
    if score >= 50:
        return "黄灯"
    return "绿灯"


def delay_alert_section(payload: dict[str, Any]) -> list[str]:
    lines = [
        "## 延期红黄灯",
        "",
        "| 业务线 | 红黄灯 | 风险分 | 核心风险 | CEO 动作 |",
        "| --- | --- | --- | --- | --- |",
    ]
    items = payload.get("delay_risk_groups", []) or payload.get("focus_groups", [])
    for item in items[:5]:
        score = float(item.get("delay_score", 0))
        title = clean_group_title(str(item.get("group_title", "")))
        lines.append(
            f"| {title} | {delay_level(score)} | {round(score, 1)} | {results_gap(item)} | {action_hint(title)} |"
        )
    lines.append("")
    return lines


def build_one_liner(payload: dict[str, Any], manual_text: str, manual_same_day: bool) -> str:
    if manual_same_day:
        section = extract_section(manual_text, "一句话判断")
        if section:
            return section
    report_date = cn_date(payload.get("generated_at"))
    required = payload.get("coverage", {}).get("summary", {}).get("required", {}) or {}
    if any(int(required.get(key, 0)) > 0 for key in ("missing", "stale", "unverified")):
        return f"截至{report_date}，关键群覆盖还没有完全闭环，当前第一优先级不是下业务重判，而是补齐必管群和时效断点。"
    groups = select_problem_groups(payload)
    names = "、".join(clean_group_title(str(item.get("group_title", ""))) for item in groups[:3])
    if names:
        return f"截至{report_date}，关键群覆盖已闭环，管理重心应从“继续收消息”切到“强制收口”；当前最该被盯住的是 {names}。"
    return f"截至{report_date}，关键群覆盖已闭环，当前重点不是补数据，而是压缩在研堆积、统一验收口径和减少管理层无效追人。"


def auto_decisions(payload: dict[str, Any]) -> list[str]:
    decisions: list[str] = []
    for item in select_problem_groups(payload):
        title = clean_group_title(str(item.get("group_title", "")))
        if "MoonX" in title:
            decisions.append("把 MoonX 主线从“持续推进”切到“尾项销号”，本周只允许围绕验收、上线和回收动作推进。")
            continue
        if "周报" in title:
            decisions.append("把周报汇总统一改成闭环表，所有事项必须补齐负责人、截止时间、验收人、未收口原因。")
            continue
        if "研发任务" in title:
            decisions.append("要求研发任务线今天内合并成一张统一版本表，旧任务未收口之前，新需求只能排队。")
            continue
        if "效能" in title:
            decisions.append("三部门效能线只看阻塞清零和结果回收，不再接受“方案还在推进中”的泛汇报。")
            continue
        if "翻译" in title:
            decisions.append("翻译线今天内锁定术语审批人与法务签核时限，把流程问题从执行问题里剥离出来。")
            continue
    fallback = [
        "把管理动作从追过程改成盯结果，只保留能被验收和销号的事项。",
        "跨团队事项全部统一到一张版本表里，不再允许各自维护一套口径。",
        "人效判断只看结果产出、闭环速度、跨线拉通和管理占用，不看消息量。",
    ]
    for item in fallback:
        if len(decisions) >= 3:
            break
        decisions.append(item)
    return decisions[:3]


def build_decision_section(payload: dict[str, Any], manual_text: str, manual_same_day: bool) -> list[str]:
    lines = ["## 今天必须拍板的三件事", ""]
    if manual_same_day:
        section = extract_section(manual_text, "今天必须拍板的三件事")
        if section:
            lines.extend(section.splitlines())
            lines.append("")
            return lines
    for idx, decision in enumerate(auto_decisions(payload), start=1):
        lines.append(f"{idx}. {decision}")
    lines.append("")
    return lines


def tracker_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in select_problem_groups(payload)[:4]:
        title = clean_group_title(str(item.get("group_title", "")))
        rows.append(
            {
                "meeting": title,
                "progress": progress_line(item),
                "gap": results_gap(item),
                "todo": action_hint(title),
            }
        )
    while len(rows) < 3:
        rows.append(
            {
                "meeting": "补充闭环项",
                "progress": "当前没有新增足以改写主判断的证据，继续沿用既有闭环要求。",
                "gap": "没有新增主判断，不代表问题已经自然消失。",
                "todo": "继续按统一闭环表追负责人、截止时间和验收结果。",
            }
        )
    return rows


def build_tracker_section(payload: dict[str, Any], manual_text: str, manual_same_day: bool) -> list[str]:
    if manual_same_day:
        section = extract_section(manual_text, "会议级待办追踪页")
        if section:
            return ["## 会议级待办追踪页", "", *section.splitlines(), ""]
    lines = [
        "## 会议级待办追踪页",
        "",
        "| 会议 | 待办进展 | 结果缺口 | 今日新增待办 |",
        "| --- | --- | --- | --- |",
    ]
    for row in tracker_rows(payload):
        lines.append(f"| {row['meeting']} | {row['progress']} | {row['gap']} | {row['todo']} |")
    lines.append("")
    return lines


def auto_problem_heading(title: str) -> str:
    label = clean_group_title(title)
    if "MoonX" in label:
        return "MoonX 主线：推进没有停，但闭环仍然偏弱"
    if "周报" in label:
        return "周报闭环：输入很多，但管理层拿到的结果口径还不够硬"
    if "研发任务" in label:
        return "研发任务线：旧任务未收口，新需求继续加压"
    if "效能" in label:
        return "效能线：问题已经暴露，但收口动作还没完全制度化"
    if "翻译" in label:
        return "翻译线：真正卡点在审批权，不在执行量"
    return f"{label}：需要从过程推进切到结果回收"


def auto_problem_body(item: dict[str, Any]) -> list[str]:
    risk = item.get("risk_stats", {}) or {}
    title = clean_group_title(str(item.get("group_title", "")))
    delivered = int(risk.get("delivered", 0))
    developing = int(risk.get("developing", 0))
    testing = int(risk.get("testing", 0))
    blocked = int(risk.get("blocked", 0))
    recent = int(item.get("recent_event_count", 0))
    latest = short_time(item.get("latest_time"))
    lead = f"{title} 最近一次高价值更新出现在 {latest}。当前已交付 {delivered} 项，在研/测试 {developing + testing} 项，阻塞 {blocked} 项，近两天新增信号 {recent} 条。"
    if blocked >= 12:
        management = "这说明该线不是没人做事，而是结果回收速度低于问题暴露速度，高层会持续被拖入追人和追口径。"
    elif developing + testing > max(3, delivered):
        management = "这说明团队有推进动作，但验收收口偏弱，很容易形成“做了很多、真正收口不多”的幻觉。"
    else:
        management = "这条线已有正向推进，但只要尾项和验收口径不被强行统一，就容易再次回到噪音状态。"
    action = f"建议动作：{action_hint(title)}"
    return [lead, management, action]


def build_problem_sections(payload: dict[str, Any], manual_text: str, manual_same_day: bool) -> list[str]:
    lines: list[str] = []
    used_any = False
    if manual_same_day:
        for prefix in PROBLEM_SECTION_PREFIXES:
            section = extract_section_by_prefix(manual_text, prefix)
            if not section:
                continue
            heading, body = section
            lines.extend([f"## {heading}", "", *body.splitlines(), ""])
            used_any = True
    if used_any:
        return lines
    for item in select_problem_groups(payload)[:4]:
        lines.extend([f"## {auto_problem_heading(str(item.get('group_title', '')))}", "", *auto_problem_body(item), ""])
    return lines


def build_ella_section(report_date: str, manual_text: str) -> list[str]:
    section = extract_section(manual_text, "Ella 项目进度表与周报串联")
    if section:
        return [
            "## Ella 项目进度表与周报串联",
            "",
            *section.splitlines(),
            "",
        ]
    return [
        "## Ella 项目进度表与周报串联",
        "",
        f"截至 {report_date}，Ella 主线缺的不是更多周报，而是统一验收口径和同一事项的跨角色闭环视图。",
        "",
        "| 事项 | 当前阶段 | 跨角色串联 | 主责任人 | 下一里程碑 |",
        "| --- | --- | --- | --- | --- |",
        "| MoonX 尾项收口 | 待验收 | 产品、前端、后端、测试都已进到最后一公里，但销号口径还不够硬 | MoonX 主线 owner | 锁尾项清单并逐项销号 |",
        "| 研发任务统一版本表 | 进行中 | 新需求还在持续流入，旧任务未完全收口，版本口径尚未完全统一 | 研发负责人 | 今天内完成负责人 / ETA / 验收人补齐 |",
        "| 周报闭环表 | 待固化 | 周报已经能串到同一主线，但仍需从消息汇总改成结果闭环表 | 各线负责人 | 后续汇报只保留结果、阻塞和新增待办 |",
        "",
        "- 管理判断：先把事项、阶段、主责任人、下一里程碑统一到一张表里，再谈更细的执行分工。",
        "",
    ]


def build_cost_lever_section(payload: dict[str, Any], metrics: dict[str, Any]) -> list[str]:
    groups = {clean_group_title(str(item.get("group_title", ""))): item for item in select_problem_groups(payload)}
    week_group = groups.get("产研周报闭环")
    dev_group = groups.get("中心研发任务线") or groups.get("永续研发任务线")
    eff_group = groups.get("三部门效能线")
    lines = [
        "## 降本增效三条抓手",
        "",
        "| 抓手 | 覆盖范围 | 直接收益 | 现在就该怎么做 |",
        "| --- | --- | --- | --- |",
        f"| 周报闭环模板 | {int((week_group or {}).get('people_count', 0)) or metrics['people_total']} 个样本 | 把“谁又发了一份周报”改成“哪件事真正收口了”，直接减少管理层重复追问。 | 所有周报只保留负责人、截止时间、验收人、未收口原因。 |",
        f"| 统一版本闭环表 | {int((dev_group or {}).get('people_count', 0)) or metrics['active_people']} 个研发相关样本 | 把插单、返工、口径不一带来的管理成本压到一张表里。 | 旧任务未收口之前，新需求只能排队，不能直接插单。 |",
        f"| 效能与审批收敛 | {int((eff_group or {}).get('people_count', 0)) or metrics['active_people']} 个跨部门样本 | 真正的降本不是少做事，而是减少重复确认、重复汇报和无效升级。 | 把翻译审批、法务签核、效能项验收时限全部写死。 |",
        "",
    ]
    return lines


def positive_reason(item: dict[str, Any]) -> str:
    if item["delivered"] >= max(3, item["testing"]):
        return "结果回收明显快于堆积速度。"
    if item["group_count"] >= 2 and item["delivered"] >= 2:
        return "跨团队拉通有效，能把事项往结果推。"
    if item["delivered"] >= 2:
        return "持续有交付，不是只报过程。"
    return "近期有真实推进，且没有明显拖慢主线收口。"


def positive_action(item: dict[str, Any]) -> str:
    if item["group_count"] >= 2:
        return "继续授权，只盯结果回收。"
    return "继续推进，但只看尾项销号。"


def negative_reason(item: dict[str, Any]) -> str:
    if item["blocked"] >= 5:
        return "阻塞持续偏高，已经开始占用管理层注意力。"
    if item["developing"] + item["testing"] >= max(5, item["delivered"] + 3):
        return "在研和测试堆积高于结果回收，收口节奏偏慢。"
    if item["delivered"] == 0:
        return "动作存在，但没有形成足够可验证结果。"
    return "结果输出和闭环速度还没有跟上事项复杂度。"


def negative_action(item: dict[str, Any]) -> str:
    if item["blocked"] >= 5:
        return "立刻锁负责人、截止时间和验收口径。"
    if item["developing"] + item["testing"] >= 5:
        return "减少过程汇报，直接按完成项验收。"
    return "下轮汇报直接看完成项，不看过程堆砌。"


def auto_people_candidates(people_rows: list[dict[str, Any]], positive: bool) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for item in people_rows:
        if item["name"] in LEADERSHIP_NAMES:
            continue
        if item["workload"] < 5:
            continue
        if positive:
            if item["delivered"] < 2:
                continue
            if item["blocked"] >= 3 or item["risk_score"] >= 25:
                continue
            candidates.append(item)
        else:
            if item["blocked"] + item["developing"] + item["testing"] < 5:
                continue
            candidates.append(item)
    key_name = "positive_score" if positive else "risk_score"
    return sorted(candidates, key=lambda row: row[key_name], reverse=True)


def names_from_rank_tables(section_text: str) -> set[str]:
    names: set[str] = set()
    for raw_line in section_text.splitlines():
        line = raw_line.strip()
        if not (line.startswith("|") and line.endswith("|") and line.count("|") >= 2):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells and all(cell and set(cell) <= {"-", ":", " "} for cell in cells):
            continue
        if len(cells) < 2:
            continue
        if re.fullmatch(r"\d+", cells[0].strip()):
            name = normalize_name(cells[1])
            if name:
                names.add(name)
    return names


def build_people_section(manual_text: str, manual_same_day: bool, people_rows: list[dict[str, Any]]) -> list[str]:
    lines = ["## 关键人效信号：前五 / 后五", ""]
    manual_section = extract_section(manual_text, "关键人效信号：前五 / 后五") if manual_same_day else ""
    excluded_names: set[str] = set()
    if manual_section:
        excluded_names = names_from_rank_tables(manual_section)
        lines.extend(manual_section.splitlines())
        lines.append("")
    else:
        positive = auto_people_candidates(people_rows, positive=True)
        negative = auto_people_candidates(people_rows, positive=False)
        negative_names = {item["name"] for item in negative[:5]}
        positive = [item for item in positive if item["name"] not in negative_names]
        lines.extend(
            [
                "这一页只保留最值得 CEO 直接识别的前五和后五。排序只看四件事：结果产出、闭环速度、跨团队拉通、是否持续占用管理层注意力。",
                "",
                "### 做得好的前五",
                "",
                "| 排名 | 人员 | 本期判断 | CEO 动作 |",
                "| --- | --- | --- | --- |",
            ]
        )
        if positive:
            for idx, item in enumerate(positive[:5], start=1):
                lines.append(f"| {idx} | {item['name']} | {positive_reason(item)} | {positive_action(item)} |")
        else:
            lines.append("| 1 | 暂无稳定样本 | 当前样本不足，不宜强行做人效表扬。 | 继续积累一周再判断。 |")

        lines.extend(
            [
                "",
                "### 做得不好的后五",
                "",
                "| 排名 | 人员 | 本期判断 | CEO 动作 |",
                "| --- | --- | --- | --- |",
            ]
        )
        if negative:
            for idx, item in enumerate(negative[:5], start=1):
                lines.append(f"| {idx} | {item['name']} | {negative_reason(item)} | {negative_action(item)} |")
        else:
            lines.append("| 1 | 暂无稳定样本 | 当前没有足够证据支撑负向人效判断。 | 继续看闭环质量和延期信号。 |")
        lines.append("")

    auto_negative = [item for item in auto_people_candidates(people_rows, positive=False) if item["name"] not in excluded_names]
    if auto_negative:
        lines.extend(
            [
                "### 补充风险名单",
                "",
                "| 人员 | 红黄灯 | 风险分 | 风险判断 |",
                "| --- | --- | --- | --- |",
            ]
        )
        for item in auto_negative[:3]:
            level = "红灯" if item["risk_score"] >= 45 else "黄灯"
            lines.append(f"| {item['name']} | {level} | {item['risk_score']} | {negative_reason(item)} |")
        lines.append("")
    return lines


def uncertain_items(payload: dict[str, Any], manual_text: str, manual_same_day: bool) -> list[str]:
    if manual_same_day:
        section = extract_section(manual_text, "暂不建议下判断的事项")
        if section:
            return [line for line in section.splitlines() if line.strip()]
    items: list[str] = []
    coverage = payload.get("coverage", {}) or {}
    for entry in coverage.get("unregistered_discovered", [])[:2]:
        title = str(entry.get("title", "")).strip()
        if title:
            items.append(f"- {title} 已被发现，但还没有纳入正式注册清单，暂不作为主判断依据。")
    if not items:
        items.append("- 当前没有足够干净的新样本值得单独下结论，但这不代表后续可以停止做覆盖审计。")
    return items[:3]


def build_markdown(period: str, payload: dict[str, Any]) -> str:
    report_date = cn_date(payload.get("generated_at"))
    report_date_key = date_key(payload.get("generated_at"))
    manual_path, manual_text = latest_manual_ceo_text()
    manual_same_day = manual_matches_report_date(manual_path, report_date_key)
    reports = load_report_details(payload)
    people_rows = aggregate_people(reports)
    metrics = company_metrics(payload, reports, people_rows)
    title = "BYDFI 高层决策简报"

    lines: list[str] = [
        f"# {title}",
        "",
        f"- 日期：{report_date}",
        "- 汇报对象：管理层",
        "- 说明：本版只保留对经营判断有直接影响、且已核实的信息。",
        "",
        "## 一句话判断",
        "",
        build_one_liner(payload, manual_text, manual_same_day),
        "",
    ]
    lines.extend(build_decision_section(payload, manual_text, manual_same_day))
    lines.extend(build_tracker_section(payload, manual_text, manual_same_day))
    lines.extend(metric_summary_section(metrics))
    lines.extend(build_problem_sections(payload, manual_text, manual_same_day))
    lines.extend(delay_alert_section(payload))
    lines.extend(["[[PAGEBREAK]]", ""])
    lines.extend(build_ella_section(report_date, manual_text))
    lines.extend(build_cost_lever_section(payload, metrics))
    lines.extend(build_people_section(manual_text, manual_same_day, people_rows))
    lines.extend(["## 暂不建议下判断的事项", ""])
    lines.extend(uncertain_items(payload, manual_text, manual_same_day))
    lines.append("")
    return "\n".join(lines)


def render_pdf(source_md: Path, output_pdf: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(RENDER_SCRIPT),
        "--source",
        str(source_md),
        "--output",
        str(output_pdf),
        "--no-desktop-copy",
    ]
    proc = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "command": command,
    }


def validate_markdown(path: Path) -> dict[str, Any] | None:
    if not VERIFY_SCRIPT.exists():
        return None
    proc = subprocess.run(
        [sys.executable, "-X", "utf8", str(VERIFY_SCRIPT), str(path)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "command": [sys.executable, "-X", "utf8", str(VERIFY_SCRIPT), str(path)],
    }


def render_ops_ceo_brief(period: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    period_key = str(period).strip().lower() or "daily"
    digest_payload = load_payload(period_key, payload=payload)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    md_path = OUTPUT_DIR / f"{period_key}_ceo_brief_{stamp}.md"
    pdf_path = OUTPUT_DIR / f"{period_key}_ceo_brief_{stamp}.pdf"
    latest_md = OUTPUT_DIR / f"{period_key}_ceo_brief_latest.md"
    latest_pdf = OUTPUT_DIR / f"{period_key}_ceo_brief_latest.pdf"

    markdown = build_markdown(period_key, digest_payload)
    write_text(md_path, markdown)
    write_latest_copy(md_path, latest_md)
    validate_result = validate_markdown(md_path)
    render_result = render_pdf(md_path, pdf_path)
    if not render_result["ok"]:
        raise RuntimeError(render_result["stderr"] or "CEO brief PDF render failed.")
    write_latest_copy(pdf_path, latest_pdf)
    return {
        "period": period_key,
        "md_path": str(md_path.resolve()),
        "pdf_path": str(pdf_path.resolve()),
        "latest_md_path": str(latest_md.resolve()),
        "latest_pdf_path": str(latest_pdf.resolve()),
        "validate": validate_result,
        "render": render_result,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate an automated management-facing CEO brief from the ops digest payload.")
    parser.add_argument("--period", default="daily", choices=["daily", "weekly"])
    parser.add_argument("--payload", type=Path, default=None)
    args = parser.parse_args()
    payload = read_json(args.payload) if args.payload else None
    result = render_ops_ceo_brief(args.period, payload=payload)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
