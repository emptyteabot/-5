from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "scheduled"
FONT_CANDIDATES = [
    Path(r"C:\Windows\Fonts\msyh.ttc"),
    Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc"),
    Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
]
FONT_NAME = "BYDFIDigestFont"


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_latest_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def register_font() -> None:
    if FONT_NAME in pdfmetrics.getRegisteredFontNames():
        return
    for candidate in FONT_CANDIDATES:
        if candidate.exists():
            pdfmetrics.registerFont(TTFont(FONT_NAME, str(candidate), subfontIndex=0 if candidate.suffix.lower() == ".ttc" else 0))
            return
    raise RuntimeError("No usable PDF font found for ops digest rendering.")


def inline(text: str) -> str:
    return escape(str(text).strip())


def parse_iso(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone()


def display_time(raw: str) -> str:
    dt = parse_iso(raw)
    if dt is None:
        return str(raw or "-").strip() or "-"
    return dt.strftime("%m-%d %H:%M")


def risk_level(score: float) -> str:
    if score >= 150:
        return "红"
    if score >= 50:
        return "黄"
    return "绿"


def attention_level(score: float) -> str:
    if score >= 40:
        return "红"
    if score >= 20:
        return "黄"
    return "绿"


def executive_judgement(payload: dict) -> str:
    required = payload.get("coverage", {}).get("summary", {}).get("required", {})
    if any(int(required.get(key, 0)) > 0 for key in ("missing", "stale", "unverified")):
        return "采集链路还未闭环，先补齐必管群覆盖，再做管理判断。"
    risks = payload.get("delay_risk_groups", []) or []
    if risks:
        names = "、".join(str(item.get("group_title", "")).strip() for item in risks[:2] if str(item.get("group_title", "")).strip())
        if names:
            return f"采集面已可用，但 {names} 的阻塞和待闭环事项仍高，优先盯闭环。"
    return "采集和结构化摘要已可用，重点转向结果回收、人效排序和延期预警。"


def group_judgement(item: dict) -> str:
    risk = item.get("risk_stats", {}) or {}
    reasons: list[str] = []
    if int(risk.get("blocked", 0)) >= 3:
        reasons.append("阻塞项偏多")
    if int(risk.get("developing", 0)) + int(risk.get("testing", 0)) > max(3, int(risk.get("delivered", 0))):
        reasons.append("在研与测试积压高于已交付")
    if int(item.get("recent_event_count", 0)) >= 3:
        reasons.append("近期新增事项流入较快")
    if not reasons:
        reasons.append("持续有交付，优先盯结果回收")
    return "；".join(reasons[:3])


def people_judgement(item: dict) -> str:
    reasons: list[str] = []
    if int(item.get("delivered", 0)) >= 3:
        reasons.append("交付回执较多")
    if int(item.get("group_count", 0)) >= 2:
        reasons.append("跨线串联较多")
    if int(item.get("blocked", 0)) >= 3:
        reasons.append("阻塞项偏多")
    if int(item.get("developing", 0)) + int(item.get("testing", 0)) >= max(4, int(item.get("delivered", 0)) + 2):
        reasons.append("在研/测试积压偏高")
    if not reasons:
        reasons.append("样本正常，继续观察")
    return "；".join(reasons[:3])


def short_groups(groups: list[str], limit: int = 3) -> str:
    values = [str(item).strip() for item in groups if str(item).strip()]
    if not values:
        return "-"
    if len(values) <= limit:
        return "、".join(values)
    return "、".join(values[:limit]) + f" 等{len(values)}个群"


def load_payload(period: str, payload: dict | None = None) -> dict:
    if payload is not None:
        return payload
    target = OUTPUT_DIR / f"{period}_ops_digest_latest.json"
    if not target.exists():
        raise FileNotFoundError(f"Digest payload not found: {target}")
    return read_json(target)


def styles() -> dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "digest-title",
            parent=sample["Title"],
            fontName=FONT_NAME,
            fontSize=20,
            leading=26,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#0f172a"),
            spaceAfter=8,
        ),
        "subtitle": ParagraphStyle(
            "digest-subtitle",
            parent=sample["BodyText"],
            fontName=FONT_NAME,
            fontSize=10,
            leading=14,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#475569"),
            spaceAfter=10,
            wordWrap="CJK",
        ),
        "h1": ParagraphStyle(
            "digest-h1",
            parent=sample["Heading1"],
            fontName=FONT_NAME,
            fontSize=13,
            leading=18,
            textColor=colors.HexColor("#0f172a"),
            spaceBefore=8,
            spaceAfter=4,
            wordWrap="CJK",
        ),
        "body": ParagraphStyle(
            "digest-body",
            parent=sample["BodyText"],
            fontName=FONT_NAME,
            fontSize=9.4,
            leading=13,
            textColor=colors.HexColor("#1f2937"),
            alignment=TA_LEFT,
            wordWrap="CJK",
        ),
        "table_header": ParagraphStyle(
            "digest-table-header",
            parent=sample["BodyText"],
            fontName=FONT_NAME,
            fontSize=8.8,
            leading=11,
            textColor=colors.white,
            wordWrap="CJK",
        ),
        "table_body": ParagraphStyle(
            "digest-table-body",
            parent=sample["BodyText"],
            fontName=FONT_NAME,
            fontSize=8.4,
            leading=11,
            textColor=colors.HexColor("#1f2937"),
            wordWrap="CJK",
        ),
    }


def p(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(inline(text), style)


def build_table(rows: list[list[str]], widths: list[float], palette: str, style_map: dict[str, ParagraphStyle]) -> Table:
    formatted = []
    for row_index, row in enumerate(rows):
        cell_style = style_map["table_header"] if row_index == 0 else style_map["table_body"]
        formatted.append([p(cell, cell_style) for cell in row])
    table = Table(formatted, colWidths=widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(palette)),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ]
        )
    )
    return table


def build_story(payload: dict) -> list:
    style_map = styles()
    period = str(payload.get("period", "daily")).strip().lower()
    title = "BYDFI 日常管理分析" if period == "daily" else "BYDFI 周度管理分析"
    generated_at = display_time(str(payload.get("generated_at", "")))
    db_stats = payload.get("db_stats", {}) or {}
    required = payload.get("coverage", {}).get("summary", {}).get("required", {}) or {}
    story: list = [
        p(title, style_map["title"]),
        p(f"自动生成时间：{generated_at}", style_map["subtitle"]),
        p(executive_judgement(payload), style_map["subtitle"]),
        Spacer(1, 2),
        p(
            "数据底座：消息 "
            f"{db_stats.get('audit_records_count', 0)} 条，文档 {db_stats.get('source_documents_count', 0)} 份，"
            f"采集批次 {db_stats.get('collection_runs_count', 0)} 次；"
            f"必管群 covered={required.get('covered', 0)} / missing={required.get('missing', 0)} / "
            f"stale={required.get('stale', 0)} / unverified={required.get('unverified', 0)}",
            style_map["body"],
        ),
        Spacer(1, 6),
        p("重点业务线", style_map["h1"]),
    ]

    focus_rows = [["业务线", "最新", "交付", "在研/测试", "阻塞", "管理判断"]]
    for item in payload.get("focus_groups", [])[:5]:
        risk = item.get("risk_stats", {}) or {}
        focus_rows.append(
            [
                str(item.get("group_title", "")),
                display_time(str(item.get("latest_time", ""))),
                str(risk.get("delivered", 0)),
                f"{risk.get('developing', 0)} / {risk.get('testing', 0)}",
                str(risk.get("blocked", 0)),
                group_judgement(item),
            ]
        )
    story.append(build_table(focus_rows, [38 * mm, 20 * mm, 14 * mm, 24 * mm, 14 * mm, 70 * mm], "#0f766e", style_map))
    story.append(Spacer(1, 6))

    story.append(p("延期风险红黄灯", style_map["h1"]))
    risk_rows = [["业务线", "级别", "风险分", "阻塞", "说明", "最近触发"]]
    for item in payload.get("delay_risk_groups", [])[:5]:
        triggers = "；".join(str(x).strip() for x in item.get("recent_titles", [])[:2] if str(x).strip()) or "-"
        risk_rows.append(
            [
                str(item.get("group_title", "")),
                risk_level(float(item.get("delay_score", 0))),
                str(item.get("delay_score", 0)),
                str((item.get("risk_stats", {}) or {}).get("blocked", 0)),
                group_judgement(item),
                triggers,
            ]
        )
    story.append(build_table(risk_rows, [34 * mm, 12 * mm, 16 * mm, 14 * mm, 56 * mm, 56 * mm], "#b45309", style_map))
    story.append(PageBreak())

    story.append(p("输出信号前五", style_map["h1"]))
    output_rows = [["人员", "输出分", "交付", "在研/测试", "阻塞", "涉及群", "判断"]]
    for item in payload.get("top_output_people", [])[:5]:
        output_rows.append(
            [
                str(item.get("name", "")),
                str(item.get("output_score", 0)),
                str(item.get("delivered", 0)),
                f"{item.get('developing', 0)} / {item.get('testing', 0)}",
                str(item.get("blocked", 0)),
                short_groups(item.get("groups", [])),
                people_judgement(item),
            ]
        )
    story.append(build_table(output_rows, [22 * mm, 16 * mm, 12 * mm, 20 * mm, 12 * mm, 48 * mm, 52 * mm], "#1d4ed8", style_map))
    story.append(Spacer(1, 6))

    story.append(p("风险关注", style_map["h1"]))
    attention_rows = [["人员", "级别", "关注分", "阻塞", "涉及群", "判断"]]
    for item in payload.get("attention_people", [])[:5]:
        attention_rows.append(
            [
                str(item.get("name", "")),
                attention_level(float(item.get("attention_score", 0))),
                str(item.get("attention_score", 0)),
                str(item.get("blocked", 0)),
                short_groups(item.get("groups", [])),
                people_judgement(item),
            ]
        )
    story.append(build_table(attention_rows, [24 * mm, 12 * mm, 16 * mm, 12 * mm, 56 * mm, 60 * mm], "#7c2d12", style_map))

    errors = payload.get("automation_errors", []) or []
    if errors:
        story.append(Spacer(1, 6))
        story.append(p("当前自动化缺口", style_map["h1"]))
        for error in errors:
            story.append(p(f"• {error}", style_map["body"]))
            story.append(Spacer(1, 2))

    return story


def render_digest_pdf(period: str, payload: dict | None = None) -> dict[str, str]:
    register_font()
    period_key = str(period).strip().lower() or "daily"
    digest_payload = load_payload(period_key, payload=payload)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    pdf_path = OUTPUT_DIR / f"{period_key}_ops_digest_{stamp}.pdf"
    latest_path = OUTPUT_DIR / f"{period_key}_ops_digest_latest.pdf"
    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        title="BYDFI Ops Digest",
        author="OpenAI Codex",
    )
    doc.build(build_story(digest_payload))
    write_latest_copy(pdf_path, latest_path)
    return {
        "pdf_path": str(pdf_path.resolve()),
        "latest_pdf_path": str(latest_path.resolve()),
    }
