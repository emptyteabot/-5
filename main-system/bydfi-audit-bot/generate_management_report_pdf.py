from __future__ import annotations

import re
import shutil
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer


ROOT = Path(__file__).resolve().parent
REPORTS_DIR = ROOT / "data" / "reports"
SOURCE_MD = REPORTS_DIR / "management_report_20260407.md"
OUTPUT_PDF = REPORTS_DIR / "management_report_20260407.pdf"
DESKTOP_COPY = Path.home() / "Desktop" / "BYDFI_Management_Report.pdf"
DESKTOP_COPY_ALT = Path.home() / "Desktop" / "BYDFI_Management_Report_Copy.pdf"
FONT_CANDIDATES = [
    Path(r"C:\Windows\Fonts\msyh.ttc"),
    Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc"),
]
FONT_NAME = "BYDFIManagementFont"


def register_font() -> None:
    if FONT_NAME in pdfmetrics.getRegisteredFontNames():
        return
    for candidate in FONT_CANDIDATES:
        if candidate.exists():
            pdfmetrics.registerFont(TTFont(FONT_NAME, str(candidate), subfontIndex=0))
            return
    raise RuntimeError("No usable Chinese font found for management PDF rendering.")


def format_inline(text: str) -> str:
    text = escape(text.strip())
    text = re.sub(r"`([^`]+)`", r"【\1】", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    return text


def load_lines() -> list[str]:
    return SOURCE_MD.read_text(encoding="utf-8").splitlines()


def build_styles() -> dict[str, ParagraphStyle]:
    styles = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "TitleCN",
            parent=styles["Title"],
            fontName=FONT_NAME,
            fontSize=20,
            leading=26,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#0f172a"),
            spaceAfter=8,
        ),
        "subtitle": ParagraphStyle(
            "SubtitleCN",
            parent=styles["BodyText"],
            fontName=FONT_NAME,
            fontSize=10,
            leading=14,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#475569"),
            spaceAfter=12,
        ),
        "h1": ParagraphStyle(
            "H1CN",
            parent=styles["Heading1"],
            fontName=FONT_NAME,
            fontSize=14,
            leading=20,
            textColor=colors.HexColor("#111827"),
            spaceBefore=10,
            spaceAfter=6,
        ),
        "h2": ParagraphStyle(
            "H2CN",
            parent=styles["Heading2"],
            fontName=FONT_NAME,
            fontSize=11.5,
            leading=17,
            textColor=colors.HexColor("#111827"),
            spaceBefore=8,
            spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "BodyCN",
            parent=styles["BodyText"],
            fontName=FONT_NAME,
            fontSize=9.8,
            leading=15,
            alignment=TA_LEFT,
            textColor=colors.HexColor("#1f2937"),
            spaceAfter=4,
        ),
        "bullet": ParagraphStyle(
            "BulletCN",
            parent=styles["BodyText"],
            fontName=FONT_NAME,
            fontSize=9.8,
            leading=15,
            leftIndent=12,
            firstLineIndent=0,
            bulletIndent=0,
            textColor=colors.HexColor("#1f2937"),
            spaceAfter=4,
        ),
        "numbered": ParagraphStyle(
            "NumberedCN",
            parent=styles["BodyText"],
            fontName=FONT_NAME,
            fontSize=9.8,
            leading=15,
            leftIndent=16,
            firstLineIndent=0,
            bulletIndent=0,
            textColor=colors.HexColor("#1f2937"),
            spaceAfter=4,
        ),
    }


def on_page(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFont(FONT_NAME, 8.5)
    canvas.setFillColor(colors.HexColor("#64748b"))
    canvas.drawString(doc.leftMargin, 8 * mm, "BYDFI 高层决策报告 | 2026-04-07")
    canvas.drawRightString(A4[0] - doc.rightMargin, 8 * mm, f"第 {canvas.getPageNumber()} 页")
    canvas.restoreState()


def build_story(lines: list[str]):
    styles = build_styles()
    title = "BYDFI 高层决策报告（截至 2026-04-07）"
    if lines and lines[0].startswith("# "):
        title = lines[0][2:].strip()

    story = [
        Paragraph(format_inline(title), styles["title"]),
        Paragraph("依据 2026-04-07 版 Markdown 报告渲染", styles["subtitle"]),
    ]

    for idx, raw in enumerate(lines):
        line = raw.rstrip()
        if idx == 0 and line.startswith("# "):
            continue
        if not line.strip():
            story.append(Spacer(1, 3))
            continue
        if line.startswith("## "):
            story.append(Paragraph(format_inline(line[3:]), styles["h1"]))
            continue
        if line.startswith("### "):
            story.append(Paragraph(format_inline(line[4:]), styles["h2"]))
            continue
        if line.startswith("- "):
            story.append(Paragraph(format_inline(line[2:]), styles["bullet"], bulletText="•"))
            continue
        match = re.match(r"^(\d+)\.\s+(.*)$", line)
        if match:
            story.append(
                Paragraph(
                    format_inline(match.group(2)),
                    styles["numbered"],
                    bulletText=f"{match.group(1)}.",
                )
            )
            continue
        story.append(Paragraph(format_inline(line), styles["body"]))

    return story


def build_pdf() -> Path:
    register_font()
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    lines = load_lines()
    doc = SimpleDocTemplate(
        str(OUTPUT_PDF),
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title="BYDFI 高层决策报告（截至 2026-04-07）",
        author="OpenAI Codex",
    )
    doc.build(build_story(lines), onFirstPage=on_page, onLaterPages=on_page)
    return OUTPUT_PDF


def copy_outputs(pdf_path: Path) -> None:
    for target in (DESKTOP_COPY, DESKTOP_COPY_ALT):
        if target.parent.exists():
            shutil.copy2(pdf_path, target)


def main() -> int:
    pdf_path = build_pdf()
    copy_outputs(pdf_path)
    print(str(pdf_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
