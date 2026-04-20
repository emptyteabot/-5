from __future__ import annotations

import argparse
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
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


ROOT = Path(__file__).resolve().parent
REPORTS_DIR = ROOT / "data" / "reports"
DEFAULT_SOURCE_MD = REPORTS_DIR / "ceo_brief_20260407.md"
DEFAULT_OUTPUT_PDF = REPORTS_DIR / "ceo_brief_20260407.pdf"
DESKTOP_TARGETS = [
    Path.home() / "Desktop" / "BYDFI_CEO_Brief.pdf",
    Path.home() / "Desktop" / "BYDFI_CEO_Brief_Final.pdf",
]
FONT_CANDIDATES = [
    Path(r"C:\Windows\Fonts\msyh.ttc"),
    Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc"),
]
FONT_NAME = "BYDFICeoBriefFont"
CONTENT_WIDTH = A4[0] - 30 * mm


def register_font() -> None:
    if FONT_NAME in pdfmetrics.getRegisteredFontNames():
        return
    for candidate in FONT_CANDIDATES:
        if candidate.exists():
            pdfmetrics.registerFont(TTFont(FONT_NAME, str(candidate), subfontIndex=0))
            return
    raise RuntimeError("No usable Chinese font found for CEO brief PDF rendering.")


def format_inline(text: str) -> str:
    text = escape(text.strip())
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"`([^`]+)`", r'<font color="#0f172a">\1</font>', text)
    return text


def build_styles() -> dict[str, ParagraphStyle]:
    styles = getSampleStyleSheet()
    body_common = {
        "fontName": FONT_NAME,
        "fontSize": 10.1,
        "leading": 15,
        "textColor": colors.HexColor("#1f2937"),
        "wordWrap": "CJK",
    }
    return {
        "title": ParagraphStyle(
            "TitleCN",
            parent=styles["Title"],
            fontName=FONT_NAME,
            fontSize=22,
            leading=28,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#0f172a"),
            spaceAfter=10,
        ),
        "subtitle": ParagraphStyle(
            "SubtitleCN",
            parent=styles["BodyText"],
            fontName=FONT_NAME,
            fontSize=10,
            leading=14,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#475569"),
            spaceAfter=14,
            wordWrap="CJK",
        ),
        "h1": ParagraphStyle(
            "H1CN",
            parent=styles["Heading1"],
            fontName=FONT_NAME,
            fontSize=14,
            leading=20,
            textColor=colors.HexColor("#111827"),
            spaceBefore=12,
            spaceAfter=6,
            wordWrap="CJK",
        ),
        "h2": ParagraphStyle(
            "H2CN",
            parent=styles["Heading2"],
            fontName=FONT_NAME,
            fontSize=11.5,
            leading=16,
            textColor=colors.HexColor("#0f172a"),
            spaceBefore=8,
            spaceAfter=4,
            wordWrap="CJK",
        ),
        "body": ParagraphStyle(
            "BodyCN",
            parent=styles["BodyText"],
            alignment=TA_LEFT,
            spaceAfter=4,
            **body_common,
        ),
        "bullet": ParagraphStyle(
            "BulletCN",
            parent=styles["BodyText"],
            leftIndent=12,
            bulletIndent=0,
            spaceAfter=4,
            **body_common,
        ),
        "numbered": ParagraphStyle(
            "NumberedCN",
            parent=styles["BodyText"],
            leftIndent=16,
            bulletIndent=0,
            spaceAfter=4,
            **body_common,
        ),
        "table_header": ParagraphStyle(
            "TableHeaderCN",
            parent=styles["BodyText"],
            fontName=FONT_NAME,
            fontSize=9.2,
            leading=12,
            textColor=colors.HexColor("#0f172a"),
            alignment=TA_LEFT,
            wordWrap="CJK",
        ),
        "table_body": ParagraphStyle(
            "TableBodyCN",
            parent=styles["BodyText"],
            fontName=FONT_NAME,
            fontSize=8.9,
            leading=12,
            textColor=colors.HexColor("#1f2937"),
            alignment=TA_LEFT,
            wordWrap="CJK",
        ),
    }


def on_page(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFont(FONT_NAME, 8.5)
    canvas.setFillColor(colors.HexColor("#64748b"))
    canvas.drawString(doc.leftMargin, 8 * mm, "BYDFI 高层决策简报 | 管理层版本")
    canvas.drawRightString(A4[0] - doc.rightMargin, 8 * mm, f"第 {canvas.getPageNumber()} 页")
    canvas.restoreState()


def load_lines(source_md: Path) -> list[str]:
    return source_md.read_text(encoding="utf-8").splitlines()


def is_table_row(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith("|") and stripped.endswith("|") and stripped.count("|") >= 2


def split_table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def is_table_divider(line: str) -> bool:
    cells = split_table_row(line)
    return bool(cells) and all(cell and set(cell) <= {"-", ":", " "} for cell in cells)


def normalize_row(row: list[str], col_count: int) -> list[str]:
    if len(row) < col_count:
        row = row + [""] * (col_count - len(row))
    return row[:col_count]


def parse_table(lines: list[str], start_idx: int) -> tuple[list[list[str]], int] | None:
    if start_idx + 1 >= len(lines):
        return None
    if not is_table_row(lines[start_idx]):
        return None
    if not is_table_row(lines[start_idx + 1]) or not is_table_divider(lines[start_idx + 1]):
        return None

    header = split_table_row(lines[start_idx])
    col_count = len(header)
    rows = [normalize_row(header, col_count)]
    idx = start_idx + 2
    while idx < len(lines) and is_table_row(lines[idx]):
        rows.append(normalize_row(split_table_row(lines[idx]), col_count))
        idx += 1
    return rows, idx


def guess_col_widths(headers: list[str]) -> list[float]:
    if headers == ["会议", "待办进展", "结果缺口", "今日新增待办"]:
        return [28 * mm, 54 * mm, 42 * mm, 56 * mm]
    if headers == ["事项", "当前阶段", "跨角色串联", "主责任人", "下一里程碑"]:
        return [24 * mm, 22 * mm, 64 * mm, 28 * mm, 42 * mm]
    col_width = CONTENT_WIDTH / len(headers)
    return [col_width] * len(headers)


def build_table(rows: list[list[str]], styles: dict[str, ParagraphStyle]) -> Table:
    headers = rows[0]
    col_widths = guess_col_widths(headers)
    formatted_rows = []
    for row_idx, row in enumerate(rows):
        cell_style = styles["table_header"] if row_idx == 0 else styles["table_body"]
        formatted_rows.append([Paragraph(format_inline(cell), cell_style) for cell in row])

    table = Table(formatted_rows, colWidths=col_widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ]
        )
    )
    return table


def build_story(lines: list[str]):
    styles = build_styles()
    title = "BYDFI 高层决策简报"
    if lines and lines[0].startswith("# "):
        title = lines[0][2:].strip()

    story = [
        Paragraph(format_inline(title), styles["title"]),
        Paragraph("只保留已核实的一手经营判断，供管理层直接阅读", styles["subtitle"]),
    ]

    idx = 0
    while idx < len(lines):
        line = lines[idx].rstrip()
        if idx == 0 and line.startswith("# "):
            idx += 1
            continue
        if not line.strip():
            story.append(Spacer(1, 3))
            idx += 1
            continue
        if line.strip() == "[[PAGEBREAK]]":
            story.append(PageBreak())
            idx += 1
            continue

        parsed_table = parse_table(lines, idx)
        if parsed_table:
            table_rows, next_idx = parsed_table
            story.append(build_table(table_rows, styles))
            story.append(Spacer(1, 6))
            idx = next_idx
            continue

        if line.startswith("## "):
            story.append(Paragraph(format_inline(line[3:]), styles["h1"]))
            idx += 1
            continue
        if line.startswith("### "):
            story.append(Paragraph(format_inline(line[4:]), styles["h2"]))
            idx += 1
            continue
        if line.startswith("- "):
            story.append(Paragraph(format_inline(line[2:]), styles["bullet"], bulletText="•"))
            idx += 1
            continue

        numbered = re.match(r"^(\d+)\.\s+(.*)$", line)
        if numbered:
            story.append(
                Paragraph(
                    format_inline(numbered.group(2)),
                    styles["numbered"],
                    bulletText=f"{numbered.group(1)}.",
                )
            )
            idx += 1
            continue

        story.append(Paragraph(format_inline(line), styles["body"]))
        idx += 1

    return story


def build_pdf(source_md: Path, output_pdf: Path) -> Path:
    register_font()
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    lines = load_lines(source_md)
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title="BYDFI 高层决策简报",
        author="OpenAI Codex",
    )
    doc.build(build_story(lines), onFirstPage=on_page, onLaterPages=on_page)
    return output_pdf


def copy_outputs(pdf_path: Path, targets: list[Path] | None = None) -> None:
    for target in targets or DESKTOP_TARGETS:
        if target.parent.exists():
            shutil.copy2(pdf_path, target)


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a management-facing BYDFI CEO brief PDF.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE_MD)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PDF)
    parser.add_argument("--desktop-target", action="append", default=None)
    parser.add_argument("--no-desktop-copy", action="store_true")
    args = parser.parse_args()

    pdf_path = build_pdf(args.source, args.output)
    if not args.no_desktop_copy:
        targets = [Path(item) for item in args.desktop_target] if args.desktop_target else None
        copy_outputs(pdf_path, targets=targets)
    print(str(pdf_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
