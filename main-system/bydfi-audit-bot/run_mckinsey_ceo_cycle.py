from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))

from ops_digest_lib import run_ops_cycle  # noqa: E402


VERIFY_SCRIPT = Path.home() / ".codex" / "skills" / "bydfi-ceo-brief" / "scripts" / "verify_ceo_brief.py"
RENDER_SCRIPT = ROOT / "generate_ceo_brief_pdf.py"
OUTPUT_DIR = ROOT / "output" / "mckinsey"
FINAL_MD_PATH = ROOT / "data" / "reports" / "ceo_brief_final_send.md"
FINAL_PDF_PATH = ROOT / "data" / "reports" / "高层决策报告_终版.pdf"
DESKTOP_TARGETS = [
    Path.home() / "Desktop" / "高层决策报告_终版.pdf",
    Path(r"E:\UserData\cyh\Desktop\高层决策报告_终版.pdf"),
]


def run_subprocess(command: list[str], *, cwd: Path, timeout: int = 240) -> dict[str, Any]:
    proc = subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "command": command,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_latest_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def validate_markdown(path: Path) -> dict[str, Any] | None:
    if not VERIFY_SCRIPT.exists() or not path.exists():
        return None
    return run_subprocess(
        [sys.executable, "-X", "utf8", str(VERIFY_SCRIPT), str(path)],
        cwd=ROOT,
        timeout=120,
    )


def render_final_pdf(markdown_path: Path) -> dict[str, Any]:
    validate_result = validate_markdown(markdown_path)
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(RENDER_SCRIPT),
        "--source",
        str(markdown_path),
        "--output",
        str(FINAL_PDF_PATH),
    ]
    for target in DESKTOP_TARGETS:
        command.extend(["--desktop-target", str(target)])
    render_result = run_subprocess(command, cwd=ROOT, timeout=300)
    return {
        "source_md_path": str(markdown_path.resolve()),
        "output_pdf_path": str(FINAL_PDF_PATH.resolve()),
        "desktop_targets": [str(path) for path in DESKTOP_TARGETS],
        "validate": validate_result,
        "render": render_result,
    }


def build_context_markdown(payload: dict[str, Any]) -> str:
    cycle = payload.get("cycle", {}) or {}
    publish_guard = cycle.get("publish_guard", {}) or {}
    final_render = payload.get("final_render")
    lines = [
        "# BYDFI 麦肯锡级日报入口",
        "",
        f"- 生成时间：{payload.get('generated_at', '')}",
        f"- 周期：{payload.get('period', '')}",
        f"- 一句话判断：{cycle.get('one_liner', '')}",
        f"- 覆盖状态：{json.dumps(cycle.get('coverage_required', {}), ensure_ascii=False)}",
        f"- 发布门禁：{publish_guard.get('guard_status', '')}",
        "",
        "## 固定入口",
        "",
        "- 技能名：`bydfi-mckinsey-report`",
        "- 日报命令：`powershell -NoProfile -ExecutionPolicy Bypass -File scripts\\run_mckinsey_ceo_cycle.ps1 -Period daily`",
        "- 周报命令：`powershell -NoProfile -ExecutionPolicy Bypass -File scripts\\run_mckinsey_ceo_cycle.ps1 -Period weekly`",
        "",
        "## 一句话触发",
        "",
        "- 跑今天增量 CEO 报告",
        "- 跑今天麦肯锡版管理报告",
        "- 先补抓再重写终版 PDF",
        "- 跑本周周日报告",
        "",
        "## 本次产物",
        "",
        f"- 日/周 digest md：`{cycle.get('latest_md_path', '')}`",
        f"- 日/周 digest json：`{cycle.get('latest_json_path', '')}`",
        f"- 自动 CEO 草稿 pdf：`{((cycle.get('ceo_render') or {}).get('latest_pdf_path', ''))}`",
        f"- 最终管理层源稿：`{payload.get('final_md_path', '')}`",
        f"- 最终桌面 PDF：`{DESKTOP_TARGETS[0]}`",
        "",
        "## 使用规则",
        "",
        "- 先跑增量补抓和自动草稿，再改 `ceo_brief_final_send.md`。",
        "- 只有源稿改完并复核后，才渲染 `高层决策报告_终版.pdf`。",
        "- 管理层版本禁止出现工具残留、需求口吻、数据库字段、写稿人内心旁白。",
    ]
    if publish_guard.get("reasons"):
        lines.extend(["", "## 当前门禁提示", ""])
        for reason in publish_guard["reasons"]:
            lines.append(f"- {reason}")
    if final_render:
        lines.extend(
            [
                "",
                "## 最终 PDF 状态",
                "",
                f"- 最终渲染成功：`{((final_render.get('render') or {}).get('ok', False))}`",
                f"- 校验通过：`{((final_render.get('validate') or {}).get('ok', False)) if final_render.get('validate') is not None else 'no-validator'}`",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "## 最终 PDF 状态",
                "",
                "- 本次只跑到增量补抓和自动草稿，没有覆盖最终 CEO 成稿。",
                "- 如果已经改好 `ceo_brief_final_send.md`，再执行：`powershell -NoProfile -ExecutionPolicy Bypass -File scripts\\run_mckinsey_ceo_cycle.ps1 -Period daily -RenderFinalPdf`。",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="One-click BYDFI McKinsey-grade report prep runner.")
    parser.add_argument("--period", choices=["daily", "weekly"], default="daily")
    parser.add_argument("--skip-discover", action="store_true")
    parser.add_argument("--skip-auto-collect", action="store_true")
    parser.add_argument("--deliver", action="store_true")
    parser.add_argument("--stale-hours", type=int, default=48)
    parser.add_argument("--render-final-pdf", action="store_true")
    args = parser.parse_args()

    cycle_result = run_ops_cycle(
        args.period,
        run_discover=not args.skip_discover,
        auto_collect=not args.skip_auto_collect,
        render_ceo=True,
        render_digest_pdf_file=True,
        deliver=bool(args.deliver),
        stale_hours=args.stale_hours,
    )

    final_render = None
    if args.render_final_pdf and FINAL_MD_PATH.exists():
        final_render = render_final_pdf(FINAL_MD_PATH)

    generated_at = datetime.now().astimezone().isoformat()
    context_payload = {
        "generated_at": generated_at,
        "period": args.period,
        "runner": str(Path(__file__).resolve()),
        "cycle": cycle_result,
        "final_md_path": str(FINAL_MD_PATH.resolve()),
        "final_pdf_path": str(FINAL_PDF_PATH.resolve()),
        "desktop_targets": [str(path) for path in DESKTOP_TARGETS],
        "final_render": final_render,
        "one_line_requests": [
            "跑今天增量 CEO 报告",
            "跑今天麦肯锡版管理报告",
            "先补抓再重写终版 PDF",
            "跑本周周日报告",
        ],
    }

    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / f"{args.period}_context_{stamp}.json"
    md_path = OUTPUT_DIR / f"{args.period}_context_{stamp}.md"
    latest_json_path = OUTPUT_DIR / f"{args.period}_context_latest.json"
    latest_md_path = OUTPUT_DIR / f"{args.period}_context_latest.md"

    write_json(json_path, context_payload)
    write_text(md_path, build_context_markdown(context_payload))
    write_latest_copy(json_path, latest_json_path)
    write_latest_copy(md_path, latest_md_path)

    result = {
        "ok": cycle_result.get("ok", False) and (final_render is None or (final_render.get("render") or {}).get("ok", False)),
        "generated_at": generated_at,
        "period": args.period,
        "context_json_path": str(json_path.resolve()),
        "context_md_path": str(md_path.resolve()),
        "latest_context_json_path": str(latest_json_path.resolve()),
        "latest_context_md_path": str(latest_md_path.resolve()),
        "cycle": cycle_result,
        "final_render": final_render,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
