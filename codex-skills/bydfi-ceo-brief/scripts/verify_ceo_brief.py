from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REQUIRED_HEADINGS = [
    "## 一句话判断",
    "## 今天必须拍板的三件事",
    "## 会议级待办追踪页",
    "## Ella 项目进度表与周报串联",
]

REQUIRED_TABLE_HEADERS = [
    "| 会议 | 待办进展 | 结果缺口 | 今日新增待办 |",
    "| 事项 | 当前阶段 | 跨角色串联 | 主责任人 | 下一里程碑 |",
]

BANNED_PATTERNS = [
    r"audit_records?",
    r"audit record",
    r"document_key",
    r"chat_id",
    r"message_id",
    r"run_id",
    r"source_url",
    r"source_content_hash",
    r"Claude分析机器人",
    r"审计机器人",
    r"自动日报能力是否已经稳定恢复",
    r"凌晨补充",
    r"Yohan：",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a BYDFI CEO brief markdown file.")
    parser.add_argument("path", type=Path, help="Path to the markdown file to validate")
    args = parser.parse_args()

    text = args.path.read_text(encoding="utf-8")
    errors: list[str] = []

    for heading in REQUIRED_HEADINGS:
        if heading not in text:
            errors.append(f"Missing required heading: {heading}")

    for header in REQUIRED_TABLE_HEADERS:
        if header not in text:
            errors.append(f"Missing required table header: {header}")

    for pattern in BANNED_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            errors.append(f"Found banned pattern: {pattern}")

    if errors:
        print("CEO brief validation failed:")
        for item in errors:
            print(f"- {item}")
        return 1

    print("CEO brief validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
