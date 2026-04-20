from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


SYSTEM_PATTERNS = [
    "加入此群",
    "将群名称",
    "添加为群管理员",
    "更新了群头像",
    "撤回了一条消息",
]

NOISE_PATTERNS = [
    "上传日志",
    "联系客服",
    "帮助中心",
    "效率指南",
    "加载中",
]

AI_HINT_PATTERNS = [
    "AI生成",
    "可能不准确",
    "谨慎甄别",
    "智能纪要",
]


@dataclass
class RiskItem:
    severity: str
    title: str
    evidence: list[str]
    impact: str
    action: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit evidence quality and report hallucination risk.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parent))
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8-sig")
    return json.loads(text)


def query_one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def query_rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    return conn.execute(sql, params).fetchall()


def exists(path: Path) -> bool:
    return path.exists()


def count_like(conn: sqlite3.Connection, table: str, column: str, patterns: list[str]) -> int:
    clauses = " OR ".join([f"{column} LIKE ?" for _ in patterns])
    sql = f"SELECT COUNT(*) FROM {table} WHERE {clauses}"
    args = tuple(f"%{pattern}%" for pattern in patterns)
    return int(query_one(conn, sql, args) or 0)


def get_latest_substantive_record(conn: sqlite3.Connection) -> dict[str, Any] | None:
    filters = " AND ".join([f"parsed_text NOT LIKE ?" for _ in SYSTEM_PATTERNS])
    sql = f"""
        SELECT id, message_timestamp, source_title, reporter_name, source_type, substr(parsed_text, 1, 800)
        FROM audit_records
        WHERE length(parsed_text) >= 120
          AND {filters}
        ORDER BY message_timestamp DESC
        LIMIT 1
    """
    args = tuple(f"%{pattern}%" for pattern in SYSTEM_PATTERNS)
    row = conn.execute(sql, args).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "message_timestamp": row[1],
        "source_title": row[2],
        "reporter_name": row[3],
        "source_type": row[4],
        "preview": row[5],
    }


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def safe_slug(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_-]+", "_", value)


def build_report(repo_root: Path) -> tuple[dict[str, Any], str]:
    data_dir = repo_root / "data"
    reports_dir = data_dir / "reports"
    logs_dir = data_dir / "logs" / "autopilot"
    db_path = data_dir / "audit_records.sqlite3"
    autopilot_state_path = reports_dir / "autopilot_state.json"
    autopilot_log_path = logs_dir / "autopilot.log"
    missing_entrypoints = [
        name
        for name in ("run_incremental_cycle.py", "run_history_backfill.py")
        if not exists(repo_root / name)
    ]

    state = read_json(autopilot_state_path)
    autopilot_log = autopilot_log_path.read_text(encoding="utf-8") if autopilot_log_path.exists() else ""

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    latest_any_ts = query_one(conn, "SELECT MAX(message_timestamp) FROM audit_records")
    latest_substantive = get_latest_substantive_record(conn)
    audit_runs_count = int(query_one(conn, "SELECT COUNT(*) FROM audit_runs") or 0)
    source_docs_count = int(query_one(conn, "SELECT COUNT(*) FROM source_documents") or 0)
    short_dept_history_count = int(
        query_one(conn, "SELECT COUNT(*) FROM dept_meeting_history WHERE length(content) <= 40") or 0
    )
    ai_summary_doc_count = count_like(conn, "source_documents", "content_text", AI_HINT_PATTERNS)
    noisy_doc_count = count_like(conn, "source_documents", "content_text", NOISE_PATTERNS)
    system_noise_record_count = count_like(conn, "audit_records", "parsed_text", SYSTEM_PATTERNS)
    ai_summary_record_count = int(
        query_one(
            conn,
            """
            SELECT COUNT(*)
            FROM audit_records
            WHERE source_title LIKE '%群汇总分析%'
               OR source_title LIKE '%Claude分析机器人%'
               OR parsed_text LIKE '%群汇总分析%'
               OR parsed_text LIKE '%面向CEO的总览判断%'
            """,
        )
        or 0
    )
    encoded_title_issue_count = int(
        query_one(
            conn,
            "SELECT COUNT(*) FROM source_documents WHERE title LIKE '%' || char(1) || '%'",
        )
        or 0
    )
    replied_without_runs = int(
        query_one(
            conn,
            """
            SELECT COUNT(*)
            FROM inbound_messages
            WHERE status = 'replied'
              AND trim(coalesce(audit_result, '')) != ''
            """,
        )
        or 0
    )

    risks: list[RiskItem] = []

    if missing_entrypoints:
        risks.append(
            RiskItem(
                severity="critical",
                title="Pipeline entrypoints are missing",
                evidence=[
                    f"Missing files: {', '.join(missing_entrypoints)}",
                    f"autopilot_state current failure: {state.get('last_incremental_retry_tail', '')}",
                    f"autopilot_state backfill failure: {state.get('last_backfill_tail', '')}",
                ],
                impact="The reporting pipeline cannot be reproduced from the current snapshot.",
                action="Do not publish fresh management analysis until the runnable entrypoints are restored.",
            )
        )

    if "degraded" in autopilot_log.lower() or state.get("last_incremental_exit") or state.get("last_backfill_exit"):
        risks.append(
            RiskItem(
                severity="critical",
                title="Autopilot health is degraded",
                evidence=[
                    f"last_incremental_exit={state.get('last_incremental_exit')}",
                    f"last_incremental_retry_exit={state.get('last_incremental_retry_exit')}",
                    f"last_backfill_exit={state.get('last_backfill_exit')}",
                    "autopilot.log contains timeout/retry/degraded health markers",
                ],
                impact="Any 'latest' report may actually be stale, partial, or failed output.",
                action="Gate report generation on a healthy pipeline status and explicit run success.",
            )
        )

    if audit_runs_count == 0:
        risks.append(
            RiskItem(
                severity="critical",
                title="No durable audit run lineage exists",
                evidence=[
                    "audit_runs count = 0",
                    f"inbound_messages replied with audit_result markers = {replied_without_runs}",
                ],
                impact="Claims cannot be traced back to prompt, model, source set, or execution run.",
                action="Persist run ids, prompt hashes, model ids, source ids, and output artifacts for every report.",
            )
        )

    if ai_summary_record_count:
        risks.append(
            RiskItem(
                severity="high",
                title="AI-generated analysis is mixed into the evidence store",
                evidence=[
                    f"analysis-like audit_records count = {ai_summary_record_count}",
                    "Examples include source titles containing 群汇总分析 or Claude分析机器人",
                ],
                impact="Later reports can end up summarizing prior AI output instead of first-order evidence.",
                action="Separate analysis outputs from raw evidence tables and exclude them from retrieval by default.",
            )
        )

    if short_dept_history_count:
        risks.append(
            RiskItem(
                severity="high",
                title="Department history contains empty or near-empty records",
                evidence=[f"dept_meeting_history records with content length <= 40: {short_dept_history_count}"],
                impact="Trend and department assessments can be driven by placeholder records instead of real content.",
                action="Reject low-content history rows from department analysis until repaired or enriched.",
            )
        )

    if noisy_doc_count or system_noise_record_count:
        risks.append(
            RiskItem(
                severity="high",
                title="Noise and system chatter are mixed into business evidence",
                evidence=[
                    f"source_documents noise hits = {noisy_doc_count}",
                    f"audit_records system-message hits = {system_noise_record_count}",
                ],
                impact="The model can mistake group operations, UI chrome, and support widgets for business progress.",
                action="Classify and strip system events/UI chrome before any summarization step.",
            )
        )

    if encoded_title_issue_count:
        risks.append(
            RiskItem(
                severity="medium",
                title="Encoding pollution exists in source metadata",
                evidence=[f"source_documents titles containing control chars = {encoded_title_issue_count}"],
                impact="Deduplication, grouping, and attribution become unreliable.",
                action="Repair encoding issues before those rows participate in retrieval or aggregation.",
            )
        )

    if ai_summary_doc_count:
        risks.append(
            RiskItem(
                severity="medium",
                title="Secondary AI meeting summaries exist in source documents",
                evidence=[f"AI summary / caution marker hits in source_documents = {ai_summary_doc_count}"],
                impact="These documents can still be useful, but only as secondary evidence with lower confidence.",
                action="Label them as secondary evidence and require first-order confirmation for management conclusions.",
            )
        )

    overall_status = "fail" if any(risk.severity == "critical" for risk in risks) else "warn"
    generated_at = iso_now()
    report = {
        "generated_at": generated_at,
        "repo_root": str(repo_root),
        "overall_status": overall_status,
        "latest_any_message_timestamp": latest_any_ts,
        "latest_substantive_record": latest_substantive,
        "metrics": {
            "audit_runs_count": audit_runs_count,
            "source_documents_count": source_docs_count,
            "short_dept_history_count": short_dept_history_count,
            "ai_summary_doc_count": ai_summary_doc_count,
            "noisy_doc_count": noisy_doc_count,
            "system_noise_record_count": system_noise_record_count,
            "ai_summary_record_count": ai_summary_record_count,
            "encoded_title_issue_count": encoded_title_issue_count,
            "replied_without_runs": replied_without_runs,
        },
        "missing_entrypoints": missing_entrypoints,
        "autopilot_state": state,
        "risks": [asdict(risk) for risk in risks],
    }

    risk_lines = "\n".join(
        [
            "\n".join(
                [
                    f"### [{risk.severity.upper()}] {risk.title}",
                    "",
                    "Evidence:",
                    *[f"- {item}" for item in risk.evidence],
                    "",
                    f"Impact: {risk.impact}",
                    f"Action: {risk.action}",
                ]
            )
            for risk in risks
        ]
    )

    latest_substantive_md = "None"
    if latest_substantive:
        latest_substantive_md = "\n".join(
            [
                f"- id: {latest_substantive['id']}",
                f"- timestamp: {latest_substantive['message_timestamp']}",
                f"- title: {latest_substantive['source_title']}",
                f"- reporter: {latest_substantive['reporter_name']}",
                f"- source_type: {latest_substantive['source_type']}",
                f"- preview: {latest_substantive['preview'][:400]}",
            ]
        )

    missing_entrypoint_lines = [f"- {name}" for name in missing_entrypoints] if missing_entrypoints else ["- none"]
    markdown_lines = [
        "# Evidence Guard Audit",
        "",
        f"- generated_at: {generated_at}",
        f"- overall_status: {overall_status}",
        f"- repo_root: {repo_root}",
        f"- latest_any_message_timestamp: {latest_any_ts}",
        "",
        "## Latest Substantive Record",
        "",
        latest_substantive_md,
        "",
        "## Metrics",
        "",
        f"- audit_runs_count: {audit_runs_count}",
        f"- source_documents_count: {source_docs_count}",
        f"- short_dept_history_count: {short_dept_history_count}",
        f"- ai_summary_doc_count: {ai_summary_doc_count}",
        f"- noisy_doc_count: {noisy_doc_count}",
        f"- system_noise_record_count: {system_noise_record_count}",
        f"- ai_summary_record_count: {ai_summary_record_count}",
        f"- encoded_title_issue_count: {encoded_title_issue_count}",
        f"- replied_without_runs: {replied_without_runs}",
        "",
        "## Missing Entrypoints",
        "",
    ]
    markdown_lines.extend(missing_entrypoint_lines)
    markdown_lines.extend(
        [
            "",
            "## Risks",
            "",
            risk_lines or "No risks detected.",
            "",
            "## Hard Gate",
            "",
            "Do not publish a management report when overall_status is `fail`.",
        ]
    )
    markdown = "\n".join(markdown_lines)

    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = safe_slug(generated_at.replace(":", "").replace("+", "_"))
    (reports_dir / f"evidence_guard_{timestamp}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (reports_dir / f"evidence_guard_{timestamp}.md").write_text(markdown, encoding="utf-8")
    return report, markdown


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    report, _ = build_report(repo_root)
    print(f"overall_status={report['overall_status']}")
    print(f"latest_any_message_timestamp={report['latest_any_message_timestamp']}")


if __name__ == "__main__":
    main()
