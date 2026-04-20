from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ops_ceo_brief import render_ops_ceo_brief
from ops_delivery_lib import deliver_digest_report
from ops_digest_pdf import render_digest_pdf


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"
OUTPUT_DIR = ROOT / "output" / "scheduled"
DB_PATH = ROOT / "data" / "audit_records.sqlite3"
GROUP_COVERAGE_SCRIPT = ROOT / "scripts" / "check_group_coverage.py"
GROUP_COLLECT_SCRIPT = ROOT / "scripts" / "collect_registered_groups.py"
GROUP_COVERAGE_PATH = ROOT / "output" / "group_coverage_latest.json"
HOT_KEYWORDS = ("P0", "新增", "临时任务", "待验收", "待上线", "多账号", "验证码", "SEO", "活动", "空投券", "风控", "预警", "问题")
COLLECTION_FRESH_HOURS = 4
PERIOD_REFRESH_HOURS = {"daily": 48, "weekly": 168}


def local_now() -> datetime:
    return datetime.now().astimezone()


def parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone()


def to_iso(dt: datetime | None) -> str:
    return dt.isoformat() if dt else ""


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def normalize_name(name: str) -> str:
    cleaned = re.sub(r"\d{1,2}:\d{2}$", "", str(name).strip())
    return cleaned.strip() or str(name).strip()


def run_subprocess(args: list[str], timeout_ms: int = 600000) -> dict[str, Any]:
    proc = subprocess.run(
        args,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=max(1, timeout_ms // 1000),
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "command": args,
    }


def load_db_stats() -> dict[str, Any]:
    stats = {
        "audit_records_count": 0,
        "source_documents_count": 0,
        "collection_runs_count": 0,
        "latest_message_timestamp": "",
        "latest_document_fetch": "",
        "collection_status": {},
    }
    if not DB_PATH.exists():
        return stats
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        stats["audit_records_count"] = cur.execute("select count(*) from audit_records").fetchone()[0]
        stats["source_documents_count"] = cur.execute("select count(*) from source_documents").fetchone()[0]
        stats["collection_runs_count"] = cur.execute("select count(*) from collection_runs").fetchone()[0]
        stats["latest_message_timestamp"] = cur.execute("select max(message_timestamp) from audit_records").fetchone()[0] or ""
        stats["latest_document_fetch"] = cur.execute("select max(last_fetched_at) from source_documents").fetchone()[0] or ""
        status_rows = cur.execute("select status, count(*) from collection_runs group by status order by count(*) desc").fetchall()
        stats["collection_status"] = {str(status): count for status, count in status_rows}
    finally:
        conn.close()
    return stats


def lookup_chat_name(chat_id: str) -> str:
    if not chat_id or not DB_PATH.exists():
        return ""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        row = cur.execute(
            """
            select coalesce(
                json_extract(raw_message_json, '$.current_chat_name'),
                json_extract(raw_message_json, '$.chat_name'),
                ''
            )
            from audit_records
            where chat_id = ?
              and coalesce(
                    json_extract(raw_message_json, '$.current_chat_name'),
                    json_extract(raw_message_json, '$.chat_name'),
                    ''
                  ) <> ''
            order by id desc
            limit 1
            """,
            (chat_id,),
        ).fetchone()
        return str(row[0]).strip() if row and row[0] else ""
    finally:
        conn.close()


def current_ceo_assets() -> dict[str, Any]:
    md_files = sorted(REPORTS_DIR.glob("ceo_brief_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    pdf_files = sorted(REPORTS_DIR.glob("ceo_brief_*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {
        "md_path": str(md_files[0].resolve()) if md_files else "",
        "pdf_path": str(pdf_files[0].resolve()) if pdf_files else "",
    }


def latest_manual_ceo_md() -> Path | None:
    files = sorted(REPORTS_DIR.glob("ceo_brief_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def manual_ceo_same_day(report_time: str | None) -> tuple[Path | None, bool]:
    source = latest_manual_ceo_md()
    if source is None:
        return None, False
    report_dt = parse_dt(report_time)
    if report_dt is None:
        return source, False
    match = re.search(r"(\d{8})", source.stem)
    if not match:
        return source, False
    return source, match.group(1) == report_dt.strftime("%Y%m%d")


def load_latest_coverage() -> dict[str, Any] | None:
    if not GROUP_COVERAGE_PATH.exists():
        return None
    try:
        return read_json(GROUP_COVERAGE_PATH)
    except Exception:
        return None


def refresh_coverage(period: str, run_discover: bool, auto_collect: bool, stale_hours: int) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[str]]:
    actions: list[dict[str, Any]] = []
    errors: list[str] = []
    temp_coverage_path = OUTPUT_DIR / f"coverage_{local_now().strftime('%Y%m%d_%H%M%S_%f')}.json"

    def build_coverage_args(include_discover: bool) -> list[str]:
        args = [
            sys.executable,
            "-X",
            "utf8",
            str(GROUP_COVERAGE_SCRIPT),
            "--stale-hours",
            str(stale_hours),
            "--collection-fresh-hours",
            str(COLLECTION_FRESH_HOURS),
            "--write-json",
            str(temp_coverage_path),
        ]
        if include_discover:
            args.append("--run-discover")
        return args

    coverage_args = build_coverage_args(run_discover)
    coverage_run = run_subprocess(coverage_args)
    coverage_payload = None
    if temp_coverage_path.exists():
        try:
            coverage_payload = read_json(temp_coverage_path)
            write_json(GROUP_COVERAGE_PATH, coverage_payload)
        except Exception:
            coverage_payload = None
    if coverage_payload is None and run_discover:
        fallback_run = run_subprocess(build_coverage_args(False))
        actions.append(
            {
                "step": "coverage_audit_fallback_no_discover",
                "ok": fallback_run["ok"],
                "returncode": fallback_run["returncode"],
                "stdout": fallback_run["stdout"][:500],
                "stderr": fallback_run["stderr"][:500],
            }
        )
        if temp_coverage_path.exists():
            try:
                coverage_payload = read_json(temp_coverage_path)
                write_json(GROUP_COVERAGE_PATH, coverage_payload)
            except Exception:
                coverage_payload = None

    if coverage_payload is None:
        coverage_payload = load_latest_coverage()

    actions.append(
        {
            "step": "coverage_audit",
            "ok": coverage_run["ok"],
            "returncode": coverage_run["returncode"],
            "stdout": coverage_run["stdout"][:500],
            "stderr": coverage_run["stderr"][:500],
        }
    )
    if not coverage_run["ok"]:
        errors.append("coverage audit failed; fallback to latest cached coverage if available")

    if coverage_payload and auto_collect:
        required_titles = [str(item.get("title", "")).strip() for item in coverage_payload.get("groups", []) if item.get("required", True)]
        required_titles = [title for title in required_titles if title]
        if required_titles:
            collect_path = OUTPUT_DIR / f"collect_required_{local_now().strftime('%Y%m%d_%H%M%S')}.json"
            collect_args = [
                sys.executable,
                "-X",
                "utf8",
                str(GROUP_COLLECT_SCRIPT),
                "--stale-hours",
                str(stale_hours),
                "--collection-fresh-hours",
                str(COLLECTION_FRESH_HOURS),
                "--refresh-hours",
                str(PERIOD_REFRESH_HOURS.get(period, 48)),
                "--skip-document-fetch",
                "--write-json",
                str(collect_path),
            ]
            for title in required_titles:
                collect_args.extend(["--title", title])
            collect_run = run_subprocess(collect_args)
            actions.append(
                {
                    "step": "collect_registered_groups",
                    "ok": collect_run["ok"],
                    "returncode": collect_run["returncode"],
                    "stdout": collect_run["stdout"][:500],
                    "stderr": collect_run["stderr"][:500],
                    "output_path": str(collect_path.resolve()),
                }
            )
            if not collect_run["ok"]:
                errors.append("registered group collect failed")

            rerun_args = build_coverage_args(run_discover)
            rerun = run_subprocess(rerun_args)
            actions.append(
                {
                    "step": "coverage_audit_after_collect",
                    "ok": rerun["ok"],
                    "returncode": rerun["returncode"],
                    "stdout": rerun["stdout"][:500],
                    "stderr": rerun["stderr"][:500],
                }
            )
            if temp_coverage_path.exists():
                try:
                    coverage_payload = read_json(temp_coverage_path)
                    write_json(GROUP_COVERAGE_PATH, coverage_payload)
                except Exception:
                    coverage_payload = None

            if coverage_payload is None and run_discover:
                rerun_fallback = run_subprocess(build_coverage_args(False))
                actions.append(
                    {
                        "step": "coverage_audit_after_collect_fallback_no_discover",
                        "ok": rerun_fallback["ok"],
                        "returncode": rerun_fallback["returncode"],
                        "stdout": rerun_fallback["stdout"][:500],
                        "stderr": rerun_fallback["stderr"][:500],
                    }
                )
                if temp_coverage_path.exists():
                    try:
                        coverage_payload = read_json(temp_coverage_path)
                        write_json(GROUP_COVERAGE_PATH, coverage_payload)
                    except Exception:
                        coverage_payload = None

            if not rerun["ok"]:
                errors.append("coverage audit after collect failed")
            if coverage_payload is None:
                coverage_payload = load_latest_coverage()
    return coverage_payload, actions, errors


def assess_publish_guard(
    period: str,
    payload: dict[str, Any],
    *,
    run_discover: bool,
    render_result: dict[str, Any] | None,
) -> dict[str, Any]:
    coverage = payload.get("coverage", {}) or {}
    summary = coverage.get("summary", {}) or {}
    required = summary.get("required", {}) or {}
    hard_reasons: list[str] = []
    review_notes: list[str] = []
    report_time = payload.get("generated_at")
    manual_source, manual_same_day = manual_ceo_same_day(report_time)

    discover_selected_count = int(coverage.get("discover_selected_count", 0) or 0)
    if not run_discover:
        if discover_selected_count <= 0:
            hard_reasons.append("本次运行跳过了 discover，且没有可复用的 discover 结果，群覆盖证明失效。")
        else:
            review_notes.append("本次日常运行沿用了最近一次 discover 结果，适合先发给你审阅，不建议直接当作最终对外版。")
    if discover_selected_count <= 0:
        hard_reasons.append("本次 discover 返回 0 个会话，群覆盖证明失效。")
    if any(int(required.get(key, 0) or 0) > 0 for key in ("missing", "stale", "unverified")):
        hard_reasons.append("必管群仍存在 missing / stale / unverified，不能对外发送。")
    collection_stale_titles = list(summary.get("required_collection_stale_titles", []) or [])
    if collection_stale_titles:
        hard_reasons.append(f"必管群未在 {COLLECTION_FRESH_HOURS} 小时内完成补采/校验：{'、'.join(collection_stale_titles[:5])}")
    if not manual_same_day:
        review_notes.append("不存在同日人工校准 CEO 源稿，本次自动发送仅供你审阅，不建议直接转发 CEO。")
    validate_result = (render_result or {}).get("validate")
    if validate_result is not None and not validate_result.get("ok", False):
        hard_reasons.append("CEO 稿结构校验未通过。")

    publishable = not hard_reasons and manual_same_day and run_discover
    review_deliverable = not hard_reasons
    reasons = [*hard_reasons, *review_notes]
    if publishable:
        guard_status = "pass"
    elif review_deliverable:
        guard_status = "review_only"
    else:
        guard_status = "blocked"

    return {
        "period": period,
        "publishable": publishable,
        "review_deliverable": review_deliverable,
        "guard_status": guard_status,
        "checks": {
            "run_discover": run_discover,
            "discover_selected_count": int(coverage.get("discover_selected_count", 0) or 0),
            "required_missing": int(required.get("missing", 0) or 0),
            "required_stale": int(required.get("stale", 0) or 0),
            "required_unverified": int(required.get("unverified", 0) or 0),
            "required_collection_stale_count": int(summary.get("required_collection_stale_count", 0) or 0),
            "manual_ceo_source_path": str(manual_source.resolve()) if manual_source else "",
            "manual_ceo_same_day": manual_same_day,
        },
        "hard_reasons": hard_reasons,
        "review_notes": review_notes,
        "reasons": reasons,
    }


def coverage_title_map(coverage: dict[str, Any] | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in (coverage or {}).get("groups", []):
        title = str(item.get("title", "")).strip()
        for chat_id in item.get("chat_ids", []):
            chat_key = str(chat_id).strip()
            if chat_key and title:
                mapping[chat_key] = title
    return mapping


def recent_titles_from_evidence(evidence_items: list[dict[str, Any]], horizon_start: datetime) -> list[str]:
    titles: list[str] = []
    seen: set[str] = set()
    for item in evidence_items:
        time_value = parse_dt(item.get("time"))
        if time_value is None or time_value < horizon_start:
            continue
        raw_title = str(item.get("title") or item.get("sourceTitle") or "").strip()
        if not raw_title or not any(keyword in raw_title for keyword in HOT_KEYWORDS):
            continue
        if raw_title in seen:
            continue
        seen.add(raw_title)
        titles.append(raw_title)
        if len(titles) >= 5:
            break
    return titles


def load_structured_reports(coverage: dict[str, Any] | None, horizon_days: int) -> list[dict[str, Any]]:
    now = local_now()
    horizon_start = now - timedelta(days=horizon_days)
    title_map = coverage_title_map(coverage)
    reports: list[dict[str, Any]] = []
    for path in sorted(REPORTS_DIR.glob("*.json")):
        try:
            data = read_json(path)
        except Exception:
            continue
        if not isinstance(data, dict) or "peopleCards" not in data or "evidenceIndex" not in data:
            continue
        chat_id = str(data.get("chatId", "")).strip()
        evidence_raw = data.get("evidenceIndex") or {}
        if isinstance(evidence_raw, dict):
            evidence_items = list(evidence_raw.values())
        elif isinstance(evidence_raw, list):
            evidence_items = evidence_raw
        else:
            evidence_items = []
        inferred_title = title_map.get(chat_id) or lookup_chat_name(chat_id) or str(data.get("title", path.stem))
        latest_dt = parse_dt(data.get("endAt"))
        age_hours = round((now - latest_dt).total_seconds() / 3600, 1) if latest_dt else None
        risk_stats = data.get("riskStats") or {}
        recent_event_count = 0
        for item in evidence_items:
            time_value = parse_dt(item.get("time"))
            if time_value is not None and time_value >= horizon_start:
                recent_event_count += 1
        reports.append(
            {
                "file": path.name,
                "chat_id": chat_id,
                "group_title": inferred_title,
                "people_count": int(data.get("peopleCount") or 0),
                "report_count": int(data.get("reportCount") or 0),
                "document_count": int(data.get("documentCount") or 0),
                "start_at": str(data.get("startAt") or ""),
                "end_at": str(data.get("endAt") or ""),
                "latest_dt": latest_dt,
                "age_hours": age_hours,
                "risk_stats": {
                    "delivered": int(risk_stats.get("delivered") or 0),
                    "testing": int(risk_stats.get("testing") or 0),
                    "review": int(risk_stats.get("review") or 0),
                    "developing": int(risk_stats.get("developing") or 0),
                    "blocked": int(risk_stats.get("blocked") or 0),
                },
                "summary_text": str(data.get("summaryText") or "").strip(),
                "people_cards": data.get("peopleCards") or [],
                "evidence_items": evidence_items,
                "recent_event_count": recent_event_count,
                "recent_titles": recent_titles_from_evidence(evidence_items, horizon_start),
            }
        )
    return reports


def build_group_focus(reports: list[dict[str, Any]], horizon_days: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = local_now()
    focus_groups: list[dict[str, Any]] = []
    risk_groups: list[dict[str, Any]] = []
    for report in reports:
        risk = report["risk_stats"]
        latest_dt = report["latest_dt"]
        age_penalty = 0.0
        if latest_dt is not None:
            age_days = max(0.0, (now - latest_dt).total_seconds() / 86400)
            age_penalty = min(age_days, 14.0)
        activity_score = report["recent_event_count"] * 1.8 + risk["delivered"] * 0.4 + risk["testing"] * 0.8 + risk["developing"] * 1.1 + len(report["recent_titles"]) * 1.5 - age_penalty * 0.8
        delay_score = risk["blocked"] * 3.0 + risk["developing"] * 1.8 + risk["testing"] * 1.0 + len(report["recent_titles"]) * 1.2 - risk["delivered"] * 0.25
        reasons: list[str] = []
        if risk["blocked"] >= 3:
            reasons.append("阻塞项偏多")
        if risk["developing"] + risk["testing"] > max(3, risk["delivered"]):
            reasons.append("开发/测试堆积高于已交付")
        if len(report["recent_titles"]) >= 2:
            reasons.append("近期新增需求持续流入")
        if report["age_hours"] is not None and report["age_hours"] > horizon_days * 24:
            reasons.append("最近更新偏旧")
        if not reasons:
            reasons.append("交付动作持续" if risk["delivered"] > 0 else "需要继续观察")
        record = {
            "group_title": report["group_title"],
            "chat_id": report["chat_id"],
            "file": report["file"],
            "latest_time": report["end_at"],
            "people_count": report["people_count"],
            "report_count": report["report_count"],
            "document_count": report["document_count"],
            "recent_event_count": report["recent_event_count"],
            "recent_titles": report["recent_titles"],
            "risk_stats": risk,
            "activity_score": round(activity_score, 1),
            "delay_score": round(delay_score, 1),
            "reason": "；".join(reasons[:3]),
        }
        focus_groups.append(record)
        risk_groups.append(record)
    focus_groups.sort(key=lambda item: (item["activity_score"], item["recent_event_count"], item["report_count"]), reverse=True)
    risk_groups.sort(key=lambda item: (item["delay_score"], item["risk_stats"]["blocked"], item["recent_event_count"]), reverse=True)
    return focus_groups[:5], [item for item in risk_groups if item["delay_score"] > 0][:5]


def build_people_signals(reports: list[dict[str, Any]], horizon_days: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = local_now()
    horizon_start = now - timedelta(days=max(7, horizon_days))
    people: dict[str, dict[str, Any]] = {}
    for report in reports:
        for card in report["people_cards"]:
            latest_dt = parse_dt(card.get("latestTime"))
            if latest_dt is None or latest_dt < horizon_start:
                continue
            name = normalize_name(str(card.get("reporterName") or "").strip())
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
                    "latest_time": latest_dt,
                    "groups": set(),
                },
            )
            item["report_count"] += int(card.get("reportCount") or 0)
            item["document_count"] += int(card.get("documentCount") or 0)
            status_mix = card.get("statusMix") or {}
            item["delivered"] += int(status_mix.get("delivered") or 0)
            item["testing"] += int(status_mix.get("testing") or 0)
            item["developing"] += int(status_mix.get("developing") or 0)
            item["review"] += int(status_mix.get("review") or 0)
            item["blocked"] += int(status_mix.get("blocked") or 0)
            item["latest_time"] = max(item["latest_time"], latest_dt)
            item["groups"].add(report["group_title"])
    output_rank: list[dict[str, Any]] = []
    attention_rank: list[dict[str, Any]] = []
    for person in people.values():
        recency_bonus = max(0.0, 3.0 - min(3.0, (now - person["latest_time"]).total_seconds() / 86400))
        group_bonus = min(2.0, len(person["groups"]) * 0.6)
        output_score = person["report_count"] * 0.7 + person["document_count"] * 1.3 + person["delivered"] * 2.0 + person["testing"] * 0.5 + group_bonus + recency_bonus - person["blocked"] * 0.4
        attention_score = person["blocked"] * 2.0 + person["developing"] * 1.3 + person["testing"] * 0.7 - person["delivered"] * 0.25
        reasons: list[str] = []
        if person["delivered"] >= 3:
            reasons.append("交付回执较多")
        if len(person["groups"]) >= 2:
            reasons.append("跨线串联较多")
        if person["blocked"] >= 3:
            reasons.append("阻塞项偏多")
        if person["developing"] + person["testing"] >= max(4, person["delivered"] + 2):
            reasons.append("在研/测试堆积偏高")
        if not reasons:
            reasons.append("样本正常，继续观察")
        record = {
            "name": person["name"],
            "report_count": person["report_count"],
            "document_count": person["document_count"],
            "delivered": person["delivered"],
            "testing": person["testing"],
            "developing": person["developing"],
            "blocked": person["blocked"],
            "group_count": len(person["groups"]),
            "groups": sorted(person["groups"]),
            "latest_time": to_iso(person["latest_time"]),
            "output_score": round(output_score, 1),
            "attention_score": round(attention_score, 1),
            "reason": "；".join(reasons[:3]),
        }
        output_rank.append(record)
        if person["report_count"] + person["document_count"] >= 2:
            attention_rank.append(record)
    output_rank.sort(key=lambda item: (item["output_score"], item["delivered"], item["group_count"]), reverse=True)
    attention_rank.sort(key=lambda item: (item["attention_score"], item["blocked"], item["developing"]), reverse=True)
    return output_rank[:5], [item for item in attention_rank if item["attention_score"] > 0][:5]


def build_one_liner(period_label: str, coverage: dict[str, Any] | None, risk_groups: list[dict[str, Any]]) -> str:
    if coverage:
        required = coverage.get("summary", {}).get("required", {})
        if any(int(required.get(key, 0)) > 0 for key in ("missing", "stale", "unverified")):
            return f"{period_label}采集链路还没闭环，先补齐必管群覆盖，再看管理判断。"
        if int(coverage.get("discover_selected_count", 0) or 0) <= 0:
            return f"{period_label}discover 结果为空，当前无法证明群覆盖完整，本次不应对外发管理稿。"
        if int(coverage.get("summary", {}).get("required_collection_stale_count", 0) or 0) > 0:
            return f"{period_label}必管群还没有在 {COLLECTION_FRESH_HOURS} 小时内完成补采/校验，本次只能做内部草稿，不能对外发。"
    if risk_groups:
        names = "、".join(item["group_title"] for item in risk_groups[:2])
        return f"{period_label}采集面已可用，但 {names} 的新增事项和未闭环任务还在堆积，优先盯闭环。"
    return f"{period_label}采集链路和结构化摘要都处于可用状态，重点转向结果回收和人效排序。"


def build_digest_payload(period: str, coverage: dict[str, Any] | None, actions: list[dict[str, Any]], errors: list[str]) -> dict[str, Any]:
    period = period.lower().strip()
    period_label = "今日" if period == "daily" else "本周"
    horizon_days = 2 if period == "daily" else 7
    reports = load_structured_reports(coverage, horizon_days=horizon_days)
    focus_groups, risk_groups = build_group_focus(reports, horizon_days=horizon_days)
    top_people, attention_people = build_people_signals(reports, horizon_days=horizon_days)
    return {
        "generated_at": local_now().isoformat(),
        "period": period,
        "period_label": period_label,
        "one_liner": build_one_liner(period_label, coverage, risk_groups),
        "db_stats": load_db_stats(),
        "coverage": coverage or {},
        "automation_actions": actions,
        "automation_errors": errors,
        "focus_groups": focus_groups,
        "delay_risk_groups": risk_groups,
        "top_output_people": top_people,
        "attention_people": attention_people,
        "report_files_used": [item["file"] for item in reports],
        "ceo_assets": current_ceo_assets(),
    }


def render_markdown(payload: dict[str, Any]) -> str:
    period_label = "日常" if payload["period"] == "daily" else "周度"
    coverage_required = payload.get("coverage", {}).get("summary", {}).get("required", {})
    publish_guard = payload.get("publish_guard", {}) or {}
    guard_status = str(publish_guard.get("guard_status", "")).strip()
    guard_label = "通过" if guard_status == "pass" else ("审阅模式" if guard_status == "review_only" else "阻断")
    lines = [
        f"# BYDFI {period_label}运营分析",
        "",
        f"- 生成时间：{payload['generated_at']}",
        f"- 一句话：{payload['one_liner']}",
        f"- 数据库：消息 {payload['db_stats'].get('audit_records_count', 0)} 条，文档 {payload['db_stats'].get('source_documents_count', 0)} 份，采集批次 {payload['db_stats'].get('collection_runs_count', 0)} 次",
        f"- 采集覆盖：必管群 covered={coverage_required.get('covered', 0)} / missing={coverage_required.get('missing', 0)} / stale={coverage_required.get('stale', 0)} / unverified={coverage_required.get('unverified', 0)}",
        f"- 发送门槛：{guard_label}",
        "",
        "## 必看业务线",
        "",
        "| 群/线 | 最新时间 | 为什么要看 | 关键数字 |",
        "| --- | --- | --- | --- |",
    ]
    for item in payload.get("focus_groups", []):
        numbers = item["risk_stats"]
        lines.append(f"| {item['group_title']} | {item['latest_time'] or '-'} | {item['reason']} | delivered={numbers['delivered']} / testing={numbers['testing']} / developing={numbers['developing']} / blocked={numbers['blocked']} |")
    lines.extend(["", "## 延期与闭环风险预警", "", "| 群/线 | 风险分 | 预警原因 | 近期触发项 |", "| --- | --- | --- | --- |"])
    for item in payload.get("delay_risk_groups", []):
        trigger_text = "；".join(item.get("recent_titles", [])) or "-"
        lines.append(f"| {item['group_title']} | {item['delay_score']} | {item['reason']} | {trigger_text} |")
    lines.extend(["", "## 输出信号靠前", "", "| 人 | 输出分 | 依据 | 涉及群 |", "| --- | --- | --- | --- |"])
    for item in payload.get("top_output_people", []):
        lines.append(f"| {item['name']} | {item['output_score']} | {item['reason']} | {'；'.join(item['groups'])} |")
    lines.extend(["", "## 风险关注候选", "", "| 人 | 关注分 | 依据 | 涉及群 |", "| --- | --- | --- | --- |"])
    for item in payload.get("attention_people", []):
        lines.append(f"| {item['name']} | {item['attention_score']} | {item['reason']} | {'；'.join(item['groups'])} |")
    if publish_guard:
        lines.extend(["", "## 发送门槛", ""])
        if guard_status == "pass":
            lines.append("- 当前满足发送门槛，可对外推送。")
        elif guard_status == "review_only":
            lines.append("- 当前覆盖与时效检查通过，但缺少同日人工 CEO 稿，本次仅适合先发给你审阅。")
            for note in publish_guard.get("review_notes", []):
                lines.append(f"- {note}")
        else:
            for reason in publish_guard.get("hard_reasons", []) or publish_guard.get("reasons", []):
                lines.append(f"- {reason}")
    if payload.get("automation_actions"):
        lines.extend(["", "## 自动化动作回执", ""])
        for action in payload["automation_actions"]:
            lines.append(f"- {action['step']}：{'成功' if action.get('ok') else '失败'}（returncode={action.get('returncode')}）")
    if payload.get("automation_errors"):
        lines.extend(["", "## 当前缺口", ""])
        for error in payload["automation_errors"]:
            lines.append(f"- {error}")
    ceo_assets = payload.get("ceo_assets", {})
    lines.extend(["", "## 相关产物", "", f"- 当前 CEO 源稿：`{ceo_assets.get('md_path', '')}`", f"- 当前 CEO PDF：`{ceo_assets.get('pdf_path', '')}`"])
    return "\n".join(lines) + "\n"


def write_digest(period: str, payload: dict[str, Any]) -> dict[str, str]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = local_now().strftime("%Y%m%d_%H%M%S")
    json_path = OUTPUT_DIR / f"{period}_ops_digest_{stamp}.json"
    md_path = OUTPUT_DIR / f"{period}_ops_digest_{stamp}.md"
    latest_json = OUTPUT_DIR / f"{period}_ops_digest_latest.json"
    latest_md = OUTPUT_DIR / f"{period}_ops_digest_latest.md"
    markdown = render_markdown(payload)
    write_json(json_path, payload)
    write_text(md_path, markdown)
    write_json(latest_json, payload)
    write_text(latest_md, markdown)
    return {
        "json_path": str(json_path.resolve()),
        "md_path": str(md_path.resolve()),
        "latest_json_path": str(latest_json.resolve()),
        "latest_md_path": str(latest_md.resolve()),
    }


def validate_and_render_ceo(period: str, payload: dict[str, Any]) -> dict[str, Any]:
    return render_ops_ceo_brief(period, payload=payload)


def latest_digest(period: str) -> dict[str, Any] | None:
    path = OUTPUT_DIR / f"{period}_ops_digest_latest.json"
    if not path.exists():
        return None
    try:
        return read_json(path)
    except Exception:
        return None


def run_ops_cycle(
    period: str,
    *,
    run_discover: bool,
    auto_collect: bool,
    render_ceo: bool,
    render_digest_pdf_file: bool,
    deliver: bool,
    stale_hours: int,
) -> dict[str, Any]:
    period_key = str(period).strip().lower()
    coverage, actions, errors = refresh_coverage(period=period_key, run_discover=run_discover, auto_collect=auto_collect, stale_hours=stale_hours)
    payload = build_digest_payload(period=period, coverage=coverage, actions=actions, errors=errors)
    render_result = validate_and_render_ceo(period, payload) if (render_ceo or deliver) else None
    publish_guard = assess_publish_guard(period_key, payload, run_discover=run_discover, render_result=render_result)
    payload["publish_guard"] = publish_guard
    output_paths = write_digest(period, payload)
    digest_pdf_result = render_digest_pdf(period, payload=payload) if (render_digest_pdf_file or deliver) else None
    delivery_result = None
    if deliver:
        try:
            delivery_result = deliver_digest_report(period, payload, digest_pdf_result or {}, render_result or {})
            if not publish_guard.get("review_deliverable", False):
                delivery_result["blocked"] = True
                delivery_result["guard_status"] = publish_guard["guard_status"]
                delivery_result["reasons"] = publish_guard.get("hard_reasons", []) or publish_guard["reasons"]
        except Exception as exc:
            errors.append(f"delivery failed: {exc}")
    return {
        "ok": publish_guard.get("review_deliverable", False) and not errors,
        "period": period,
        "generated_at": payload["generated_at"],
        "one_liner": payload["one_liner"],
        "coverage_required": (coverage or {}).get("summary", {}).get("required", {}),
        "publish_guard": publish_guard,
        "json_path": output_paths["json_path"],
        "md_path": output_paths["md_path"],
        "latest_json_path": output_paths["latest_json_path"],
        "latest_md_path": output_paths["latest_md_path"],
        "automation_errors": errors,
        "digest_pdf": digest_pdf_result,
        "delivery": delivery_result,
        "ceo_render": render_result,
    }
