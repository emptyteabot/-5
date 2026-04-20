from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WATCH_DIR = ROOT / "output" / "watchers"
STATE_PATH = WATCH_DIR / "bloodbattle_weekly_trigger_state.json"
COLLECT_SCRIPT = ROOT / "scripts" / "collect_registered_groups.py"
WEEKLY_SCRIPT = ROOT / "run_weekly_ops_cycle.py"
DB_PATH = ROOT / "data" / "audit_records.sqlite3"


def local_now() -> datetime:
    return datetime.now().astimezone()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_subprocess(args: list[str], timeout_seconds: int = 900) -> dict[str, Any]:
    proc = subprocess.run(
        args,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_seconds,
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "command": args,
    }


def parse_collect_metrics(stdout: str) -> dict[str, Any]:
    patterns = {
        "run_id": r"Run ID:\s*([^\s]+)",
        "chat_id": r"Chat ID:\s*([^\s]+)",
        "current_chat": r"Current chat:\s*(.+)",
        "ingested": r"Ingested:\s*(\d+)",
        "new_messages": r"New messages:\s*(\d+)",
        "updated_messages": r"Updated messages:\s*(\d+)",
        "fetched_documents": r"Fetched documents:\s*(\d+)",
    }
    result: dict[str, Any] = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, stdout or "")
        if not match:
            continue
        value = match.group(1).strip()
        if key in {"ingested", "new_messages", "updated_messages", "fetched_documents"}:
            result[key] = int(value)
        else:
            result[key] = value
    return result


def run_target_collect(group_title: str, refresh_hours: int) -> dict[str, Any]:
    output_path = WATCH_DIR / f"collect_{local_now().strftime('%Y%m%d_%H%M%S')}.json"
    args = [
        sys.executable,
        "-X",
        "utf8",
        str(COLLECT_SCRIPT),
        "--title",
        group_title,
        "--refresh-hours",
        str(refresh_hours),
        "--skip-summarize",
        "--write-json",
        str(output_path),
    ]
    result = run_subprocess(args, timeout_seconds=1200)
    payload: dict[str, Any] = {}
    if output_path.exists():
        try:
            payload = read_json(output_path)
        except Exception:
            payload = {}
    runs = payload.get("runs", []) or []
    run_item = runs[0] if runs else {}
    metrics = parse_collect_metrics(str(run_item.get("stdout", "")))
    return {
        "result": result,
        "payload_path": str(output_path.resolve()),
        "payload": payload,
        "run": run_item,
        "metrics": metrics,
    }


def latest_marker(chat_id: str | None) -> dict[str, Any]:
    if not chat_id or not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        row = cur.execute(
            """
            select id, message_id, message_timestamp, reporter_name, source_title
            from audit_records
            where chat_id = ?
            order by message_timestamp desc, id desc
            limit 1
            """,
            (chat_id,),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return read_json(path)
    except Exception:
        return {}


def should_trigger(*, state: dict[str, Any], marker: dict[str, Any], metrics: dict[str, Any], force_deliver: bool) -> tuple[bool, str]:
    if force_deliver:
        return True, "force_deliver"
    if not state:
        return False, "bootstrap_baseline"
    if int(metrics.get("new_messages", 0) or 0) <= 0:
        return False, "no_new_messages"

    current_message_id = str(marker.get("message_id") or "").strip()
    last_message_id = str(state.get("last_message_id") or "").strip()
    if current_message_id and current_message_id != last_message_id:
        return True, "new_message_id"

    current_row_id = int(marker.get("id") or 0)
    last_row_id = int(state.get("last_row_id") or 0)
    if current_row_id and current_row_id > last_row_id:
        return True, "new_row_id"

    current_ts = str(marker.get("message_timestamp") or "").strip()
    last_ts = str(state.get("last_message_timestamp") or "").strip()
    if current_ts and current_ts != last_ts:
        return True, "new_message_timestamp"

    return False, "state_already_seen"


def run_weekly_delivery() -> dict[str, Any]:
    args = [
        sys.executable,
        "-X",
        "utf8",
        str(WEEKLY_SCRIPT),
        "--render-digest-pdf",
        "--deliver",
    ]
    return run_subprocess(args, timeout_seconds=1800)


def build_state(*, group_title: str, chat_id: str, marker: dict[str, Any], metrics: dict[str, Any], last_delivery: str = "") -> dict[str, Any]:
    return {
        "group_title": group_title,
        "chat_id": chat_id,
        "last_checked_at": local_now().isoformat(),
        "last_message_id": str(marker.get("message_id") or "").strip(),
        "last_row_id": int(marker.get("id") or 0),
        "last_message_timestamp": str(marker.get("message_timestamp") or "").strip(),
        "last_reporter_name": str(marker.get("reporter_name") or "").strip(),
        "last_source_title": str(marker.get("source_title") or "").strip(),
        "last_metrics": metrics,
        "last_delivery_at": last_delivery,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Trigger BYDFI weekly report delivery when 血战到底 receives a new message.")
    parser.add_argument("--group-title", default="血战到底")
    parser.add_argument("--refresh-hours", type=int, default=72)
    parser.add_argument("--trigger-weekday", type=int, default=4, help="Python weekday number. Friday is 4.")
    parser.add_argument("--state-path", default=str(STATE_PATH))
    parser.add_argument("--force-deliver", action="store_true")
    args = parser.parse_args()

    now = local_now()
    state_path = Path(args.state_path).expanduser().resolve()
    state = load_state(state_path)

    if not args.force_deliver and now.weekday() != args.trigger_weekday:
        payload = {
            "ok": True,
            "triggered": False,
            "reason": f"weekday_mismatch:{now.weekday()}",
            "now": now.isoformat(),
            "expected_weekday": args.trigger_weekday,
            "state_path": str(state_path),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    collect = run_target_collect(args.group_title, args.refresh_hours)
    metrics = collect.get("metrics", {}) or {}
    chat_id = str(metrics.get("chat_id") or state.get("chat_id") or "").strip()
    marker = latest_marker(chat_id)
    trigger, reason = should_trigger(state=state, marker=marker, metrics=metrics, force_deliver=args.force_deliver)

    delivery = None
    if trigger:
        delivery = run_weekly_delivery()

    new_state = build_state(
        group_title=args.group_title,
        chat_id=chat_id,
        marker=marker,
        metrics=metrics,
        last_delivery=now.isoformat() if trigger else str(state.get("last_delivery_at") or ""),
    )
    write_json(state_path, new_state)

    payload = {
        "ok": bool(collect["result"].get("ok")) and (delivery is None or delivery.get("ok", False)),
        "triggered": trigger,
        "reason": reason,
        "now": now.isoformat(),
        "group_title": args.group_title,
        "state_path": str(state_path),
        "collect": {
            "ok": collect["result"].get("ok"),
            "payload_path": collect["payload_path"],
            "metrics": metrics,
            "stderr": collect["result"].get("stderr", "")[:800],
        },
        "latest_marker": marker,
        "delivery": delivery,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
