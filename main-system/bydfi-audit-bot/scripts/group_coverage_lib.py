from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_PATH = ROOT / "config" / "lark_group_registry.json"
DEFAULT_DB_PATH = ROOT / "data" / "audit_records.sqlite3"
DEFAULT_DISCOVER_OUTPUT = ROOT / "output" / "lark_discover_latest.json"
DEFAULT_EXTERNAL_COLLECTOR = ROOT.parents[1] / "lark-collector" / "external_group_collector.py"
DEFAULT_STORAGE_STATE = ROOT.parents[1] / "lark-collector" / "data" / "playwright" / "lark_storage_state.json"
SIBLING_COLLECTOR_STORAGE_STATE = ROOT.parents[1] / "lark-collector" / "data" / "playwright" / "lark_storage_state.json"
REPO_LOCAL_STORAGE_STATE = ROOT / "data" / "playwright" / "lark_storage_state.json"
DEFAULT_STALE_HOURS = 48
DEFAULT_COLLECTION_FRESH_HOURS = 4
LOCAL_TZ = timezone(timedelta(hours=8))
ENV_EXTERNAL_COLLECTOR_PATH = "BYDFI_EXTERNAL_COLLECTOR_PATH"
ENV_STORAGE_STATE_PATH = "BYDFI_LARK_STORAGE_STATE_PATH"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso8601(raw: str | None) -> datetime | None:
    if not raw:
        return None
    value = str(raw).strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_discover_time_hint(raw: str | None, *, now: datetime) -> datetime | None:
    if not raw:
        return None
    value = str(raw).strip()
    if not value:
        return None
    local_now = now.astimezone(LOCAL_TZ)
    if ":" in value and len(value) <= 5:
        try:
            hour_text, minute_text = value.split(":", 1)
            hinted = local_now.replace(hour=int(hour_text), minute=int(minute_text), second=0, microsecond=0)
            return hinted.astimezone(timezone.utc)
        except Exception:
            return None
    if value == "昨天":
        return (local_now - timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0).astimezone(timezone.utc)
    if value == "前天":
        return (local_now - timedelta(days=2)).replace(hour=23, minute=59, second=0, microsecond=0).astimezone(timezone.utc)
    if "月" in value and "日" in value:
        try:
            month_text, rest = value.split("月", 1)
            day_text = rest.replace("日", "")
            hinted = datetime(local_now.year, int(month_text), int(day_text), 23, 59, tzinfo=LOCAL_TZ)
            return hinted.astimezone(timezone.utc)
        except Exception:
            return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            hinted = datetime.strptime(value, fmt).replace(hour=23, minute=59, tzinfo=LOCAL_TZ)
            return hinted.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_registry(path: Path | None = None) -> dict[str, Any]:
    target = path or DEFAULT_REGISTRY_PATH
    payload = load_json(target)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid registry payload: {target}")
    payload.setdefault("groups", [])
    return payload


def _candidate_path(raw: str | Path | None) -> Path | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return Path(text).expanduser()


def _prefer_existing(*candidates: str | Path | None) -> Path:
    existing: list[Path] = []
    fallback: list[Path] = []
    for candidate in candidates:
        path = _candidate_path(candidate)
        if path is None:
            continue
        fallback.append(path)
        if path.exists():
            existing.append(path)
    selected = existing[0] if existing else fallback[0]
    return selected.resolve(strict=False)


def resolve_external_collector_path(
    registry: dict[str, Any] | None = None,
    explicit: str | Path | None = None,
) -> Path:
    collector_cfg = (registry or {}).get("collector", {})
    return _prefer_existing(
        explicit,
        os.environ.get(ENV_EXTERNAL_COLLECTOR_PATH),
        collector_cfg.get("external_collector_path"),
        DEFAULT_EXTERNAL_COLLECTOR,
    )


def resolve_storage_state_path(
    registry: dict[str, Any] | None = None,
    explicit: str | Path | None = None,
) -> Path:
    collector_cfg = (registry or {}).get("collector", {})
    return _prefer_existing(
        explicit,
        os.environ.get(ENV_STORAGE_STATE_PATH),
        collector_cfg.get("storage_state_path"),
        SIBLING_COLLECTOR_STORAGE_STATE,
        REPO_LOCAL_STORAGE_STATE,
        DEFAULT_STORAGE_STATE,
    )


def registry_title_set(registry: dict[str, Any]) -> set[str]:
    titles: set[str] = set()
    for entry in registry.get("groups", []):
        title = str(entry.get("title", "")).strip()
        if title:
            titles.add(title)
        for alias in entry.get("aliases", []):
            alias_value = str(alias).strip()
            if alias_value:
                titles.add(alias_value)
    return titles


def is_suspicious_discovery_title(title: str) -> bool:
    cleaned = str(title).strip()
    if not cleaned:
        return False
    if len(cleaned) >= 80:
        return True
    if ("\n" in cleaned or "\r" in cleaned) and len(cleaned) >= 40:
        return True
    if (":" in cleaned or "：" in cleaned) and len(cleaned) >= 60:
        return True
    return False


def run_discovery(
    *,
    external_collector_path: Path,
    storage_state_path: Path,
    db_path: Path,
    output_path: Path,
    required_titles: list[str] | None = None,
    search_terms: list[str] | None = None,
    search_exhaustive: bool = True,
    search_limit: int = 400,
    scroll_iterations: int = 18,
) -> dict[str, Any]:
    cached_payload = load_json(output_path) if output_path.exists() else None

    def execute(*, target_output: Path, extra_terms: list[str] | None, exhaustive: bool) -> dict[str, Any]:
        command = [
            sys.executable,
            "-X",
            "utf8",
            str(external_collector_path),
            "discover",
            "--storage-state",
            str(storage_state_path),
            "--search-limit",
            str(search_limit),
            "--scroll-iterations",
            str(scroll_iterations),
            "--inspect-output",
            str(target_output),
        ]
        if exhaustive:
            command.append("--search-exhaustive")
        for term in extra_terms or []:
            cleaned = str(term).strip()
            if cleaned:
                command.extend(["--search-term", cleaned])

        env = os.environ.copy()
        env["AUDIT_DB_PATH"] = str(db_path)
        env["DATA_DIR"] = str(ROOT / "data")
        env["REPORTS_DIR"] = str(ROOT / "data" / "reports")

        proc = subprocess.run(
            command,
            cwd=str(external_collector_path.parent),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                "Discovery failed.\n"
                f"stdout:\n{proc.stdout.strip()}\n"
                f"stderr:\n{proc.stderr.strip()}"
            )
        if target_output.exists():
            payload = load_json(target_output)
        else:
            stdout = proc.stdout.strip()
            if not stdout:
                raise RuntimeError("Discovery completed without JSON output.")
            payload = json.loads(stdout)
        if int(payload.get("discovered_count", 0) or 0) <= 0 or int(payload.get("selected_count", 0) or 0) <= 0:
            raise RuntimeError(f"Discovery returned no visible chats: {payload}")
        return payload

    def merge_payloads(primary: dict[str, Any], secondary: dict[str, Any] | None) -> dict[str, Any]:
        if not secondary:
            return primary
        merged_chats: list[dict[str, Any]] = []
        seen_feed_ids: set[str] = set()
        seen_titles: set[str] = set()
        for source in (primary, secondary):
            for item in source.get("chats", []):
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                feed_id = str(item.get("feed_id") or item.get("feedId") or "").strip()
                if not title and not feed_id:
                    continue
                unique_key = feed_id or title
                if unique_key in seen_feed_ids or title in seen_titles:
                    continue
                seen_feed_ids.add(unique_key)
                if title:
                    seen_titles.add(title)
                merged_chats.append(item)
        merged = dict(primary)
        merged["chats"] = merged_chats
        merged["selected_count"] = len(merged_chats)
        merged["discovered_count"] = max(
            int(primary.get("discovered_count", 0) or 0),
            int(secondary.get("discovered_count", 0) or 0),
            len(merged_chats),
        )
        return merged

    basic_output = output_path.with_name(f"{output_path.stem}_basic{output_path.suffix}")
    basic_payload: dict[str, Any] | None = None
    basic_error: RuntimeError | None = None
    for _ in range(2):
        try:
            basic_payload = execute(target_output=basic_output, extra_terms=[], exhaustive=False)
            basic_error = None
            break
        except RuntimeError as exc:
            basic_error = exc

    normalized_required = [str(title).strip() for title in (required_titles or []) if str(title).strip()]
    if basic_payload is not None:
        basic_payload = merge_payloads(basic_payload, cached_payload)
        selected_titles = {
            str(item.get("title", "")).strip()
            for item in basic_payload.get("chats", [])
            if str(item.get("title", "")).strip()
        }
        missing_required = [title for title in normalized_required if title not in selected_titles]
        if not missing_required:
            write_json(output_path, basic_payload)
            return basic_payload
        if not search_exhaustive and not any(str(term).strip() for term in (search_terms or [])):
            write_json(output_path, basic_payload)
            return basic_payload
    else:
        raise basic_error or RuntimeError("Discovery failed without a usable basic result.")

    try:
        expanded_terms = list(dict.fromkeys([*missing_required, *(search_terms or [])]))
        expanded_payload = execute(
            target_output=output_path,
            extra_terms=expanded_terms,
            exhaustive=search_exhaustive,
        )
        expanded_payload = merge_payloads(expanded_payload, basic_payload)
        expanded_payload = merge_payloads(expanded_payload, cached_payload)
        return expanded_payload
    except RuntimeError:
        if basic_payload is not None:
            write_json(output_path, basic_payload)
            return basic_payload
        if basic_error is not None:
            raise basic_error
        raise


def _query_group_rows(conn: sqlite3.Connection, titles: list[str]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in titles)
    sql = f"""
        SELECT
            chat_id,
            COUNT(*) AS message_count,
            MIN(COALESCE(message_timestamp, created_at)) AS oldest_message_timestamp,
            MAX(COALESCE(message_timestamp, created_at)) AS latest_message_timestamp
        FROM audit_records
        WHERE json_extract(raw_message_json, '$.current_chat_name') IN ({placeholders})
           OR json_extract(raw_message_json, '$.chat_name') IN ({placeholders})
        GROUP BY chat_id
        ORDER BY MAX(COALESCE(message_timestamp, created_at)) DESC, COUNT(*) DESC
    """
    params = titles + titles
    return conn.execute(sql, params).fetchall()


def _query_latest_run(conn: sqlite3.Connection, chat_ids: list[str]) -> sqlite3.Row | None:
    if not chat_ids:
        return None
    placeholders = ",".join("?" for _ in chat_ids)
    sql = f"""
        SELECT
            run_id,
            chat_id,
            started_at,
            finished_at,
            status,
            error_message
        FROM collection_runs
        WHERE chat_id IN ({placeholders})
        ORDER BY COALESCE(finished_at, started_at) DESC
        LIMIT 1
    """
    return conn.execute(sql, chat_ids).fetchone()


def build_coverage_report(
    *,
    registry: dict[str, Any],
    db_path: Path,
    discover_payload: dict[str, Any] | None = None,
    stale_hours: int = DEFAULT_STALE_HOURS,
    collection_fresh_hours: int = DEFAULT_COLLECTION_FRESH_HOURS,
) -> dict[str, Any]:
    now = utc_now()
    discover_items: dict[str, dict[str, Any]] = {}
    for item in (discover_payload or {}).get("chats", []):
        title = str(item.get("title", "")).strip()
        if title and title not in discover_items:
            discover_items[title] = item
    discover_titles = set(discover_items)
    registry_titles = registry_title_set(registry)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        groups: list[dict[str, Any]] = []
        required_counts = {"covered": 0, "missing": 0, "stale": 0, "unverified": 0}
        total_counts = {"covered": 0, "missing": 0, "stale": 0, "unverified": 0}
        required_collection_stale = 0
        total_collection_stale = 0

        for entry in registry.get("groups", []):
            title = str(entry.get("title", "")).strip()
            if not title:
                continue
            aliases = [str(item).strip() for item in entry.get("aliases", []) if str(item).strip()]
            candidate_titles = [title, *aliases]
            rows = _query_group_rows(conn, candidate_titles)
            chat_ids = [str(row["chat_id"]) for row in rows if str(row["chat_id"]).strip()]
            latest_message_dt = None
            oldest_message_dt = None
            latest_message_raw = ""
            oldest_message_raw = ""
            total_messages = 0

            for row in rows:
                total_messages += int(row["message_count"] or 0)
                row_latest = parse_iso8601(row["latest_message_timestamp"])
                row_oldest = parse_iso8601(row["oldest_message_timestamp"])
                if row_latest and (latest_message_dt is None or row_latest > latest_message_dt):
                    latest_message_dt = row_latest
                    latest_message_raw = str(row["latest_message_timestamp"] or "")
                if row_oldest and (oldest_message_dt is None or row_oldest < oldest_message_dt):
                    oldest_message_dt = row_oldest
                    oldest_message_raw = str(row["oldest_message_timestamp"] or "")

            latest_age_hours = None
            if latest_message_dt is not None:
                latest_age_hours = round((now - latest_message_dt).total_seconds() / 3600, 1)

            discover_item = next((discover_items[candidate] for candidate in candidate_titles if candidate in discover_items), None)
            discover_seen = discover_item is not None
            discover_time_hint = str((discover_item or {}).get("time_hint", "")).strip()
            discover_hint_dt = parse_discover_time_hint(discover_time_hint, now=now)
            discover_recent_activity = False
            if discover_hint_dt is not None:
                discover_recent_activity = (now - discover_hint_dt).total_seconds() <= collection_fresh_hours * 3600
            last_run = _query_latest_run(conn, chat_ids)
            last_collection_finished_raw = ""
            last_collection_age_hours = None
            collection_fresh = False
            if last_run is not None:
                last_collection_finished_raw = str(last_run["finished_at"] or last_run["started_at"] or "")
                last_collection_finished_dt = parse_iso8601(last_collection_finished_raw)
                if last_collection_finished_dt is not None:
                    last_collection_age_hours = round((now - last_collection_finished_dt).total_seconds() / 3600, 1)
                    collection_fresh = last_collection_age_hours <= collection_fresh_hours and str(last_run["status"] or "") == "completed"
            if not collection_fresh and discover_seen and not discover_recent_activity:
                collection_fresh = True
            if total_messages == 0:
                status = "missing" if discover_seen else "unverified"
            elif latest_age_hours is not None and latest_age_hours > stale_hours:
                status = "stale"
            else:
                status = "covered"

            total_counts[status] += 1
            if bool(entry.get("required", True)):
                required_counts[status] += 1
            if not collection_fresh:
                total_collection_stale += 1
                if bool(entry.get("required", True)):
                    required_collection_stale += 1

            groups.append(
                {
                    "title": title,
                    "aliases": aliases,
                    "category": str(entry.get("category", "")).strip(),
                    "required": bool(entry.get("required", True)),
                    "priority": str(entry.get("priority", "")).strip(),
                    "notes": str(entry.get("notes", "")).strip(),
                    "status": status,
                    "discover_seen": discover_seen,
                    "discover_time_hint": discover_time_hint,
                    "discover_recent_activity": discover_recent_activity,
                    "message_count": total_messages,
                    "chat_ids": chat_ids,
                    "oldest_message_timestamp": oldest_message_raw,
                    "latest_message_timestamp": latest_message_raw,
                    "latest_age_hours": latest_age_hours,
                    "last_collection_finished_at": last_collection_finished_raw,
                    "last_collection_age_hours": last_collection_age_hours,
                    "collection_fresh": collection_fresh,
                    "last_collection_run": (
                        {
                            "run_id": str(last_run["run_id"]),
                            "chat_id": str(last_run["chat_id"]),
                            "started_at": str(last_run["started_at"] or ""),
                            "finished_at": str(last_run["finished_at"] or ""),
                            "status": str(last_run["status"] or ""),
                            "error_message": str(last_run["error_message"] or ""),
                        }
                        if last_run is not None
                        else None
                    ),
                }
            )
    finally:
        conn.close()

    groups.sort(key=lambda item: (item["status"], item["priority"], item["title"]))
    unregistered_discovered = []
    suspicious_discovered = []
    for item in (discover_payload or {}).get("chats", []):
        title = str(item.get("title", "")).strip()
        if not title or title in registry_titles:
            continue
        target = suspicious_discovered if is_suspicious_discovery_title(title) else unregistered_discovered
        target.append(
            {
                "title": title,
                "badge": str(item.get("badge", "")).strip(),
                "time_hint": str(item.get("time_hint", "")).strip(),
                "preview_text": str(item.get("preview_text", "")).strip(),
            }
        )
    unregistered_discovered.sort(key=lambda item: item["title"])
    suspicious_discovered.sort(key=lambda item: item["title"])

    return {
        "generated_at": now.isoformat(),
        "db_path": str(db_path),
        "stale_hours": stale_hours,
        "collection_fresh_hours": collection_fresh_hours,
        "registry_count": len(groups),
        "discover_selected_count": len((discover_payload or {}).get("chats", [])),
        "summary": {
            "required": required_counts,
            "all": total_counts,
            "required_missing_titles": [item["title"] for item in groups if item["required"] and item["status"] == "missing"],
            "required_stale_titles": [item["title"] for item in groups if item["required"] and item["status"] == "stale"],
            "required_unverified_titles": [item["title"] for item in groups if item["required"] and item["status"] == "unverified"],
            "required_collection_stale_count": required_collection_stale,
            "all_collection_stale_count": total_collection_stale,
            "required_collection_stale_titles": [item["title"] for item in groups if item["required"] and not item["collection_fresh"]],
            "unregistered_discovered_count": len(unregistered_discovered),
            "suspicious_discovered_count": len(suspicious_discovered),
        },
        "unregistered_discovered": unregistered_discovered,
        "suspicious_discovered": suspicious_discovered,
        "groups": groups,
    }


def format_human_summary(report: dict[str, Any]) -> str:
    lines = [
        f"generated_at={report['generated_at']}",
        f"required.covered={report['summary']['required']['covered']}",
        f"required.missing={report['summary']['required']['missing']}",
        f"required.stale={report['summary']['required']['stale']}",
        f"required.unverified={report['summary']['required']['unverified']}",
        "",
    ]
    for item in report.get("groups", []):
        lines.append(
            " | ".join(
                [
                    item["status"],
                    item["title"],
                    item.get("category", ""),
                    f"discover_seen={item['discover_seen']}",
                    f"messages={item['message_count']}",
                    f"latest={item['latest_message_timestamp'] or '-'}",
                ]
            )
        )
    if report.get("unregistered_discovered"):
        lines.append("")
        lines.append("unregistered_discovered:")
        for item in report["unregistered_discovered"]:
            lines.append(f"- {item['title']} | {item['time_hint']} | {item['preview_text'][:80]}")
    if report.get("suspicious_discovered"):
        lines.append("")
        lines.append("suspicious_discovered:")
        for item in report["suspicious_discovered"]:
            lines.append(f"- {item['title'][:80]} | {item['time_hint']} | {item['preview_text'][:80]}")
    return "\n".join(lines)
