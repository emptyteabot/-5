from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"
DOCS_DIR = ROOT / "docs"
CONFIG_DIR = ROOT / "config"
SCRIPTS_DIR = ROOT / "scripts"
SKILLS_DIR = Path.home() / ".codex" / "skills"
CEO_SKILL_DIR = SKILLS_DIR / "bydfi-ceo-brief"
OPS_SKILL_DIR = SKILLS_DIR / "bydfi-exchange-ops"
VERIFY_SCRIPT = CEO_SKILL_DIR / "scripts" / "verify_ceo_brief.py"
RENDER_SCRIPT = ROOT / "generate_ceo_brief_pdf.py"
GROUP_REGISTRY_PATH = CONFIG_DIR / "lark_group_registry.json"
GROUP_COVERAGE_SCRIPT = SCRIPTS_DIR / "check_group_coverage.py"
DAILY_OPS_SCRIPT = ROOT / "run_daily_ops_cycle.py"
WEEKLY_OPS_SCRIPT = ROOT / "run_weekly_ops_cycle.py"
SCHEDULER_DOC = DOCS_DIR / "exchange-internship-scheduler-20260407.md"
DESKTOP_DIRS = [Path.home() / "Desktop", Path(r"E:\UserData\cyh\Desktop")]
DB_CANDIDATES = [
    ROOT / "tmp_desktop_audit_records.sqlite3",
    ROOT / "data" / "audit_records.sqlite3",
]
PROTOCOL_VERSION = "2025-11-25"


TOOLS: list[dict[str, Any]] = [
    {
        "name": "archive.summary",
        "description": "Return the main archive documents, skills, MCP entrypoint, and latest CEO brief assets for this BYDFI internship workspace.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "sources.list",
        "description": "List structured report sources under data/reports that contain people cards and evidence indexes.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "report.search",
        "description": "Search report markdown/json files for a keyword or regex pattern.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Keyword or regex to search for."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20},
                "file_glob": {"type": "string", "description": "Optional glob under data/reports, for example *.md or *.json."},
            },
            "required": ["pattern"],
            "additionalProperties": False,
        },
    },
    {
        "name": "requirements.search",
        "description": "Query audit_records for Kater requirements, screenshot notes, or historical manual guidance.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Keyword to match in source_title or parsed_text."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10},
            },
            "required": ["keyword"],
            "additionalProperties": False,
        },
    },
    {
        "name": "group.registry",
        "description": "Return the current Lark group registry used for completeness claims and backfill operations.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "group.coverage",
        "description": "Audit registered Lark groups against local SQLite coverage. Optionally rerun Lark discovery first.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_discover": {"type": "boolean", "default": False},
                "stale_hours": {"type": "integer", "minimum": 1, "maximum": 720, "default": 48}
            },
            "additionalProperties": False
        },
    },
    {
        "name": "ops.daily_run",
        "description": "Run the daily ops cycle: coverage audit, optional backfill, and daily internal digest generation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_discover": {"type": "boolean", "default": True},
                "auto_collect": {"type": "boolean", "default": True},
                "render_ceo": {"type": "boolean", "default": False},
                "stale_hours": {"type": "integer", "minimum": 1, "maximum": 720, "default": 48}
            },
            "additionalProperties": False
        },
    },
    {
        "name": "ops.weekly_run",
        "description": "Run the weekly ops cycle: coverage audit, optional backfill, and weekly internal digest generation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_discover": {"type": "boolean", "default": True},
                "auto_collect": {"type": "boolean", "default": True},
                "render_ceo": {"type": "boolean", "default": False},
                "stale_hours": {"type": "integer", "minimum": 1, "maximum": 720, "default": 48}
            },
            "additionalProperties": False
        },
    },
    {
        "name": "ops.digest_latest",
        "description": "Return the latest generated daily or weekly internal ops digest.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "period": {"type": "string", "enum": ["daily", "weekly"], "default": "daily"}
            },
            "additionalProperties": False
        },
    },
    {
        "name": "ceo_brief.validate",
        "description": "Run the existing CEO brief validator against a markdown file. Defaults to the latest ceo_brief_*.md file.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "md_path": {"type": "string", "description": "Optional absolute or repo-relative markdown path."}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "ceo_brief.render",
        "description": "Render the latest CEO brief PDF using the existing repo script and report repo/desktop output paths.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
]


def eprint(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def write_message(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def success_response(request_id: Any, result: dict[str, Any]) -> None:
    write_message({"jsonrpc": "2.0", "id": request_id, "result": result})


def error_response(request_id: Any, code: int, message: str) -> None:
    write_message({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}})


def tool_text_result(text: str, *, is_error: bool = False) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "isError": is_error}


def format_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def latest_ceo_brief_md() -> Path | None:
    files = sorted(REPORTS_DIR.glob("ceo_brief_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def latest_ceo_brief_pdf() -> Path | None:
    files = sorted(REPORTS_DIR.glob("ceo_brief_*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def desktop_ceo_pdfs() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for directory in DESKTOP_DIRS:
        if not directory.exists():
            continue
        for path in sorted(directory.glob("高层决策报告_*.pdf")):
            stat = path.stat()
            items.append(
                {
                    "path": str(path),
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                }
            )
    return items


def resolve_db_path() -> Path:
    for candidate in DB_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("No audit_records sqlite database found.")


def resolve_primary_db_path() -> Path:
    primary = ROOT / "data" / "audit_records.sqlite3"
    if primary.exists():
        return primary
    return resolve_db_path()


def resolve_md_path(raw_path: str | None) -> Path:
    if raw_path:
        path = Path(raw_path)
        if not path.is_absolute():
            path = ROOT / raw_path
        return path.resolve()
    latest = latest_ceo_brief_md()
    if latest is None:
        raise FileNotFoundError("No ceo_brief_*.md file found.")
    return latest.resolve()


def load_group_registry() -> dict[str, Any]:
    if not GROUP_REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Group registry not found: {GROUP_REGISTRY_PATH}")
    payload = json.loads(GROUP_REGISTRY_PATH.read_text(encoding="utf-8"))
    groups = []
    for item in payload.get("groups", []):
        groups.append(
            {
                "title": item.get("title", ""),
                "category": item.get("category", ""),
                "required": bool(item.get("required", True)),
                "priority": item.get("priority", ""),
                "notes": item.get("notes", ""),
            }
        )
    return {
        "path": str(GROUP_REGISTRY_PATH.resolve()),
        "version": payload.get("version", ""),
        "count": len(groups),
        "collector": payload.get("collector", {}),
        "groups": groups,
    }


def archive_summary() -> dict[str, Any]:
    return {
        "archive_doc": str((DOCS_DIR / "exchange-internship-archive-20260407.md").resolve()),
        "mcp_doc": str((DOCS_DIR / "exchange-internship-mcp-20260407.md").resolve()),
        "ceo_skill": str(CEO_SKILL_DIR.resolve()),
        "ops_skill": str(OPS_SKILL_DIR.resolve()),
        "mcp_server": str((ROOT / "mcp" / "bydfi_exchange_ops_server.py").resolve()),
        "scheduler_doc": str(SCHEDULER_DOC.resolve()) if SCHEDULER_DOC.exists() else None,
        "latest_ceo_brief_md": str(resolve_md_path(None)),
        "latest_ceo_brief_pdf": str(latest_ceo_brief_pdf().resolve()) if latest_ceo_brief_pdf() else None,
        "desktop_pdfs": desktop_ceo_pdfs(),
    }


def list_sources() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in sorted(REPORTS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        if "peopleCards" not in data or "evidenceIndex" not in data:
            continue
        items.append(
            {
                "file": path.name,
                "title": data.get("title", ""),
                "peopleCount": data.get("peopleCount"),
                "reportCount": data.get("reportCount"),
                "documentCount": data.get("documentCount"),
                "startAt": data.get("startAt"),
                "endAt": data.get("endAt"),
            }
        )
    return items


def search_reports(pattern: str, limit: int = 20, file_glob: str = "*") -> list[dict[str, Any]]:
    regex = re.compile(pattern, re.IGNORECASE)
    matches: list[dict[str, Any]] = []
    for path in sorted(REPORTS_DIR.glob(file_glob)):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for line_no, line in enumerate(text.splitlines(), 1):
            if regex.search(line):
                matches.append({"file": path.name, "line": line_no, "text": line.strip()})
                if len(matches) >= limit:
                    return matches
    return matches


def search_requirements(keyword: str, limit: int = 10) -> list[dict[str, Any]]:
    db_path = resolve_db_path()
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT id, reporter_name, source_title, source_type, substr(parsed_text, 1, 280)
            FROM audit_records
            WHERE source_title LIKE ? OR parsed_text LIKE ? OR reporter_name LIKE ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", limit),
        ).fetchall()
        return [
            {
                "id": row[0],
                "reporter_name": row[1],
                "source_title": row[2],
                "source_type": row[3],
                "preview": row[4],
            }
            for row in rows
        ]
    finally:
        conn.close()


def run_subprocess(args: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        args,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def group_coverage(run_discover: bool = False, stale_hours: int = 48) -> dict[str, Any]:
    if not GROUP_COVERAGE_SCRIPT.exists():
        raise FileNotFoundError(f"Coverage script not found: {GROUP_COVERAGE_SCRIPT}")
    db_path = resolve_primary_db_path()
    coverage_path = ROOT / "output" / "group_coverage_latest.json"
    discover_output = ROOT / "output" / "lark_discover_latest.json"
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(GROUP_COVERAGE_SCRIPT),
        "--registry",
        str(GROUP_REGISTRY_PATH),
        "--db",
        str(db_path),
        "--discover-output",
        str(discover_output),
        "--stale-hours",
        str(stale_hours),
        "--write-json",
        str(coverage_path),
    ]
    if run_discover:
        command.append("--run-discover")
    result = run_subprocess(command)
    payload: dict[str, Any] = {
        "ok": result["ok"],
        "returncode": result["returncode"],
        "coverage_path": str(coverage_path.resolve()) if coverage_path.exists() else None,
        "stdout": result["stdout"],
        "stderr": result["stderr"],
    }
    if coverage_path.exists():
        try:
            payload["coverage"] = json.loads(coverage_path.read_text(encoding="utf-8"))
        except Exception:
            payload["coverage"] = result["stdout"]
    return payload


def run_ops_cycle(kind: str, *, run_discover: bool = True, auto_collect: bool = True, render_ceo: bool = False, stale_hours: int = 48) -> dict[str, Any]:
    script = DAILY_OPS_SCRIPT if kind == "daily" else WEEKLY_OPS_SCRIPT
    if not script.exists():
        raise FileNotFoundError(f"Ops runner not found: {script}")
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(script),
        "--stale-hours",
        str(stale_hours),
    ]
    if not run_discover:
        command.append("--skip-discover")
    if not auto_collect:
        command.append("--skip-auto-collect")
    if render_ceo:
        command.append("--render-ceo")
    result = run_subprocess(command)
    payload: dict[str, Any] = {"ok": result["ok"], "returncode": result["returncode"], "stdout": result["stdout"], "stderr": result["stderr"]}
    if result["stdout"]:
        try:
            payload["result"] = json.loads(result["stdout"])
        except Exception:
            pass
    return payload


def latest_ops_digest(period: str = "daily") -> dict[str, Any]:
    target = ROOT / "output" / "scheduled" / f"{period}_ops_digest_latest.json"
    if not target.exists():
        raise FileNotFoundError(f"Digest not found: {target}")
    payload = json.loads(target.read_text(encoding="utf-8"))
    return {"path": str(target.resolve()), "digest": payload}


def validate_ceo_brief(md_path: str | None) -> dict[str, Any]:
    target = resolve_md_path(md_path)
    if not VERIFY_SCRIPT.exists():
        raise FileNotFoundError(f"Validator not found: {VERIFY_SCRIPT}")
    result = run_subprocess([sys.executable, "-X", "utf8", str(VERIFY_SCRIPT), str(target)])
    result["target"] = str(target)
    return result


def render_ceo_brief() -> dict[str, Any]:
    if not RENDER_SCRIPT.exists():
        raise FileNotFoundError(f"Render script not found: {RENDER_SCRIPT}")
    result = run_subprocess([sys.executable, "-X", "utf8", str(RENDER_SCRIPT)])
    repo_pdf = latest_ceo_brief_pdf()
    result["repo_pdf"] = str(repo_pdf.resolve()) if repo_pdf else None
    result["desktop_pdfs"] = desktop_ceo_pdfs()
    return result


def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if name == "archive.summary":
        return tool_text_result(format_json(archive_summary()))
    if name == "sources.list":
        return tool_text_result(format_json(list_sources()))
    if name == "report.search":
        pattern = str(arguments.get("pattern", "")).strip()
        if not pattern:
            return tool_text_result("pattern is required", is_error=True)
        limit = int(arguments.get("limit", 20))
        file_glob = str(arguments.get("file_glob", "*"))
        return tool_text_result(format_json(search_reports(pattern, limit=limit, file_glob=file_glob)))
    if name == "requirements.search":
        keyword = str(arguments.get("keyword", "")).strip()
        if not keyword:
            return tool_text_result("keyword is required", is_error=True)
        limit = int(arguments.get("limit", 10))
        return tool_text_result(format_json(search_requirements(keyword, limit=limit)))
    if name == "group.registry":
        return tool_text_result(format_json(load_group_registry()))
    if name == "group.coverage":
        run_discover = bool(arguments.get("run_discover", False))
        stale_hours = int(arguments.get("stale_hours", 48))
        return tool_text_result(format_json(group_coverage(run_discover=run_discover, stale_hours=stale_hours)))
    if name == "ops.daily_run":
        return tool_text_result(
            format_json(
                run_ops_cycle(
                    "daily",
                    run_discover=bool(arguments.get("run_discover", True)),
                    auto_collect=bool(arguments.get("auto_collect", True)),
                    render_ceo=bool(arguments.get("render_ceo", False)),
                    stale_hours=int(arguments.get("stale_hours", 48)),
                )
            )
        )
    if name == "ops.weekly_run":
        return tool_text_result(
            format_json(
                run_ops_cycle(
                    "weekly",
                    run_discover=bool(arguments.get("run_discover", True)),
                    auto_collect=bool(arguments.get("auto_collect", True)),
                    render_ceo=bool(arguments.get("render_ceo", False)),
                    stale_hours=int(arguments.get("stale_hours", 48)),
                )
            )
        )
    if name == "ops.digest_latest":
        return tool_text_result(format_json(latest_ops_digest(str(arguments.get("period", "daily")).strip() or "daily")))
    if name == "ceo_brief.validate":
        return tool_text_result(format_json(validate_ceo_brief(arguments.get("md_path"))))
    if name == "ceo_brief.render":
        return tool_text_result(format_json(render_ceo_brief()))
    raise KeyError(f"Unknown tool: {name}")


def handle_request(message: dict[str, Any]) -> None:
    request_id = message.get("id")
    method = message.get("method")
    params = message.get("params") or {}

    if method == "initialize":
        requested_version = params.get("protocolVersion") or PROTOCOL_VERSION
        success_response(
            request_id,
            {
                "protocolVersion": requested_version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {
                    "name": "bydfi-exchange-ops",
                    "title": "BYDFI Exchange Ops",
                    "version": "1.0.0",
                    "description": "Local BYDFI internship archive and CEO brief operations server.",
                },
                "instructions": "Use these tools to inspect archived report assets, search evidence, validate CEO briefs, and render the latest management PDF.",
            },
        )
        return

    if method in {"notifications/initialized", "initialized", "exit"}:
        return

    if method == "ping":
        success_response(request_id, {})
        return

    if method == "tools/list":
        success_response(request_id, {"tools": TOOLS})
        return

    if method == "tools/call":
        try:
            tool_name = str(params.get("name", ""))
            arguments = params.get("arguments") or {}
            result = call_tool(tool_name, arguments)
            success_response(request_id, result)
        except KeyError as exc:
            error_response(request_id, -32602, str(exc))
        except Exception as exc:
            error_response(request_id, -32000, str(exc))
        return

    error_response(request_id, -32601, f"Method not found: {method}")


def main() -> int:
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            eprint(f"Invalid JSON: {exc}")
            continue

        if isinstance(message, list):
            for item in message:
                if isinstance(item, dict):
                    handle_request(item)
            continue

        if isinstance(message, dict):
            handle_request(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
