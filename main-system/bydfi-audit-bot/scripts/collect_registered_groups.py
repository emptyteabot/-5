from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from group_coverage_lib import (
    DEFAULT_DB_PATH,
    DEFAULT_COLLECTION_FRESH_HOURS,
    DEFAULT_DISCOVER_OUTPUT,
    DEFAULT_REGISTRY_PATH,
    DEFAULT_STALE_HOURS,
    ROOT,
    build_coverage_report,
    load_json,
    load_registry,
    resolve_external_collector_path,
    resolve_storage_state_path,
    run_discovery,
    write_json,
)


def _run_collect(
    *,
    external_collector: Path,
    storage_state: Path,
    db_path: Path,
    group_title: str,
    hours: int,
    summarize: bool,
    skip_document_fetch: bool,
) -> dict[str, object]:
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(external_collector),
        "collect",
        "--group-name",
        group_title,
        "--hours",
        str(hours),
        "--storage-state",
        str(storage_state),
    ]
    if skip_document_fetch:
        command.append("--skip-document-fetch")
    if summarize:
        command.append("--summarize")

    env = os.environ.copy()
    env["AUDIT_DB_PATH"] = str(db_path)
    env["DATA_DIR"] = str(ROOT / "data")
    env["REPORTS_DIR"] = str(ROOT / "data" / "reports")

    proc = subprocess.run(
        command,
        cwd=str(external_collector.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    return {
        "title": group_title,
        "hours": hours,
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect missing/stale registered Lark groups.")
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY_PATH))
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--discover-output", default=str(DEFAULT_DISCOVER_OUTPUT))
    parser.add_argument("--external-collector", default="")
    parser.add_argument("--storage-state", default="")
    parser.add_argument("--stale-hours", type=int, default=DEFAULT_STALE_HOURS)
    parser.add_argument("--collection-fresh-hours", type=int, default=DEFAULT_COLLECTION_FRESH_HOURS)
    parser.add_argument("--history-hours", type=int, default=4000)
    parser.add_argument("--refresh-hours", type=int, default=168)
    parser.add_argument("--title", action="append", default=[])
    parser.add_argument("--include-optional", action="store_true")
    parser.add_argument("--run-discover", action="store_true")
    parser.add_argument("--skip-summarize", action="store_true")
    parser.add_argument("--skip-document-fetch", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write-json", default="")
    args = parser.parse_args()

    registry_path = Path(args.registry).expanduser().resolve()
    db_path = Path(args.db).expanduser().resolve()
    discover_output = Path(args.discover_output).expanduser().resolve()
    registry = load_registry(registry_path)
    external_collector = resolve_external_collector_path(registry, args.external_collector)
    storage_state = resolve_storage_state_path(registry, args.storage_state)

    discover_payload = None
    if args.run_discover:
        collector_cfg = registry.get("collector", {})
        required_titles = [
            str(item.get("title", "")).strip()
            for item in registry.get("groups", [])
            if item.get("required", True) and str(item.get("title", "")).strip()
        ]
        discover_payload = run_discovery(
            external_collector_path=external_collector,
            storage_state_path=storage_state,
            db_path=db_path,
            output_path=discover_output,
            required_titles=required_titles,
            search_terms=list(collector_cfg.get("search_terms", [])),
            search_exhaustive=bool(collector_cfg.get("search_exhaustive", True)),
            search_limit=int(collector_cfg.get("search_limit", 400)),
            scroll_iterations=int(collector_cfg.get("scroll_iterations", 18)),
        )
    elif discover_output.exists():
        discover_payload = load_json(discover_output)

    before = build_coverage_report(
        registry=registry,
        db_path=db_path,
        discover_payload=discover_payload,
        stale_hours=args.stale_hours,
        collection_fresh_hours=args.collection_fresh_hours,
    )

    requested_titles = {str(item).strip() for item in args.title if str(item).strip()}
    targets = []
    for item in before.get("groups", []):
        if not args.include_optional and not item.get("required", True):
            continue
        if requested_titles and item["title"] not in requested_titles:
            continue
        if requested_titles or item["status"] in {"missing", "stale"}:
            targets.append(item)

    runs = []
    for item in targets:
        hours = args.history_hours if item["status"] in {"missing", "unverified"} else args.refresh_hours
        if args.dry_run:
            runs.append(
                {
                    "title": item["title"],
                    "hours": hours,
                    "ok": True,
                    "returncode": 0,
                    "stdout": "dry-run",
                    "stderr": "",
                }
            )
            continue
        runs.append(
            _run_collect(
                external_collector=external_collector,
                storage_state=storage_state,
                db_path=db_path,
                group_title=item["title"],
                hours=hours,
                summarize=not args.skip_summarize,
                skip_document_fetch=bool(args.skip_document_fetch),
            )
        )

    after = build_coverage_report(
        registry=registry,
        db_path=db_path,
        discover_payload=discover_payload,
        stale_hours=args.stale_hours,
        collection_fresh_hours=args.collection_fresh_hours,
    )

    payload = {
        "registry_path": str(registry_path),
        "db_path": str(db_path),
        "discover_output": str(discover_output) if discover_payload is not None else "",
        "resolved_external_collector_path": str(external_collector),
        "resolved_storage_state_path": str(storage_state),
        "targets": [item["title"] for item in targets],
        "runs": runs,
        "before": before["summary"],
        "after": after["summary"],
    }

    if args.write_json:
        write_json(Path(args.write_json).expanduser().resolve(), payload)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
