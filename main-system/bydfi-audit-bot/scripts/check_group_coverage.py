from __future__ import annotations

import argparse
import json
from pathlib import Path

from group_coverage_lib import (
    DEFAULT_DB_PATH,
    DEFAULT_COLLECTION_FRESH_HOURS,
    DEFAULT_DISCOVER_OUTPUT,
    DEFAULT_REGISTRY_PATH,
    DEFAULT_STALE_HOURS,
    build_coverage_report,
    format_human_summary,
    load_json,
    load_registry,
    resolve_external_collector_path,
    resolve_storage_state_path,
    run_discovery,
    write_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit registered Lark groups against local SQLite coverage.")
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY_PATH))
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--discover-output", default=str(DEFAULT_DISCOVER_OUTPUT))
    parser.add_argument("--external-collector", default="")
    parser.add_argument("--storage-state", default="")
    parser.add_argument("--stale-hours", type=int, default=DEFAULT_STALE_HOURS)
    parser.add_argument("--collection-fresh-hours", type=int, default=DEFAULT_COLLECTION_FRESH_HOURS)
    parser.add_argument("--run-discover", action="store_true")
    parser.add_argument("--write-json", default="")
    parser.add_argument("--text", action="store_true", help="Print a short text summary instead of JSON.")
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

    report = build_coverage_report(
        registry=registry,
        db_path=db_path,
        discover_payload=discover_payload,
        stale_hours=args.stale_hours,
        collection_fresh_hours=args.collection_fresh_hours,
    )
    report["registry_path"] = str(registry_path)
    report["discover_output"] = str(discover_output) if discover_payload is not None else ""
    report["resolved_external_collector_path"] = str(external_collector)
    report["resolved_storage_state_path"] = str(storage_state)

    if args.write_json:
        write_json(Path(args.write_json).expanduser().resolve(), report)

    if args.text:
        print(format_human_summary(report))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
