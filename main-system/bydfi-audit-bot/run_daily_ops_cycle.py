from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))

from ops_digest_lib import run_ops_cycle  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the daily BYDFI ops cycle.")
    parser.add_argument("--skip-discover", action="store_true")
    parser.add_argument("--skip-auto-collect", action="store_true")
    parser.add_argument("--render-ceo", action="store_true")
    parser.add_argument("--render-digest-pdf", action="store_true")
    parser.add_argument("--deliver", action="store_true")
    parser.add_argument("--stale-hours", type=int, default=48)
    args = parser.parse_args()
    payload = run_ops_cycle(
        "daily",
        run_discover=not args.skip_discover,
        auto_collect=not args.skip_auto_collect,
        render_ceo=bool(args.render_ceo),
        render_digest_pdf_file=bool(args.render_digest_pdf),
        deliver=bool(args.deliver),
        stale_hours=args.stale_hours,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
