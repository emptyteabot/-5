from __future__ import annotations

import argparse

from custom_bot_sender import send_custom_bot_text


def main() -> int:
    parser = argparse.ArgumentParser(description="Send a UTF-8 webhook text message.")
    parser.add_argument("--webhook-url", required=True)
    parser.add_argument("--text", required=True)
    args = parser.parse_args()

    result = send_custom_bot_text(args.text, webhook_url=args.webhook_url)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
