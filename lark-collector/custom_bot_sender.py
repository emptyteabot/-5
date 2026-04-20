from __future__ import annotations

from typing import Any

import requests

from config import CUSTOM_BOT_WEBHOOK_URL


def send_custom_bot_text(text: str, *, webhook_url: str | None = None, timeout: int = 15) -> dict[str, Any]:
    target = (webhook_url or CUSTOM_BOT_WEBHOOK_URL).strip()
    if not target:
        raise RuntimeError("CUSTOM_BOT_WEBHOOK_URL 为空，无法向外部群自定义机器人发送消息。")

    resp = requests.post(
        target,
        json={"msg_type": "text", "content": {"text": text}},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("StatusCode") not in (0, None):
        raise RuntimeError(f"custom bot send failed: {data}")
    return data
