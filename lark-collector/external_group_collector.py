from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from playwright.sync_api import BrowserContext, Page, Playwright, sync_playwright

from config import (
    BOT_DISPLAY_NAME,
    EXTERNAL_GROUP_MESSAGE_SCAN_LIMIT,
    EXTERNAL_GROUP_REPORT_KEYWORDS,
    EXTERNAL_GROUP_SCROLL_ITERATIONS,
    LARK_WEB_BASE_URL,
    PLAYWRIGHT_BROWSER_CHANNEL,
    PLAYWRIGHT_EDGE_PROFILE_DIR,
    PLAYWRIGHT_NAV_TIMEOUT_MS,
    PLAYWRIGHT_SLOW_MO_MS,
    PLAYWRIGHT_STORAGE_STATE_MAX_AGE_HOURS,
    PLAYWRIGHT_STORAGE_STATE_PATH,
    PLAYWRIGHT_USE_EDGE_PROFILE,
    DB_PATH,
)
from custom_bot_sender import send_custom_bot_text
from group_summary import build_group_summary_report, default_window
from message_sources import build_source_payload
from storage import (
    create_collection_run,
    finish_collection_run,
    get_document,
    get_record,
    set_parsed_content,
    upsert_document,
    upsert_message,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

LOCAL_TZ = timezone(timedelta(hours=8))
DEFAULT_MESSENGER_URL = f"{LARK_WEB_BASE_URL.rstrip('/')}/next/messenger/"
AUTH_STATE_MIN_BYTES = 500
AUTH_STATE_MAX_AGE_HOURS = PLAYWRIGHT_STORAGE_STATE_MAX_AGE_HOURS
FUTURE_TOLERANCE = timedelta(minutes=2)

CHINESE_FULL_DATETIME_RE = re.compile(
    r"(?P<year>\d{4})年(?P<month>\d{1,2})月(?P<day>\d{1,2})日\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})"
)
CHINESE_MONTH_DAY_RE = re.compile(
    r"(?P<month>\d{1,2})月(?P<day>\d{1,2})日\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})"
)
SLASH_MONTH_DAY_RE = re.compile(
    r"(?P<month>\d{1,2})[-/](?P<day>\d{1,2})\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})"
)
RELATIVE_DAY_TIME_RE = re.compile(
    r"(?P<label>今天|昨天|前天)\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})"
)
TIME_ONLY_RE = re.compile(r"(?P<hour>\d{1,2}):(?P<minute>\d{2})$")
RELATIVE_DAY_OFFSETS = {"今天": 0, "昨天": 1, "前天": 2}

MESSAGE_NODE_SNAPSHOT_JS = """
() => {
  const selectors = [
    '[data-message-id]',
    '[data-testid*="message"]',
    '.threadMsg',
    '[class*="message-item"]',
    '[class*="MessageItem"]',
    '[class*="msg-item"]',
    '[role="listitem"]'
  ];
  const roots = [];
  const seen = new Set();
  const push = (candidate) => {
    if (!candidate) return;
    const root = candidate.closest('[data-message-id], [data-testid*="message"], .threadMsg, [class*="message-item"], [class*="MessageItem"], [class*="msg-item"], [role="listitem"]') || candidate;
    const text = (root.innerText || '').trim();
    const key = root.getAttribute('data-message-id') || root.id || `${text.slice(0, 120)}|${Math.round(root.getBoundingClientRect().top)}`;
    if (!text && !root.querySelector('a[href]')) return;
    if (seen.has(key)) return;
    seen.add(key);
    roots.push(root);
  };

  selectors.forEach((selector) => {
    document.querySelectorAll(selector).forEach(push);
  });

  return roots.map((root, index) => {
    const rect = root.getBoundingClientRect();
    const links = Array.from(root.querySelectorAll('a[href]'))
      .map((node) => node.href)
      .filter(Boolean);
    const senderCandidates = Array.from(
      root.querySelectorAll('[data-sender-name], [class*="sender"], [class*="Sender"], [class*="author"], [class*="Author"], [class*="nick"], [class*="Nick"], [class*="name"], [class*="Name"]')
    )
      .map((node) => (node.textContent || '').trim())
      .filter((value) => value && value.length <= 40);
    const timeNode = root.querySelector('time, [datetime], [data-time], [data-create-time], [class*="time"], [class*="Time"]');
    return {
      ordinal: index,
      messageId: root.getAttribute('data-message-id') || root.id || '',
      text: (root.innerText || '').trim(),
      ariaLabel: root.getAttribute('aria-label') || '',
      className: root.className || '',
      timestampHint:
        (timeNode && (timeNode.getAttribute('datetime') || timeNode.getAttribute('data-time') || timeNode.getAttribute('data-create-time') || timeNode.textContent || '')) || '',
      senderCandidates: Array.from(new Set(senderCandidates)).slice(0, 8),
      links: Array.from(new Set(links)).slice(0, 12),
      top: Math.round(rect.top),
      height: Math.round(rect.height),
      htmlPreview: (root.innerHTML || '').slice(0, 2000)
    };
  }).filter((item) => item.text || item.links.length);
}
"""

SCROLL_PAGE_JS = """
() => {
  const messageSelectors = ['.js-message-item', '[data-message-id]'];
  const messageCount = (el) => {
    let count = 0;
    for (const sel of messageSelectors) {
      count += el.querySelectorAll(sel).length;
    }
    return count;
  };

  const candidates = Array.from(document.querySelectorAll('*'))
    .filter((node) => {
      const el = node;
      if (!(el instanceof HTMLElement)) return false;
      if (el.scrollHeight - el.clientHeight < 240) return false;
      const style = window.getComputedStyle(el);
      if (style.display === 'none' || style.visibility === 'hidden') return false;
      return style.overflowY === 'auto' || style.overflowY === 'scroll';
    })
    .map((el) => ({ el, msg: messageCount(el), area: el.clientHeight * el.clientWidth }))
    .sort((a, b) => (b.msg - a.msg) || (b.area - a.area))
    .slice(0, 8)
    .map((x) => x.el);

  let changed = 0;
  for (const el of candidates) {
    const before = el.scrollTop;
    const delta = Math.max(900, el.clientHeight * 0.9);
    el.scrollTop = Math.max(0, before - delta);
    if (el.scrollTop !== before) {
      el.dispatchEvent(new Event('scroll', { bubbles: true }));
      changed += 1;
    }
  }

  if (!changed) {
    const windowBefore = window.scrollY;
    window.scrollBy(0, -window.innerHeight);
    if (window.scrollY !== windowBefore) changed += 1;
  }
  return changed;
}
"""

AGGRESSIVE_SCROLL_PAGE_JS = """
() => {
  const messageSelectors = ['.js-message-item', '[data-message-id]'];
  const messageCount = (el) => {
    let count = 0;
    for (const sel of messageSelectors) {
      count += el.querySelectorAll(sel).length;
    }
    return count;
  };

  const candidates = Array.from(document.querySelectorAll('*'))
    .filter((node) => {
      const el = node;
      if (!(el instanceof HTMLElement)) return false;
      if (el.scrollHeight - el.clientHeight < 240) return false;
      const style = window.getComputedStyle(el);
      if (style.display === 'none' || style.visibility === 'hidden') return false;
      return style.overflowY === 'auto' || style.overflowY === 'scroll';
    })
    .map((el) => ({ el, msg: messageCount(el), area: el.clientHeight * el.clientWidth }))
    .sort((a, b) => (b.msg - a.msg) || (b.area - a.area))
    .slice(0, 8)
    .map((x) => x.el);

  let changed = 0;
  for (const el of candidates) {
    const before = el.scrollTop;
    const delta = Math.max(el.clientHeight * 3, 2400);
    el.scrollTop = Math.max(0, before - delta);
    if (before <= 4) {
      el.scrollTop = 0;
    }
    if (el.scrollTop !== before) {
      el.dispatchEvent(new Event('scroll', { bubbles: true }));
      changed += 1;
    }
  }

  if (!changed) {
    const windowBefore = window.scrollY;
    window.scrollBy(0, -window.innerHeight * 2);
    if (window.scrollY !== windowBefore) {
      changed += 1;
    }
  }
  return changed;
}
"""

SCROLL_TO_BOTTOM_JS = """
() => {
  const candidates = Array.from(document.querySelectorAll('*'))
    .filter((node) => {
      const el = node;
      if (!(el instanceof HTMLElement)) return false;
      if (el.scrollHeight - el.clientHeight < 240) return false;
      const style = window.getComputedStyle(el);
      if (style.display === 'none' || style.visibility === 'hidden') return false;
      return style.overflowY === 'auto' || style.overflowY === 'scroll';
    })
    .map((el) => {
      const msgCount = el.querySelectorAll('.js-message-item, [data-message-id]').length;
      const area = el.clientHeight * el.clientWidth;
      return { el, msgCount, area };
    })
    .sort((a, b) => (b.msgCount - a.msgCount) || (b.area - a.area))
    .slice(0, 8)
    .map((x) => x.el);

  let changed = 0;
  for (const el of candidates) {
    const before = el.scrollTop;
    el.scrollTop = el.scrollHeight;
    if (el.scrollTop !== before) changed += 1;
  }
  if (!changed) {
    const before = window.scrollY;
    window.scrollTo(0, document.body.scrollHeight);
    if (window.scrollY !== before) changed += 1;
  }
  return changed;
}
"""


@dataclass
class CollectedReport:
    message_id: str
    chat_id: str
    sender_id: str
    sender_name: str
    created_at: str
    title: str
    body: str
    source_type: str
    raw_message: dict[str, Any]


@dataclass
class DiscoveredChat:
    feed_id: str
    title: str
    badge: str
    time_hint: str
    preview_text: str
    is_bot: bool
    is_group_candidate: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "feed_id": self.feed_id,
            "title": self.title,
            "badge": self.badge,
            "time_hint": self.time_hint,
            "preview_text": self.preview_text,
            "is_bot": self.is_bot,
            "is_group_candidate": self.is_group_candidate,
        }


class AuthExpiredError(RuntimeError):
    """Raised when the persisted Lark auth state is missing, stale, or clearly broken."""


SIDEBAR_TIME_RE = re.compile(
    r"^(?:\d{1,2}:\d{2}|昨天|前天|\d{1,2}月\d{1,2}日|\d{4}[/-]\d{1,2}[/-]\d{1,2})$"
)
GROUP_TITLE_HINTS = (
    "群",
    "任务",
    "周报",
    "合作",
    "业务",
    "血战到底",
    "研发",
    "测试",
    "项目",
    "协作",
    "会议",
    "复盘",
)
BOT_TITLE_HINTS = (
    "助手",
    "审批",
    "安全中心",
    "assistant",
    "bot",
    "机器人",
    "假勤",
)
LOGIN_URL_HINTS = (
    "/accounts/page/login",
    "/accounts/login",
    "/accounts/auth",
    "passport",
)
DEFAULT_SEARCH_BUCKETS = [
    "永续",
    "MoonX",
    "周报",
    "项目",
    "任务",
    "协作",
    "部门",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
]

SIDEBAR_FEED_SNAPSHOT_JS = """
() => {
  return Array.from(document.querySelectorAll('[data-feed-id]')).map((node) => {
    const card = node.closest('.a11y_feed_card_item') || node;
    const rect = card.getBoundingClientRect();
    return {
      feedId: node.getAttribute('data-feed-id') || '',
      text: (node.innerText || card.innerText || '').trim(),
      active: card.getAttribute('data-feed-active') === 'true',
      top: Math.round(rect.top),
      height: Math.round(rect.height),
    };
  }).filter((item) => item.feedId && item.text);
}
"""


def _stable_message_id(chat_id: str, node: dict[str, Any], timestamp: datetime, sender_name: str, body_seed: str) -> str:
    native_id = str(node.get("messageId") or "").strip()
    if native_id:
        return f"webmsg_{chat_id}_{native_id}"
    fallback_seed = "|".join([chat_id, sender_name, timestamp.isoformat(), body_seed[:240]])
    return f"web_{hashlib.sha256(fallback_seed.encode('utf-8')).hexdigest()[:24]}"


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _document_key_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or ""
    match = re.search(r"/(wiki|docx|document)/([A-Za-z0-9]+)", path)
    if match:
        return f"{match.group(1)}_{match.group(2)}"
    normalized = f"{parsed.netloc}{parsed.path}?{parsed.query}".strip("?")
    return f"url_{_slug(normalized or url)}"


def _storage_state_path(path: str | None = None) -> Path:
    return Path(path or PLAYWRIGHT_STORAGE_STATE_PATH).expanduser()


def _validate_storage_state_or_raise(path: Path, *, max_age_hours: int = AUTH_STATE_MAX_AGE_HOURS) -> None:
    if PLAYWRIGHT_USE_EDGE_PROFILE:
        return
    if not path.exists():
        raise AuthExpiredError(f"飞书会话状态文件不存在：{path}")
    stat = path.stat()
    if stat.st_size < AUTH_STATE_MIN_BYTES:
        raise AuthExpiredError("飞书会话状态已过期或文件损坏，强制阻断抓取链路：storage_state 文件体积异常。")
    age_seconds = max(0.0, datetime.now().timestamp() - stat.st_mtime)
    if age_seconds > max_age_hours * 3600:
        raise AuthExpiredError(
            f"飞书会话状态已过期或文件损坏，强制阻断抓取链路：storage_state 已超过 {max_age_hours} 小时未刷新。"
        )


def _launch_context(playwright: Playwright, *, headed: bool, storage_state_path: Path | None) -> BrowserContext:
    kwargs: dict[str, Any] = {
        "headless": not headed,
        "slow_mo": PLAYWRIGHT_SLOW_MO_MS,
        "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }
    channel = PLAYWRIGHT_BROWSER_CHANNEL.strip()
    if channel and channel not in {"chromium", "chrome"}:
        kwargs["channel"] = channel
    context_kwargs: dict[str, Any] = {
        "locale": "zh-CN",
        "timezone_id": "Asia/Shanghai",
        "viewport": {"width": 1440, "height": 1200},
    }
    if PLAYWRIGHT_USE_EDGE_PROFILE:
        user_data_dir = Path(PLAYWRIGHT_EDGE_PROFILE_DIR).expanduser()
        if user_data_dir.name.lower() == "default":
            user_data_dir = user_data_dir.parent
        kwargs["headless"] = False
        return playwright.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            **kwargs,
            **context_kwargs,
        )
    browser = playwright.chromium.launch(**kwargs)
    if storage_state_path and storage_state_path.exists():
        context_kwargs["storage_state"] = str(storage_state_path)
    return browser.new_context(**context_kwargs)


def _looks_like_login_page(page: Page) -> bool:
    try:
        current_url = page.url.lower()
    except Exception:
        current_url = ""
    if any(hint in current_url for hint in LOGIN_URL_HINTS):
        return True
    try:
        title = (page.title() or "").strip()
    except Exception:
        title = ""
    if "登录" in title or "login" in title.lower():
        return True
    try:
        preview = page.evaluate(
            """
            () => {
              const bodyText = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
              return bodyText.slice(0, 500);
            }
            """
        )
    except Exception:
        preview = ""
    return "欢迎使用 Lark" in preview and "下一步" in preview


def _collect_page_diagnostics(page: Page) -> dict[str, Any]:
    try:
        payload = page.evaluate(
            """
            () => {
              const bodyText = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
              return {
                href: location.href,
                title: document.title || '',
                feedCount: document.querySelectorAll('[data-feed-id]').length,
                listItemCount: document.querySelectorAll('[role="listitem"]').length,
                searchCount: document.querySelectorAll(
                  "input[placeholder*='搜索'], input[placeholder*='Search'], [role='searchbox'], div.appNavbar-search-input"
                ).length,
                buttonTexts: Array.from(document.querySelectorAll('button, [role="button"]'))
                  .slice(0, 10)
                  .map((node) => (node.innerText || node.getAttribute('aria-label') || '').trim())
                  .filter(Boolean),
                bodyPreview: bodyText.slice(0, 500),
              };
            }
            """
        )
    except Exception as exc:
        payload = {
            "href": page.url,
            "title": "",
            "feedCount": 0,
            "listItemCount": 0,
            "searchCount": 0,
            "buttonTexts": [],
            "bodyPreview": "",
            "diagnosticError": str(exc),
        }
    return payload


def _ensure_messenger_page_or_raise(page: Page, *, context_label: str) -> dict[str, Any]:
    diagnostics = _collect_page_diagnostics(page)
    if _looks_like_login_page(page):
        preview = diagnostics.get("bodyPreview", "")
        raise AuthExpiredError(
            f"{context_label}失败：当前页面仍然是 Lark 登录页，无法证明已进入 messenger。"
            f" url={diagnostics.get('href', '')} title={diagnostics.get('title', '')!r} preview={preview!r}"
        )
    return diagnostics


def _wait_for_messenger_ready(page: Page, *, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + max(5, timeout_seconds)
    latest = _collect_page_diagnostics(page)
    while time.monotonic() < deadline:
        latest = _collect_page_diagnostics(page)
        if not _looks_like_login_page(page):
            return latest
        page.wait_for_timeout(1000)
    return latest


def _wait_for_sidebar_feed_ready(page: Page, *, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + max(5, timeout_seconds)
    latest = _collect_page_diagnostics(page)
    while time.monotonic() < deadline:
        latest = _collect_page_diagnostics(page)
        if int(latest.get("feedCount") or 0) > 0:
            return latest
        page.wait_for_timeout(1000)
    return latest


def _validate_saved_login_state(path: Path, *, login_url: str) -> dict[str, Any]:
    _validate_storage_state_or_raise(path)
    with sync_playwright() as playwright:
        context = _launch_context(playwright, headed=False, storage_state_path=path)
        page = context.new_page()
        page.set_default_timeout(PLAYWRIGHT_NAV_TIMEOUT_MS)
        page.goto(login_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        page.wait_for_timeout(2500)
        diagnostics = _collect_page_diagnostics(page)
        login_required = _looks_like_login_page(page)
        context.close()
    return {
        "ok": not login_required,
        "diagnostics": diagnostics,
    }


def save_login_state(
    storage_state_path: str | None = None,
    *,
    login_url: str = DEFAULT_MESSENGER_URL,
    wait_seconds: int = 30,
) -> Path:
    state_path = _storage_state_path(storage_state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        context = _launch_context(playwright, headed=True, storage_state_path=None)
        page = context.new_page()
        page.set_default_timeout(PLAYWRIGHT_NAV_TIMEOUT_MS)
        page.goto(login_url, wait_until="domcontentloaded")
        print(f"已打开 {login_url}，请在浏览器里完成 Lark 登录。")
        effective_wait = wait_seconds if wait_seconds > 0 else 180
        print(f"将在最多 {effective_wait} 秒内等待你进入 messenger，检测到登录成功后自动保存。")
        diagnostics = _wait_for_messenger_ready(page, timeout_seconds=effective_wait)
        if _looks_like_login_page(page):
            context.close()
            raise AuthExpiredError(
                "登录态未保存：等待超时后页面仍停留在 Lark 登录流程。"
                f" url={diagnostics.get('href', '')} title={diagnostics.get('title', '')!r}"
            )
        context.storage_state(path=str(state_path))
        context.close()
    validation = _validate_saved_login_state(state_path, login_url=login_url)
    if not validation["ok"]:
        diagnostics = validation["diagnostics"]
        raise AuthExpiredError(
            "登录态保存失败：保存后的 storage_state 重新打开仍跳回登录页，当前自动抓取不可用。"
            f" url={diagnostics.get('href', '')} title={diagnostics.get('title', '')!r}"
        )
    return state_path


def _slug(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _stable_chat_id(
    *,
    explicit_chat_id: str | None,
    requested_group_name: str,
    current_chat_name: str,
    current_url: str,
) -> str:
    if explicit_chat_id:
        return explicit_chat_id.strip()
    requested_title = requested_group_name.strip()
    if requested_title:
        existing_chat_id = _find_existing_chat_id_by_title(requested_title)
        if existing_chat_id:
            return existing_chat_id
        return f"external_web_{_slug(requested_title)}"
    if current_chat_name.strip():
        return f"external_web_{_slug(current_chat_name.strip())}"
    parsed = urlparse(current_url)
    raw = f"{parsed.netloc}{parsed.path}{parsed.query}".strip() or current_url
    return f"external_web_{_slug(raw)}"


def _looks_like_time(text: str) -> bool:
    text = text.strip()
    return bool(
        re.search(r"\b\d{1,2}:\d{2}\b", text)
        or re.search(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", text)
        or re.search(r"\b\d{1,2}[-/]\d{1,2}\b", text)
    )


def _looks_like_sidebar_time(text: str) -> bool:
    return bool(SIDEBAR_TIME_RE.fullmatch(text.strip()))


def _looks_like_person_name(text: str) -> bool:
    value = text.strip()
    if not value:
        return False
    if re.fullmatch(r"[A-Za-z][A-Za-z ._-]{0,31}", value):
        return True
    return bool(re.fullmatch(r"[\u4e00-\u9fff]{2,3}", value))


def _looks_like_group_title(text: str) -> bool:
    value = text.strip()
    if not value:
        return False
    lowered = value.lower()
    if any(token.lower() in lowered for token in GROUP_TITLE_HINTS):
        return True
    if any(token in value for token in ("&", "·", "/", "｜", "|")):
        return True
    if _looks_like_person_name(value):
        return False
    return bool(re.fullmatch(r"[\u4e00-\u9fff]{4,}", value))


def _normalize_discovered_chat(payload: dict[str, Any]) -> DiscoveredChat | None:
    feed_id = str(payload.get("feedId") or "").strip()
    raw_text = str(payload.get("text") or "").strip()
    if not feed_id or not raw_text:
        return None

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return None

    title = lines[0]
    cursor = 1
    badge = ""
    if cursor < len(lines) and lines[cursor] in {"外部", "机器人", "内部"}:
        badge = lines[cursor]
        cursor += 1

    time_hint = ""
    if cursor < len(lines) and _looks_like_sidebar_time(lines[cursor]):
        time_hint = lines[cursor]
        cursor += 1

    preview_text = " ".join(lines[cursor:]).strip()
    lowered_title = title.lower()
    lowered_preview = preview_text.lower()
    is_bot = (
        badge == "机器人"
        or any(token in lowered_title for token in BOT_TITLE_HINTS)
        or lowered_preview.startswith("docs assistant:")
    )
    group_hint = any(
        marker in preview_text for marker in ("加入此群", "加入话题群", "创建了群组", "群汇总分析")
    )
    is_group_candidate = (not is_bot) and (
        _looks_like_group_title(title)
        or (group_hint and not _looks_like_person_name(title))
        or (badge == "外部" and not _looks_like_person_name(title))
    )

    return DiscoveredChat(
        feed_id=feed_id,
        title=title,
        badge=badge,
        time_hint=time_hint,
        preview_text=preview_text,
        is_bot=is_bot,
        is_group_candidate=is_group_candidate,
    )


def _guess_sender_name(node: dict[str, Any]) -> str:
    for candidate in node.get("senderCandidates", []):
        value = str(candidate).strip()
        if value and len(value) <= 32 and not _looks_like_time(value):
            return value

    lines = [line.strip() for line in str(node.get("text") or "").splitlines() if line.strip()]
    for line in lines[:3]:
        if len(line) <= 32 and not _looks_like_time(line) and not line.startswith("http"):
            return line

    aria_label = str(node.get("ariaLabel") or "").strip()
    if aria_label:
        pieces = [part.strip() for part in re.split(r"[,\n]", aria_label) if part.strip()]
        for piece in pieces[:3]:
            if len(piece) <= 32 and not _looks_like_time(piece):
                return piece

    return "unknown"


def _find_existing_chat_id_by_title(title: str) -> str:
    normalized_title = title.strip()
    if not normalized_title:
        return ""
    db_path = Path(DB_PATH).expanduser()
    if not db_path.exists():
        return ""

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT chat_id, raw_message_json
            FROM audit_records
            WHERE chat_id LIKE 'external_web_%'
              AND raw_message_json IS NOT NULL
              AND raw_message_json <> ''
              AND raw_message_json LIKE ?
            """,
            (f"%{normalized_title}%",),
        ).fetchall()
    finally:
        conn.close()

    counts: dict[str, int] = {}
    for chat_id, raw_message_json in rows:
        try:
            raw_message = json.loads(raw_message_json or "{}")
        except json.JSONDecodeError:
            continue
        chat_name = str(raw_message.get("chat_name") or raw_message.get("current_chat_name") or "").strip()
        if not chat_name:
            continue
        chat_head = next((line.strip() for line in chat_name.splitlines() if line.strip()), "")
        if chat_head != normalized_title:
            continue
        counts[str(chat_id)] = counts.get(str(chat_id), 0) + 1

    if not counts:
        return ""
    return max(counts.items(), key=lambda item: item[1])[0]


def _parse_timestamp(value: str, *, fallback: datetime) -> datetime:
    raw = value.strip()
    if not raw:
        return fallback

    local_fallback = fallback.astimezone(LOCAL_TZ)

    if raw.isdigit():
        number = int(raw)
        if number > 10_000_000_000:
            return datetime.fromtimestamp(number / 1000, tz=timezone.utc)
        if number > 1_000_000_000:
            return datetime.fromtimestamp(number, tz=timezone.utc)

    for pattern in (
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y年%m月%d日 %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            dt = datetime.strptime(raw, pattern)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=LOCAL_TZ)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    match = CHINESE_FULL_DATETIME_RE.search(raw)
    if match:
        year = int(match.group("year"))
        month = int(match.group("month"))
        day = int(match.group("day"))
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        try:
            dt = datetime(year, month, day, hour, minute, tzinfo=LOCAL_TZ)
        except ValueError:
            return fallback
        return dt.astimezone(timezone.utc)

    match = CHINESE_MONTH_DAY_RE.search(raw)
    if match:
        month = int(match.group("month"))
        day = int(match.group("day"))
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        try:
            dt = datetime(local_fallback.year, month, day, hour, minute, tzinfo=LOCAL_TZ)
        except ValueError:
            return fallback
        if dt.astimezone(timezone.utc) > fallback + FUTURE_TOLERANCE:
            try:
                dt = dt.replace(year=dt.year - 1)
            except ValueError:
                return fallback
        return dt.astimezone(timezone.utc)

    match = re.search(
        r"(?P<year>\d{4})年(?P<month>\d{1,2})月(?P<day>\d{1,2})日\s+"
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?",
        raw,
    )
    if match:
        year = int(match.group("year"))
        month = int(match.group("month"))
        day = int(match.group("day"))
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        second = int(match.group("second") or "0")
        try:
            dt = datetime(year, month, day, hour, minute, second, tzinfo=LOCAL_TZ)
        except ValueError:
            return fallback
        return dt.astimezone(timezone.utc)

    match = re.search(
        r"(?P<label>今天|昨天|前天)\s+"
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?",
        raw,
    )
    if match:
        day_offset = {"今天": 0, "昨天": 1, "前天": 2}[match.group("label")]
        anchor = local_fallback - timedelta(days=day_offset)
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        second = int(match.group("second") or "0")
        dt = anchor.replace(hour=hour, minute=minute, second=second, microsecond=0)
        return dt.astimezone(timezone.utc)

    match = re.search(
        r"(?P<label>周一|周二|周三|周四|周五|周六|周日|星期一|星期二|星期三|星期四|星期五|星期六|星期日|星期天)\s+"
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?",
        raw,
    )
    if match:
        weekday_map = {
            "周一": 0,
            "星期一": 0,
            "周二": 1,
            "星期二": 1,
            "周三": 2,
            "星期三": 2,
            "周四": 3,
            "星期四": 3,
            "周五": 4,
            "星期五": 4,
            "周六": 5,
            "星期六": 5,
            "周日": 6,
            "星期日": 6,
            "星期天": 6,
        }
        days_back = (local_fallback.weekday() - weekday_map[match.group("label")]) % 7
        anchor = local_fallback - timedelta(days=days_back)
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        second = int(match.group("second") or "0")
        dt = anchor.replace(hour=hour, minute=minute, second=second, microsecond=0)
        return dt.astimezone(timezone.utc)

    match = re.search(r"(\d{1,2})[-/](\d{1,2})\s+(\d{1,2}):(\d{2})", raw)
    if match:
        month, day, hour, minute = map(int, match.groups())
        try:
            dt = datetime(local_fallback.year, month, day, hour, minute, tzinfo=LOCAL_TZ)
        except ValueError:
            return fallback
        if dt > local_fallback + timedelta(days=1):
            try:
                dt = dt.replace(year=dt.year - 1)
            except ValueError:
                return fallback
        return dt.astimezone(timezone.utc)

    match = re.search(r"(\d{1,2})月(\d{1,2})日\s+(\d{1,2}):(\d{2})", raw)
    if match:
        month, day, hour, minute = map(int, match.groups())
        try:
            dt = datetime(local_fallback.year, month, day, hour, minute, tzinfo=LOCAL_TZ)
        except ValueError:
            return fallback
        if dt > local_fallback + timedelta(days=1):
            try:
                dt = dt.replace(year=dt.year - 1)
            except ValueError:
                return fallback
        return dt.astimezone(timezone.utc)

    match = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", raw)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        second = int(match.group(3) or "0")
        dt = local_fallback.replace(hour=hour, minute=minute, second=second, microsecond=0)
        if dt.astimezone(timezone.utc) > fallback + FUTURE_TOLERANCE:
            dt -= timedelta(days=1)
        return dt.astimezone(timezone.utc)

    return fallback


def _resolve_collected_timestamps(nodes: list[dict[str, Any]], *, fallback: datetime) -> list[datetime]:
    return _resolve_node_timestamps_v2(nodes, fallback=fallback)


def _choose_year_for_month_day_v2(
    month: int,
    day: int,
    hour: int,
    minute: int,
    *,
    fallback_local: datetime,
    anchor_local: datetime | None = None,
) -> datetime:
    candidate_years = {
        fallback_local.year - 1,
        fallback_local.year,
        fallback_local.year + 1,
    }
    if anchor_local is not None:
        candidate_years.update({anchor_local.year - 1, anchor_local.year, anchor_local.year + 1})

    candidates: list[datetime] = []
    for year in sorted(candidate_years):
        try:
            candidates.append(datetime(year, month, day, hour, minute, tzinfo=LOCAL_TZ))
        except ValueError:
            continue

    if not candidates:
        return fallback_local

    if anchor_local is not None:
        futureish = [dt for dt in candidates if dt >= anchor_local - timedelta(hours=12)]
        if futureish:
            return min(futureish, key=lambda dt: (dt - anchor_local, abs((dt - fallback_local).total_seconds())))
        return max(candidates, key=lambda dt: dt.timestamp())

    past_or_now = [dt for dt in candidates if dt <= fallback_local]
    if past_or_now:
        return max(past_or_now, key=lambda dt: dt.timestamp())
    return min(candidates, key=lambda dt: dt.timestamp())


def _parse_timestamp_v2(value: str, *, fallback: datetime, anchor: datetime | None = None) -> datetime:
    raw = value.strip()
    fallback_local = fallback.astimezone(LOCAL_TZ)
    anchor_local = anchor.astimezone(LOCAL_TZ) if anchor is not None else None
    if not raw:
        if anchor_local is not None:
            dt = min(anchor_local + timedelta(seconds=1), fallback_local)
            return dt.astimezone(timezone.utc)
        return fallback

    if raw.isdigit():
        number = int(raw)
        if number > 10_000_000_000:
            return datetime.fromtimestamp(number / 1000, tz=timezone.utc)
        if number > 1_000_000_000:
            return datetime.fromtimestamp(number, tz=timezone.utc)

    for pattern in (
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y年%m月%d日 %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            dt = datetime.strptime(raw, pattern)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=LOCAL_TZ)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日\s+(\d{1,2}):(\d{2})", raw)
    if match:
        year, month, day, hour, minute = map(int, match.groups())
        return datetime(year, month, day, hour, minute, tzinfo=LOCAL_TZ).astimezone(timezone.utc)

    match = re.search(r"昨天\s+(\d{1,2}):(\d{2})", raw)
    if match:
        hour, minute = map(int, match.groups())
        dt = (fallback_local - timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        return dt.astimezone(timezone.utc)

    match = re.search(r"前天\s+(\d{1,2}):(\d{2})", raw)
    if match:
        hour, minute = map(int, match.groups())
        dt = (fallback_local - timedelta(days=2)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        return dt.astimezone(timezone.utc)

    match = re.search(r"今天\s+(\d{1,2}):(\d{2})", raw)
    if match:
        hour, minute = map(int, match.groups())
        dt = fallback_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if dt.astimezone(timezone.utc) > fallback + FUTURE_TOLERANCE:
            dt = dt - timedelta(days=1)
        return dt.astimezone(timezone.utc)

    match = re.search(r"(\d{1,2})月(\d{1,2})日\s+(\d{1,2}):(\d{2})", raw)
    if match:
        month, day, hour, minute = map(int, match.groups())
        dt = _choose_year_for_month_day_v2(
            month,
            day,
            hour,
            minute,
            fallback_local=fallback_local,
            anchor_local=anchor_local,
        )
        return dt.astimezone(timezone.utc)

    match = re.search(r"(\d{1,2})[-/](\d{1,2})\s+(\d{1,2}):(\d{2})", raw)
    if match:
        month, day, hour, minute = map(int, match.groups())
        dt = _choose_year_for_month_day_v2(
            month,
            day,
            hour,
            minute,
            fallback_local=fallback_local,
            anchor_local=anchor_local,
        )
        return dt.astimezone(timezone.utc)

    match = re.search(r"(?<!\d)(\d{1,2}):(\d{2})(?!\d)", raw)
    if match:
        hour, minute = map(int, match.groups())
        base_local = anchor_local or fallback_local
        dt = base_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if anchor_local is not None and dt < anchor_local - timedelta(hours=6):
            dt = dt + timedelta(days=1)
        if anchor_local is None and dt.astimezone(timezone.utc) > fallback + FUTURE_TOLERANCE:
            dt = dt - timedelta(days=1)
        return dt.astimezone(timezone.utc)

    return fallback


def _resolve_node_timestamps_v2(nodes: list[dict[str, Any]], *, fallback: datetime) -> list[datetime]:
    indexed_nodes = list(enumerate(nodes))

    def sort_key(item: tuple[int, dict[str, Any]]) -> tuple[int, int]:
        original_index, node = item
        message_id = str(node.get("messageId") or "").strip()
        if message_id.isdigit():
            return (0, int(message_id))
        return (1, original_index)

    sorted_nodes = sorted(indexed_nodes, key=sort_key)
    resolved_sorted: list[datetime | None] = [None] * len(sorted_nodes)
    anchor: datetime | None = None
    for position, (_, node) in enumerate(sorted_nodes):
        raw = str(node.get("timestampHint") or "").strip()
        if not raw:
            continue
        parsed = _parse_timestamp_v2(raw, fallback=fallback, anchor=anchor)
        resolved_sorted[position] = parsed
        anchor = parsed

    next_known: datetime | None = None
    for position in range(len(sorted_nodes) - 1, -1, -1):
        if resolved_sorted[position] is not None:
            next_known = resolved_sorted[position]
            continue

        previous_known = None
        back = position - 1
        while back >= 0:
            if resolved_sorted[back] is not None:
                previous_known = resolved_sorted[back]
                break
            back -= 1

        if previous_known is not None and next_known is not None:
            candidate = min(previous_known + timedelta(seconds=1), next_known - timedelta(seconds=1))
        elif previous_known is not None:
            candidate = previous_known + timedelta(seconds=1)
        elif next_known is not None:
            candidate = next_known - timedelta(seconds=1)
        else:
            candidate = fallback
        resolved_sorted[position] = candidate

    resolved: list[datetime | None] = [None] * len(nodes)
    for (original_index, _), parsed in zip(sorted_nodes, resolved_sorted):
        resolved[original_index] = parsed

    return [item or fallback for item in resolved]


def _extract_links_from_text(text: str) -> list[str]:
    return re.findall(r"https?://[^\s<>\]\)\"']+", text)


def _is_doc_link(url: str) -> bool:
    return any(marker in url for marker in ("/docx/", "/wiki/", "/document/"))


def _is_report_candidate(text: str, links: list[str]) -> bool:
    blob = text.lower()
    if any(keyword.lower() in blob for keyword in EXTERNAL_GROUP_REPORT_KEYWORDS):
        return True
    if any(_is_doc_link(link) for link in links):
        return True
    # Task groups may not use "weekly report" words, but still carry executable items.
    task_markers = ("临时任务", "任务", "需求", "卡点", "禅道", "prd", "story-view")
    if any(marker in blob for marker in task_markers):
        if len(text) >= 30 or len(links) >= 1 or len(text.splitlines()) >= 2:
            return True
    if len(links) >= 1 and len(text) >= 30:
        return True
    return len(text.splitlines()) >= 4 and len(text) >= 80


def _fetch_document_text(context: BrowserContext, url: str) -> str:
    def _merge_segments(segments: list[str]) -> str:
        merged = ""
        for raw in segments:
            segment = str(raw or "").strip()
            if not segment:
                continue
            if not merged:
                merged = segment
                continue
            if segment in merged:
                continue
            overlap = 0
            max_len = min(len(merged), len(segment), 2000)
            for size in range(max_len, 80, -1):
                if merged[-size:] == segment[:size]:
                    overlap = size
                    break
            if overlap:
                merged = f"{merged}{segment[overlap:]}"
            else:
                merged = f"{merged}\n\n{segment}"
        return merged[:20000]

    def _collect_scrollable_text(page: Page, selector: str) -> str:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="attached", timeout=2000)
        except Exception:
            return ""

        segments: list[str] = []
        stagnant_rounds = 0
        last_top = -1
        for _ in range(12):
            try:
                text = locator.inner_text(timeout=2000).strip()
            except Exception:
                text = ""
            if text:
                segments.append(text)

            try:
                metrics = locator.evaluate(
                    """(el) => {
                        const nextTop = Math.min(
                            el.scrollTop + Math.max(700, el.clientHeight - 200),
                            Math.max(0, el.scrollHeight - el.clientHeight),
                        );
                        const beforeTop = el.scrollTop;
                        el.scrollTop = nextTop;
                        return {
                            beforeTop,
                            top: el.scrollTop,
                            clientHeight: el.clientHeight,
                            scrollHeight: el.scrollHeight,
                        };
                    }"""
                )
            except Exception:
                break

            page.wait_for_timeout(900)
            current_top = int(metrics.get("top", 0))
            if current_top == last_top:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            last_top = current_top
            if current_top + int(metrics.get("clientHeight", 0)) >= int(metrics.get("scrollHeight", 0)) - 5 and stagnant_rounds >= 1:
                break

        return _merge_segments(segments)

    page = context.new_page()
    page.set_default_timeout(PLAYWRIGHT_NAV_TIMEOUT_MS)
    try:
        page.goto(url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        page.wait_for_timeout(2000)

        for selector in (
            "div.bear-web-x-container",
            "[class*='docx-in-wiki']",
            "[class*='wiki'][class*='container']",
        ):
            collected = _collect_scrollable_text(page, selector)
            if len(collected) >= 200:
                return collected[:20000]

        for selector in (
            "article",
            "main",
            "[data-testid*='doc']",
            "[data-testid*='wiki']",
            "[class*='doc']",
            "[class*='wiki']",
            "body",
        ):
            locator = page.locator(selector).first
            try:
                text = locator.inner_text(timeout=2000).strip()
            except Exception:
                continue
            if len(text) >= 80:
                return text[:20000]
        try:
            return page.locator("body").inner_text(timeout=2000).strip()[:20000]
        except Exception:
            return ""
    finally:
        page.close()


def _normalize_report(
    context: BrowserContext,
    *,
    chat_id: str,
    node: dict[str, Any],
    current_chat_name: str = "",
    current_chat_url: str = "",
    collected_at: datetime,
    parsed_timestamp: datetime | None = None,
    fetch_documents: bool = True,
    include_all_messages: bool = False,
    existing_record: dict[str, Any] | None = None,
) -> tuple[CollectedReport | None, int]:
    text = str(node.get("text") or "").strip()
    links = list(dict.fromkeys([*node.get("links", []), *_extract_links_from_text(text)]))
    if not include_all_messages and not _is_report_candidate(text, links):
        return None, 0

    sender_name = _guess_sender_name(node)
    sender_id = hashlib.sha256(sender_name.encode("utf-8")).hexdigest()[:12]
    timestamp = parsed_timestamp or _parse_timestamp(str(node.get("timestampHint") or ""), fallback=collected_at)
    provisional_message_id = _stable_message_id(chat_id, node, timestamp, sender_name, text)
    doc_links = [link for link in links if _is_doc_link(link)]

    if existing_record and str(existing_record.get("parsed_text") or "").strip():
        can_reuse_existing = True
        if fetch_documents and doc_links:
            for link in doc_links:
                cached_document = get_document(_document_key_from_url(link))
                if not cached_document or not str(cached_document.get("content_text") or "").strip():
                    can_reuse_existing = False
                    break
        if can_reuse_existing:
            return CollectedReport(
                message_id=provisional_message_id,
                chat_id=chat_id,
                sender_id=sender_id,
                sender_name=sender_name,
                created_at=timestamp.isoformat(),
                title=str(existing_record.get("source_title") or f"{sender_name} 汇报"),
                body=str(existing_record.get("parsed_text") or ""),
                source_type=str(existing_record.get("source_type") or "external_cached_message"),
                raw_message={
                    "node": node,
                    "links": links,
                    "cached": True,
                    "chat_name": current_chat_name,
                    "chat_url": current_chat_url,
                },
            ), 0

    doc_texts: list[str] = []
    fetched_document_count = 0
    if fetch_documents:
        for link in links:
            if not _is_doc_link(link):
                continue
            document_key = _document_key_from_url(link)
            cached_document = get_document(document_key)
            if cached_document and str(cached_document.get("content_text") or "").strip():
                fetched = str(cached_document.get("content_text") or "").strip()
            else:
                try:
                    fetched = _fetch_document_text(context, link)
                    upsert_document(
                        document_key,
                        document_url=link,
                        document_type=document_key.split("_", 1)[0],
                        content_text=fetched,
                        content_hash=_hash_text(fetched) if fetched else "",
                        fetch_status="success" if fetched else "empty",
                        error_message="",
                    )
                    fetched_document_count += 1
                except Exception as exc:
                    upsert_document(
                        document_key,
                        document_url=link,
                        document_type=document_key.split("_", 1)[0],
                        content_text="",
                        content_hash="",
                        fetch_status="error",
                        error_message=str(exc),
                    )
                    fetched = ""
            if fetched:
                doc_texts.append(fetched)

    raw_content_text = text
    if links:
        link_block = "\n".join(links)
        if link_block not in raw_content_text:
            raw_content_text = f"{raw_content_text}\n{link_block}".strip()

    payload = build_source_payload(
        {
            "message_id": str(node.get("messageId") or ""),
            "chat_id": chat_id,
            "message_type": "text",
            "content": json.dumps({"text": raw_content_text}, ensure_ascii=False),
        }
    )

    title = str(payload.get("title") or "").strip() or f"{sender_name} 汇报"
    body_parts = [str(payload.get("body") or "").strip(), *[part.strip() for part in doc_texts if part.strip()]]
    body = "\n\n".join(part for part in body_parts if part)
    if not body:
        return None, fetched_document_count

    source_type = "external_document_message" if doc_texts else f"external_{payload.get('source_type') or 'text_message'}"
    message_id = provisional_message_id

    return CollectedReport(
        message_id=message_id,
        chat_id=chat_id,
        sender_id=sender_id,
        sender_name=sender_name,
        created_at=timestamp.isoformat(),
        title=title,
        body=body[:20000],
        source_type=source_type,
        raw_message={
            "node": node,
            "links": links,
            "doc_text_lengths": [len(part) for part in doc_texts],
            "chat_name": current_chat_name,
            "chat_url": current_chat_url,
        },
    ), fetched_document_count


def _snapshot_messages(page: Page) -> list[dict[str, Any]]:
    items = page.evaluate(MESSAGE_NODE_SNAPSHOT_JS)
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _scroll_and_collect(
    page: Page,
    *,
    scan_limit: int,
    scroll_iterations: int,
    since_dt: datetime | None = None,
    fallback_now: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    consecutive_no_new_batches = 0
    aggressive_scrolls = 0
    oldest_seen_ts: datetime | None = None
    newest_seen_ts: datetime | None = None
    fallback_time = fallback_now or datetime.now(timezone.utc)

    max_idle_batches = max(120, min(scroll_iterations, 400))
    aggressive_every = 8
    for _ in range(scroll_iterations):
        batch = _snapshot_messages(page)
        new_in_batch = 0
        for item in batch:
            key = str(item.get("messageId") or "") or hashlib.sha256(
                f"{item.get('text', '')}|{item.get('top', 0)}".encode("utf-8")
            ).hexdigest()
            if key in seen_keys:
                continue
            seen_keys.add(key)
            collected.append(item)
            new_in_batch += 1
            parsed_ts = _parse_timestamp_v2(str(item.get("timestampHint") or ""), fallback=fallback_time)
            if oldest_seen_ts is None or parsed_ts < oldest_seen_ts:
                oldest_seen_ts = parsed_ts
            if newest_seen_ts is None or parsed_ts > newest_seen_ts:
                newest_seen_ts = parsed_ts
            if len(collected) >= scan_limit:
                return collected, {
                    "oldest_seen_timestamp": oldest_seen_ts.isoformat() if oldest_seen_ts else "",
                    "newest_seen_timestamp": newest_seen_ts.isoformat() if newest_seen_ts else "",
                    "consecutive_no_new_batches": consecutive_no_new_batches,
                    "aggressive_scrolls": aggressive_scrolls,
                }

        if new_in_batch == 0:
            consecutive_no_new_batches += 1
        else:
            consecutive_no_new_batches = 0

        use_aggressive_scroll = consecutive_no_new_batches > 0 and consecutive_no_new_batches % aggressive_every == 0
        scroll_script = AGGRESSIVE_SCROLL_PAGE_JS if use_aggressive_scroll else SCROLL_PAGE_JS
        changed = int(page.evaluate(scroll_script) or 0)
        if use_aggressive_scroll:
            aggressive_scrolls += 1
        page.wait_for_timeout(1800 if use_aggressive_scroll else (1100 if changed else 800))
        if since_dt is not None and oldest_seen_ts is not None and oldest_seen_ts <= since_dt and consecutive_no_new_batches >= 2:
            break
        if consecutive_no_new_batches >= max_idle_batches and not changed:
            break
    return collected, {
        "oldest_seen_timestamp": oldest_seen_ts.isoformat() if oldest_seen_ts else "",
        "newest_seen_timestamp": newest_seen_ts.isoformat() if newest_seen_ts else "",
        "consecutive_no_new_batches": consecutive_no_new_batches,
        "aggressive_scrolls": aggressive_scrolls,
    }


def _current_chat_name(page: Page) -> str:
    selectors = [
        "header [class*='title']",
        "header [data-testid*='title']",
        "[class*='chat-header'] [class*='title']",
        "[class*='ChatHeader'] [class*='title']",
        "[class*='header'] [class*='title']",
        "text=/外部/",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            text = locator.inner_text(timeout=1200).strip()
        except Exception:
            continue
        if not text:
            continue
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for candidate in lines:
            if candidate in {"外部", "内部", "机器人"}:
                continue
            if candidate.isdigit():
                continue
            if candidate and len(candidate) <= 80:
                return candidate
        if text and len(text) <= 80:
            return text
    return ""


def _find_sidebar_search_box(page: Page):
    selectors = [
        "input[placeholder*='搜索']",
        "input[placeholder*='Search']",
        "[role='searchbox']",
        "input[type='search']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(timeout=1200)
            return locator
        except Exception:
            continue
    return None


def _open_global_search_dialog(page: Page) -> bool:
    trigger = page.locator("div.appNavbar-search-input").first
    try:
        trigger.click(timeout=1500)
        page.wait_for_timeout(800)
        return True
    except Exception:
        return False


def _find_global_search_editor(page: Page):
    selectors = [
        "#search_bar_editor [contenteditable='true']",
        "#search_bar_editor .zone-container[contenteditable='true']",
        "[contenteditable='true'][data-slate-editor='true']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(timeout=1200)
            return locator
        except Exception:
            continue
    return None


def _perform_sidebar_search(page: Page, term: str) -> bool:
    if not _open_global_search_dialog(page):
        return False
    editor = _find_global_search_editor(page)
    if editor is None:
        return False
    try:
        editor.click(timeout=1200, force=True)
        page.evaluate(
            """
            () => {
              const el = document.querySelector('#search_bar_editor [contenteditable="true"]')
                || document.querySelector('#search_bar_editor .zone-container[contenteditable="true"]')
                || document.querySelector('[contenteditable="true"][data-slate-editor="true"]');
              if (!el) return;
              el.focus();
              el.textContent = '';
              el.dispatchEvent(new InputEvent('input', { bubbles: true, data: '', inputType: 'deleteContentBackward' }));
            }
            """
        )
        page.keyboard.insert_text(term)
        page.wait_for_timeout(2200)
        return True
    except Exception:
        return False


def _clear_sidebar_search(page: Page) -> None:
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(600)
    except Exception:
        pass


def _snapshot_global_search_group_results(page: Page) -> list[dict[str, Any]]:
    try:
        page.locator('button[data-tab-key="search-tab-4"]').first.click(timeout=1500)
        page.wait_for_timeout(1200)
    except Exception:
        return []
    return page.evaluate(
        """
        () => {
          return Array.from(document.querySelectorAll('.group-chat-card')).map((card, index) => {
            const title = (card.querySelector('.chat-text-title')?.innerText || '').trim();
            const tags = Array.from(card.querySelectorAll('.search-tags .search-tag'))
              .map((el) => (el.innerText || '').trim())
              .filter(Boolean);
            const meta = (card.querySelector('.meta-segment')?.innerText || '').trim();
            return {
              feedId: `search_group_${index}_${title}`,
              text: [title, ...tags, meta].filter(Boolean).join('\\n'),
              active: card.classList.contains('active'),
              top: Math.round(card.getBoundingClientRect().top),
              height: Math.round(card.getBoundingClientRect().height),
            };
          }).filter((item) => item.text);
        }
        """
    )


def _find_chat_list_scroller(page: Page):
    selectors = [
        "div.scroller.feed-main-list",
        "div.scroller.a11y_feed_main_list",
        "div.lark_feedMainList",
        "div[class*='feed-main-list']",
        "div[class*='feedMainList']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(timeout=1500)
            return locator
        except Exception:
            continue
    try:
        handle = page.evaluate_handle(
            """
            () => {
              const feed = document.querySelector('[data-feed-id]');
              if (!feed) return null;
              let node = feed.parentElement;
              while (node) {
                const style = window.getComputedStyle(node);
                const scrollable = style.overflowY === 'auto' || style.overflowY === 'scroll';
                if (scrollable && node.scrollHeight - node.clientHeight > 120) return node;
                node = node.parentElement;
              }
              return null;
            }
            """
        )
        if str(handle) != "JSHandle:null":
            return handle.as_element()
    except Exception:
        pass
    return None


def _snapshot_sidebar_chats(page: Page) -> list[dict[str, Any]]:
    return page.evaluate(SIDEBAR_FEED_SNAPSHOT_JS)


def _discover_sidebar_chats(page: Page, *, scroll_iterations: int) -> list[DiscoveredChat]:
    scroller = _find_chat_list_scroller(page)
    if scroller is None:
        discovered: dict[str, DiscoveredChat] = {}
        for payload in _snapshot_sidebar_chats(page):
            chat = _normalize_discovered_chat(payload)
            if chat is None:
                continue
            discovered.setdefault(chat.feed_id, chat)
        return list(discovered.values())
    try:
        scroller.evaluate("(node) => { node.scrollTop = 0; }")
        page.wait_for_timeout(800)
    except Exception:
        pass

    discovered: dict[str, DiscoveredChat] = {}
    stagnant_rounds = 0
    last_scroll_top = -1
    iterations = max(1, scroll_iterations)

    for _ in range(iterations):
        for payload in _snapshot_sidebar_chats(page):
            chat = _normalize_discovered_chat(payload)
            if chat is None:
                continue
            discovered.setdefault(chat.feed_id, chat)

        scroll_state = scroller.evaluate(
            """
            (node) => {
              const before = node.scrollTop;
              const step = Math.max(520, Math.floor(node.clientHeight * 0.85));
              const maxTop = Math.max(0, node.scrollHeight - node.clientHeight);
              node.scrollTop = Math.min(maxTop, before + step);
              return { before, after: node.scrollTop, maxTop };
            }
            """
        )
        page.wait_for_timeout(900 if scroll_state["after"] != scroll_state["before"] else 400)

        if scroll_state["after"] == scroll_state["before"] or scroll_state["after"] == last_scroll_top:
            stagnant_rounds += 1
            if stagnant_rounds >= 2:
                break
        else:
            stagnant_rounds = 0
        last_scroll_top = scroll_state["after"]

    return list(discovered.values())


def _merge_discovered_chats(discovered: list[DiscoveredChat], payloads: list[dict[str, Any]], seen: set[str]) -> None:
    for payload in payloads:
        chat = _normalize_discovered_chat(payload)
        if chat is None or chat.feed_id in seen:
            continue
        discovered.append(chat)
        seen.add(chat.feed_id)


def discover_visible_chats(
    *,
    messenger_url: str | None = None,
    headed: bool = False,
    storage_state_path: str | None = None,
    scroll_iterations: int = 24,
    include_bots: bool = False,
    include_direct: bool = False,
    search_terms: list[str] | None = None,
    search_exhaustive: bool = False,
    search_limit: int = 0,
) -> dict[str, Any]:
    state_path = _storage_state_path(storage_state_path)
    _validate_storage_state_or_raise(state_path)

    with sync_playwright() as playwright:
        context = _launch_context(playwright, headed=headed, storage_state_path=state_path)
        page = context.new_page()
        page.set_default_timeout(PLAYWRIGHT_NAV_TIMEOUT_MS)
        target_url = (messenger_url or DEFAULT_MESSENGER_URL).strip()
        page.goto(target_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        page.wait_for_timeout(2500)
        _ensure_messenger_page_or_raise(page, context_label="discover")
        _wait_for_sidebar_feed_ready(page, timeout_seconds=60)
        discovered = _discover_sidebar_chats(page, scroll_iterations=scroll_iterations)

        search_queue: list[str] = []
        if search_terms:
            search_queue.extend(search_terms)
        if search_exhaustive:
            search_queue.extend(DEFAULT_SEARCH_BUCKETS)

        seen_feed_ids = {chat.feed_id for chat in discovered}
        for term in search_queue:
            if not term.strip():
                continue
            if search_limit and len(seen_feed_ids) >= search_limit:
                break
            if not _perform_sidebar_search(page, term):
                continue
            payloads = _snapshot_global_search_group_results(page)
            _merge_discovered_chats(discovered, payloads, seen_feed_ids)
            _clear_sidebar_search(page)

        context.close()

    selected: list[DiscoveredChat] = []
    seen_titles: set[str] = set()
    for chat in discovered:
        if chat.is_bot and not include_bots:
            continue
        if not chat.is_group_candidate and not include_direct:
            continue
        if chat.title in seen_titles:
            continue
        seen_titles.add(chat.title)
        selected.append(chat)

    selected.sort(key=lambda item: item.title.lower())
    return {
        "messenger_url": target_url,
        "discovered_count": len(discovered),
        "selected_count": len(selected),
        "chats": [chat.to_dict() for chat in selected],
    }


def _matches_selected_chat(page: Page, target: str) -> bool:
    current = _current_chat_name(page)
    return bool(current and (target in current or current in target))


def _try_click_visible_feed_card(page: Page, group_name: str, feed_id: str | None = None) -> bool:
    target = group_name.strip()
    feed_target = str(feed_id or "").strip()
    if not target and not feed_target:
        return False
    scroller = _find_chat_list_scroller(page)
    if scroller is not None:
        try:
            scroller.evaluate("(node) => { node.scrollTop = 0; }")
            page.wait_for_timeout(700)
        except Exception:
            pass

    stagnant_rounds = 0
    last_scroll_top = -1
    for _ in range(24):
        try:
            clicked = bool(
                page.evaluate(
                    """
                    ({ target, feedId }) => {
                      const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                      if (feedId) {
                        const exact = document.querySelector(`[data-feed-id="${feedId}"]`);
                        if (exact) {
                          const card = exact.closest('.a11y_feed_card_item') || exact;
                          card.scrollIntoView({ block: 'center' });
                          card.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                          card.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                          card.click();
                          return true;
                        }
                      }
                      const score = (title, text) => {
                        if (!title && !text) return 0;
                        if (title === target) return 100;
                        if (text === target) return 90;
                        if (title.includes(target)) return 80;
                        if (text.includes(target)) return 70;
                        return 0;
                      };

                      const cards = Array.from(document.querySelectorAll('[data-feed-id]'))
                        .map((node) => {
                          const card = node.closest('.a11y_feed_card_item') || node;
                          const text = normalize(node.innerText || card.innerText || '');
                          const lines = text.split(/\\n+/).map(normalize).filter(Boolean);
                          const title = lines[0] || '';
                          return { card, title, text, score: score(title, text) };
                        })
                        .filter((item) => item.text && item.score > 0)
                        .sort((a, b) => b.score - a.score);

                      if (!cards.length) return false;
                      const best = cards[0].card;
                      best.scrollIntoView({ block: 'center' });
                      best.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                      best.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                      best.click();
                      return true;
                    }
                    """,
                    {"target": target, "feedId": feed_target},
                )
            )
        except Exception:
            clicked = False
        if clicked:
            page.wait_for_timeout(1800)
            if target:
                if _matches_selected_chat(page, target):
                    return True
            else:
                current_name = _current_chat_name(page)
                if current_name and current_name not in {"外部", "内部", "机器人"}:
                    return True

        if scroller is None:
            break
        try:
            scroll_state = scroller.evaluate(
                """
                (node) => {
                  const before = node.scrollTop;
                  const step = Math.max(520, Math.floor(node.clientHeight * 0.85));
                  const maxTop = Math.max(0, node.scrollHeight - node.clientHeight);
                  node.scrollTop = Math.min(maxTop, before + step);
                  return { before, after: node.scrollTop };
                }
                """
            )
        except Exception:
            break
        page.wait_for_timeout(700 if scroll_state["after"] != scroll_state["before"] else 350)
        if scroll_state["after"] == scroll_state["before"] or scroll_state["after"] == last_scroll_top:
            stagnant_rounds += 1
            if stagnant_rounds >= 2:
                break
        else:
            stagnant_rounds = 0
        last_scroll_top = scroll_state["after"]
    return False


def _try_click_global_search_result(page: Page, group_name: str) -> bool:
    target = group_name.strip()
    if not target:
        return False
    if not _perform_sidebar_search(page, target):
        return False
    try:
        try:
            page.locator('button[data-tab-key="search-tab-4"]').first.click(timeout=1500)
            page.wait_for_timeout(1000)
        except Exception:
            pass

        locators = [
            page.locator(".group-chat-card").filter(has_text=target).first,
            page.get_by_text(target, exact=True).last,
            page.locator(f"text={target}").last,
        ]
        for locator in locators:
            try:
                locator.click(timeout=2500)
                page.wait_for_timeout(1800)
                if _matches_selected_chat(page, target):
                    return True
            except Exception:
                continue
        return False
    finally:
        _clear_sidebar_search(page)


def _try_select_group_by_name(page: Page, group_name: str, feed_id: str | None = None) -> bool:
    target = group_name.strip()
    if not target and not str(feed_id or "").strip():
        return False

    if target:
        search_box = _find_sidebar_search_box(page)
        if search_box is not None:
            try:
                search_box.click(timeout=1500)
                search_box.fill(target, timeout=2000)
                page.wait_for_timeout(1200)
            except Exception:
                pass

    locators = [
        page.get_by_text(target, exact=True).first,
        page.locator(f"text={target}").first,
    ]
    for locator in locators:
        try:
            locator.click(timeout=2500)
            page.wait_for_timeout(1800)
            if _matches_selected_chat(page, target):
                return True
        except Exception:
            continue

    if _try_click_visible_feed_card(page, target, feed_id=feed_id):
        return True
    if target and _try_click_global_search_result(page, target):
        return True
    return False


def _open_target_chat(
    page: Page,
    *,
    group_url: str | None,
    messenger_url: str | None,
    group_name: str | None,
    feed_id: str | None,
    use_current_chat: bool,
    wait_seconds: int = 0,
) -> tuple[str, str]:
    if group_url:
        page.goto(group_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        page.wait_for_timeout(2500)
        return page.url, _current_chat_name(page)

    target_url = (messenger_url or DEFAULT_MESSENGER_URL).strip()
    page.goto(target_url, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass
    page.wait_for_timeout(2500)
    _ensure_messenger_page_or_raise(page, context_label="打开目标群")
    _wait_for_sidebar_feed_ready(page, timeout_seconds=60)

    if group_name or feed_id:
        if not _try_select_group_by_name(page, group_name or "", feed_id=feed_id):
            raise RuntimeError(f"没有在 messenger 中找到群：{group_name}")

    if use_current_chat or not group_name:
        if wait_seconds > 0:
            print(f"请在 {wait_seconds} 秒内把浏览器切到目标群，倒计时结束后自动继续采集。")
            page.wait_for_timeout(wait_seconds * 1000)
        else:
            print("请确认 Playwright 打开的窗口里已经选中目标群，然后按 Enter 继续。")
            input()
            page.wait_for_timeout(1000)

    return page.url, _current_chat_name(page)


def collect_group_reports(
    *,
    group_url: str | None = None,
    messenger_url: str | None = None,
    group_name: str | None = None,
    feed_id: str | None = None,
    use_current_chat: bool = False,
    chat_id: str | None = None,
    hours: int,
    headed: bool = False,
    storage_state_path: str | None = None,
    inspect_output: str | None = None,
    fetch_documents: bool = True,
    include_all_messages: bool = False,
    summarize_after_collect: bool = False,
    run_id: str | None = None,
    wait_seconds: int = 0,
) -> dict[str, Any]:
    state_path = _storage_state_path(storage_state_path)
    _validate_storage_state_or_raise(state_path)

    start_at, end_at = default_window(hours)
    collected_at = datetime.now(timezone.utc)
    fallback_timestamp = end_at
    effective_run_id = run_id or uuid4().hex

    with sync_playwright() as playwright:
        context = _launch_context(playwright, headed=headed, storage_state_path=state_path)
        page = context.new_page()
        page.set_default_timeout(PLAYWRIGHT_NAV_TIMEOUT_MS)
        current_url, current_chat_name = _open_target_chat(
            page,
            group_url=group_url,
            messenger_url=messenger_url,
            group_name=group_name,
            feed_id=feed_id,
            use_current_chat=use_current_chat,
            wait_seconds=wait_seconds,
        )
        # Ensure collection starts from the latest visible messages.
        for _ in range(2):
            try:
                page.evaluate(SCROLL_TO_BOTTOM_JS)
            except Exception:
                pass
            page.wait_for_timeout(500)
        effective_chat_id = _stable_chat_id(
            explicit_chat_id=chat_id,
            requested_group_name=group_name or "",
            current_chat_name=current_chat_name,
            current_url=current_url,
        )
        create_collection_run(
            effective_run_id,
            chat_id=effective_chat_id,
            since_timestamp=start_at.isoformat(),
        )

        try:
            nodes, scroll_meta = _scroll_and_collect(
                page,
                scan_limit=EXTERNAL_GROUP_MESSAGE_SCAN_LIMIT,
                scroll_iterations=EXTERNAL_GROUP_SCROLL_ITERATIONS,
                since_dt=start_at,
                fallback_now=collected_at,
            )
            resolved_timestamps = _resolve_collected_timestamps(nodes, fallback=fallback_timestamp)
            if inspect_output:
                inspect_path = Path(inspect_output).expanduser()
                inspect_path.parent.mkdir(parents=True, exist_ok=True)
                inspect_nodes = []
                for node, resolved_timestamp in zip(nodes, resolved_timestamps):
                    payload = dict(node)
                    payload["resolvedTimestamp"] = resolved_timestamp.isoformat()
                    inspect_nodes.append(payload)
                inspect_path.write_text(json.dumps(inspect_nodes, ensure_ascii=False, indent=2), encoding="utf-8")

            normalized: list[CollectedReport] = []
            new_message_ids: list[str] = []
            updated_message_ids: list[str] = []
            fetched_documents_total = 0
            new_messages = 0
            updated_messages = 0
            for node, parsed_timestamp in zip(nodes, resolved_timestamps):
                if parsed_timestamp < start_at or parsed_timestamp > end_at:
                    continue
                sender_name = _guess_sender_name(node)
                provisional_message_id = _stable_message_id(
                    effective_chat_id,
                    node,
                    parsed_timestamp,
                    sender_name,
                    str(node.get("text") or ""),
                )
                existing_record = get_record(provisional_message_id)
                report = _normalize_report(
                    context,
                    chat_id=effective_chat_id,
                    node=node,
                    current_chat_name=current_chat_name,
                    current_chat_url=current_url,
                    collected_at=collected_at,
                    parsed_timestamp=parsed_timestamp,
                    fetch_documents=fetch_documents,
                    include_all_messages=include_all_messages,
                    existing_record=existing_record,
                )
                normalized_report, fetched_document_count = report
                fetched_documents_total += fetched_document_count
                if normalized_report is None:
                    continue
                if existing_record is None:
                    new_messages += 1
                    new_message_ids.append(normalized_report.message_id)
                else:
                    updated_messages += 1
                    updated_message_ids.append(normalized_report.message_id)
                normalized.append(normalized_report)
        finally:
            context.close()

    try:
        for report in normalized:
            upsert_message(
                report.message_id,
                chat_id=report.chat_id,
                sender_id=report.sender_id,
                reporter_name=report.sender_name,
                message_type="text",
                raw_event={"source": "external_group_collector"},
                raw_message=report.raw_message,
                status="received",
                created_at=report.created_at,
                message_timestamp=report.created_at,
                collected_at=collected_at.isoformat(),
                is_backfill=True,
                run_id=effective_run_id,
            )
            set_parsed_content(
                report.message_id,
                source_type=report.source_type,
                source_title=report.title,
                parsed_text=report.body,
                source_content_hash=_hash_text(report.body),
                reporter_name=report.sender_name,
                status="parsed",
            )
        finish_collection_run(
            effective_run_id,
            new_messages=new_messages,
            updated_messages=updated_messages,
            fetched_documents=fetched_documents_total,
            status="completed",
        )

        report_payload: dict[str, Any] = {}
        if summarize_after_collect:
            report_payload = build_group_summary_report(
                effective_chat_id,
                start_at=start_at,
                end_at=end_at,
            )
        report_payload.update(
            {
                "run_id": effective_run_id,
                "chat_id": effective_chat_id,
                "current_chat_name": current_chat_name,
                "current_url": current_url,
                "ingested_count": len(normalized),
                "new_messages": new_messages,
                "updated_messages": updated_messages,
                "new_message_ids": new_message_ids,
                "updated_message_ids": updated_message_ids,
                "collected_message_ids": [report.message_id for report in normalized],
                "fetched_documents": fetched_documents_total,
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "oldest_seen_timestamp": scroll_meta.get("oldest_seen_timestamp", ""),
                "newest_seen_timestamp": scroll_meta.get("newest_seen_timestamp", ""),
                "consecutive_no_new_batches": scroll_meta.get("consecutive_no_new_batches", 0),
            }
        )
        return report_payload
    except Exception as exc:
        finish_collection_run(
            effective_run_id,
            new_messages=new_messages,
            updated_messages=updated_messages,
            fetched_documents=fetched_documents_total,
            status="failed",
            error_message=str(exc),
        )
        raise


def batch_collect_visible_chats(
    *,
    messenger_url: str | None = None,
    hours: int,
    headed: bool = False,
    storage_state_path: str | None = None,
    scroll_iterations: int = 24,
    include_bots: bool = False,
    include_direct: bool = False,
    search_terms: list[str] | None = None,
    search_exhaustive: bool = False,
    search_limit: int = 0,
    limit: int = 0,
    fetch_documents: bool = True,
    include_all_messages: bool = False,
    summarize_after_collect: bool = False,
) -> dict[str, Any]:
    discovered = discover_visible_chats(
        messenger_url=messenger_url,
        headed=headed,
        storage_state_path=storage_state_path,
        scroll_iterations=scroll_iterations,
        include_bots=include_bots,
        include_direct=include_direct,
        search_terms=search_terms,
        search_exhaustive=search_exhaustive,
        search_limit=search_limit,
    )

    chats = list(discovered["chats"])
    if limit > 0:
        chats = chats[:limit]

    results: list[dict[str, Any]] = []
    for chat in chats:
        title = str(chat.get("title") or "").strip()
        if not title:
            continue
        try:
            payload = collect_group_reports(
                messenger_url=messenger_url,
                group_name=title,
                use_current_chat=False,
                chat_id=None,
                hours=hours,
                headed=headed,
                storage_state_path=storage_state_path,
                fetch_documents=fetch_documents,
                include_all_messages=include_all_messages,
                summarize_after_collect=summarize_after_collect,
            )
            results.append(
                {
                    "title": title,
                    "badge": chat.get("badge", ""),
                    "feed_id": chat.get("feed_id", ""),
                    "status": "completed",
                    "run_id": payload.get("run_id", ""),
                    "chat_id": payload.get("chat_id", ""),
                    "ingested_count": payload.get("ingested_count", 0),
                    "new_messages": payload.get("new_messages", 0),
                    "updated_messages": payload.get("updated_messages", 0),
                    "fetched_documents": payload.get("fetched_documents", 0),
                    "current_url": payload.get("current_url", ""),
                }
            )
        except Exception as exc:
            results.append(
                {
                    "title": title,
                    "badge": chat.get("badge", ""),
                    "feed_id": chat.get("feed_id", ""),
                    "status": "failed",
                    "error": str(exc),
                }
            )

    return {
        "messenger_url": discovered["messenger_url"],
        "discovered_count": discovered["discovered_count"],
        "selected_count": len(chats),
        "completed_count": sum(1 for item in results if item["status"] == "completed"),
        "failed_count": sum(1 for item in results if item["status"] == "failed"),
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect external Lark group reports with Playwright.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    login_parser = subparsers.add_parser("login", help="Open Lark Web in headed mode and save login state.")
    login_parser.add_argument("--storage-state", default=PLAYWRIGHT_STORAGE_STATE_PATH)
    login_parser.add_argument("--login-url", default=DEFAULT_MESSENGER_URL)
    login_parser.add_argument("--wait-seconds", type=int, default=180)

    discover_parser = subparsers.add_parser("discover", help="List visible chats from the current Lark messenger account.")
    discover_parser.add_argument("--messenger-url", default=DEFAULT_MESSENGER_URL)
    discover_parser.add_argument("--headed", action="store_true")
    discover_parser.add_argument("--storage-state", default=PLAYWRIGHT_STORAGE_STATE_PATH)
    discover_parser.add_argument("--scroll-iterations", type=int, default=24)
    discover_parser.add_argument("--include-bots", action="store_true")
    discover_parser.add_argument("--include-direct", action="store_true")
    discover_parser.add_argument("--inspect-output", default="")
    discover_parser.add_argument("--search-term", action="append", default=[])
    discover_parser.add_argument("--search-exhaustive", action="store_true")
    discover_parser.add_argument("--search-limit", type=int, default=0)

    collect_parser = subparsers.add_parser("collect", help="Collect reports from an external Lark group and persist them.")
    collect_parser.add_argument("--group-url", default="")
    collect_parser.add_argument("--messenger-url", default=DEFAULT_MESSENGER_URL)
    collect_parser.add_argument("--group-name", default="")
    collect_parser.add_argument("--feed-id", default="")
    collect_parser.add_argument("--use-current-chat", action="store_true")
    collect_parser.add_argument("--chat-id", default="")
    collect_parser.add_argument("--hours", type=int, default=72)
    collect_parser.add_argument("--headed", action="store_true")
    collect_parser.add_argument("--storage-state", default=PLAYWRIGHT_STORAGE_STATE_PATH)
    collect_parser.add_argument("--inspect-output", default="")
    collect_parser.add_argument("--skip-document-fetch", action="store_true")
    collect_parser.add_argument("--all-messages", action="store_true")
    collect_parser.add_argument("--wait-seconds", type=int, default=0)
    collect_parser.add_argument("--summarize", action="store_true")
    collect_parser.add_argument("--send-custom-bot", action="store_true")

    batch_parser = subparsers.add_parser("batch-collect", help="Discover visible chats and collect them one by one.")
    batch_parser.add_argument("--messenger-url", default=DEFAULT_MESSENGER_URL)
    batch_parser.add_argument("--hours", type=int, default=4000)
    batch_parser.add_argument("--headed", action="store_true")
    batch_parser.add_argument("--storage-state", default=PLAYWRIGHT_STORAGE_STATE_PATH)
    batch_parser.add_argument("--scroll-iterations", type=int, default=24)
    batch_parser.add_argument("--include-bots", action="store_true")
    batch_parser.add_argument("--include-direct", action="store_true")
    batch_parser.add_argument("--search-term", action="append", default=[])
    batch_parser.add_argument("--search-exhaustive", action="store_true")
    batch_parser.add_argument("--search-limit", type=int, default=0)
    batch_parser.add_argument("--limit", type=int, default=0)
    batch_parser.add_argument("--skip-document-fetch", action="store_true")
    batch_parser.add_argument("--all-messages", action="store_true")
    batch_parser.add_argument("--summarize", action="store_true")
    batch_parser.add_argument("--inspect-output", default="")

    summarize_parser = subparsers.add_parser("summarize", help="Build text/json report from already collected records.")
    summarize_parser.add_argument("--chat-id", required=True)
    summarize_parser.add_argument("--hours", type=int, default=72)
    summarize_parser.add_argument("--send-custom-bot", action="store_true")

    args = parser.parse_args()

    if args.command == "login":
        path = save_login_state(
            args.storage_state,
            login_url=args.login_url,
            wait_seconds=args.wait_seconds,
        )
        print(f"登录态已保存：{path}")
        return 0

    if args.command == "discover":
        payload = discover_visible_chats(
            messenger_url=args.messenger_url or None,
            headed=args.headed,
            storage_state_path=args.storage_state,
            scroll_iterations=args.scroll_iterations,
            include_bots=bool(args.include_bots),
            include_direct=bool(args.include_direct),
            search_terms=args.search_term or None,
            search_exhaustive=bool(args.search_exhaustive),
            search_limit=args.search_limit,
        )
        if args.inspect_output:
            inspect_path = Path(args.inspect_output).expanduser()
            inspect_path.parent.mkdir(parents=True, exist_ok=True)
            inspect_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.command == "summarize":
        start_at, end_at = default_window(args.hours)
        payload = build_group_summary_report(args.chat_id, start_at=start_at, end_at=end_at)
        payload.update(
            {
                "chat_id": args.chat_id,
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
            }
        )
        print(f"Chat ID: {payload['chat_id']}")
        print(f"People: {payload['people_count']}")
        print(f"Report text path: {payload['text_path']}")
        print(f"Report json path: {payload['json_path']}")
        if args.send_custom_bot:
            send_custom_bot_text(str(payload["summary"]))
            print("已通过自定义机器人发送报告文本。")
        return 0

    if args.command == "batch-collect":
        payload = batch_collect_visible_chats(
            messenger_url=args.messenger_url or None,
            hours=args.hours,
            headed=args.headed,
            storage_state_path=args.storage_state,
            scroll_iterations=args.scroll_iterations,
            include_bots=bool(args.include_bots),
            include_direct=bool(args.include_direct),
            search_terms=args.search_term or None,
            search_exhaustive=bool(args.search_exhaustive),
            search_limit=args.search_limit,
            limit=args.limit,
            fetch_documents=not args.skip_document_fetch,
            include_all_messages=bool(args.all_messages),
            summarize_after_collect=bool(args.summarize),
        )
        if args.inspect_output:
            inspect_path = Path(args.inspect_output).expanduser()
            inspect_path.parent.mkdir(parents=True, exist_ok=True)
            inspect_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    payload = collect_group_reports(
        group_url=args.group_url or None,
        messenger_url=args.messenger_url or None,
        group_name=args.group_name or None,
        feed_id=args.feed_id or None,
        use_current_chat=bool(args.use_current_chat),
        chat_id=args.chat_id or None,
        hours=args.hours,
        headed=args.headed,
        storage_state_path=args.storage_state,
        inspect_output=args.inspect_output or None,
        fetch_documents=not args.skip_document_fetch,
        include_all_messages=bool(args.all_messages),
        wait_seconds=args.wait_seconds,
        summarize_after_collect=bool(args.summarize),
    )

    print(f"Bot: {BOT_DISPLAY_NAME}")
    print(f"Run ID: {payload['run_id']}")
    print(f"Chat ID: {payload['chat_id']}")
    print(f"Current chat: {payload['current_chat_name']}")
    print(f"Current URL: {payload['current_url']}")
    print(f"Ingested: {payload['ingested_count']}")
    print(f"New messages: {payload['new_messages']}")
    print(f"Updated messages: {payload['updated_messages']}")
    print(f"Fetched documents: {payload['fetched_documents']}")
    if payload.get("people_count") is not None:
        print(f"People: {payload['people_count']}")
    if payload.get("text_path"):
        print(f"Report text path: {payload['text_path']}")
    if payload.get("json_path"):
        print(f"Report json path: {payload['json_path']}")

    if args.send_custom_bot and payload.get("summary"):
        send_custom_bot_text(str(payload["summary"]))
        print("已通过自定义机器人发送报告文本。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
