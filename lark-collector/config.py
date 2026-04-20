from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Model provider
MODEL_PROVIDER = os.getenv("MODEL_PROVIDER", "anthropic").strip().lower()
MODEL_NAME = os.getenv("MODEL_NAME", os.getenv("CLAUDE_MODEL", "claude-opus-4-1")).strip()
REVIEW_MODEL = os.getenv("REVIEW_MODEL", MODEL_NAME).strip()
MODEL_REASONING_EFFORT = os.getenv("MODEL_REASONING_EFFORT", "high").strip().lower()
GROUP_SUMMARY_REASONING_EFFORT = os.getenv("GROUP_SUMMARY_REASONING_EFFORT", "low").strip().lower()
API_TIMEOUT = int(os.getenv("API_TIMEOUT", 120))
PLAYWRIGHT_STORAGE_STATE_MAX_AGE_HOURS = int(os.getenv("PLAYWRIGHT_STORAGE_STATE_MAX_AGE_HOURS", 720))

# Anthropic / Claude / proxy
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_BASE_URL = os.getenv("ANTHROPIC_BASE_URL", "").strip().rstrip("/")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", MODEL_NAME).strip()
CLAUDE_AUTH_MODE = os.getenv("CLAUDE_AUTH_MODE", "auto").strip().lower()

# OpenAI-compatible provider
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "").strip().rstrip("/")
OPENAI_WIRE_API = os.getenv("OPENAI_WIRE_API", "responses").strip().lower()

# Feishu app credentials
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "").strip()
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "").strip()
FEISHU_VERIFICATION_TOKEN = os.getenv("FEISHU_VERIFICATION_TOKEN", "").strip()
FEISHU_ENCRYPT_KEY = os.getenv("FEISHU_ENCRYPT_KEY", "").strip()
BOT_DISPLAY_NAME = os.getenv("BOT_DISPLAY_NAME", "日周报分析助手").strip()

# Intake
SUPPORTED_MESSAGE_TYPES = ["text", "post", "interactive", "file", "image"]
TRIGGER_KEYWORDS = [
    "周报",
    "月报",
    "工作总结",
    "汇报",
    "会议纪要",
    "会议总结",
    "会议记录",
    "周会",
    "复盘会",
    "产研周报",
    "风控周报",
    "SEO周报",
    "招商周报",
]

# Storage and reports
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data"))).expanduser()
DB_PATH = str(Path(os.getenv("AUDIT_DB_PATH", str(DATA_DIR / "audit_records.sqlite3"))).expanduser())
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", str(DATA_DIR / "reports"))).expanduser()
BOARD_CACHE_DIR = Path(os.getenv("BOARD_CACHE_DIR", str(DATA_DIR / "boards"))).expanduser()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://agenthelpjob.com/bydfi-audit-bot").strip().rstrip("/")
MAX_SOURCE_BODY_CHARS = int(os.getenv("MAX_SOURCE_BODY_CHARS", 20000))
STORAGE_PATH = DB_PATH
MAX_AUDIT_INPUT_CHARS = MAX_SOURCE_BODY_CHARS

# Version plan (bitable)
VERSION_PLAN_APP_TOKEN = os.getenv("VERSION_PLAN_APP_TOKEN", "").strip()
VERSION_PLAN_TABLE_ID = os.getenv("VERSION_PLAN_TABLE_ID", "").strip()
VERSION_PLAN_VIEW_ID = os.getenv("VERSION_PLAN_VIEW_ID", "").strip()
VERSION_PLAN_TITLE_FIELD = os.getenv("VERSION_PLAN_TITLE_FIELD", "标题").strip()
VERSION_PLAN_OWNER_FIELD = os.getenv("VERSION_PLAN_OWNER_FIELD", "负责人").strip()
VERSION_PLAN_STATUS_FIELD = os.getenv("VERSION_PLAN_STATUS_FIELD", "状态").strip()
VERSION_PLAN_DUE_FIELD = os.getenv("VERSION_PLAN_DUE_FIELD", "截止时间").strip()

# Scheduled summary
GROUP_SUMMARY_CHAT_ID = os.getenv("GROUP_SUMMARY_CHAT_ID", "").strip()
GROUP_SUMMARY_DEFAULT_HOURS = int(os.getenv("GROUP_SUMMARY_DEFAULT_HOURS", 24))
GROUP_SUMMARY_CRON_HOUR = int(os.getenv("GROUP_SUMMARY_CRON_HOUR", 18))

# Department timeline tracking
DEPT_TRACKING_ENABLED = os.getenv("DEPT_TRACKING_ENABLED", "true").lower() == "true"
DEPT_TRACKING_CHAT_IDS = [
    item.strip()
    for item in os.getenv("DEPT_TRACKING_CHAT_IDS", "").split(",")
    if item.strip()
]
DEPT_TRACKING_MIN_TEXT_LEN = int(os.getenv("DEPT_TRACKING_MIN_TEXT_LEN", 400))

# External Lark group collection
LARK_WEB_BASE_URL = os.getenv("LARK_WEB_BASE_URL", "https://www.larksuite.com").strip().rstrip("/")
PLAYWRIGHT_STORAGE_STATE_PATH = os.getenv(
    "PLAYWRIGHT_STORAGE_STATE_PATH",
    str(DATA_DIR / "playwright" / "lark_storage_state.json"),
).strip()
PLAYWRIGHT_BROWSER_CHANNEL = os.getenv("PLAYWRIGHT_BROWSER_CHANNEL", "msedge").strip().lower()
PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
PLAYWRIGHT_SLOW_MO_MS = int(os.getenv("PLAYWRIGHT_SLOW_MO_MS", 0))
PLAYWRIGHT_NAV_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_NAV_TIMEOUT_MS", 45000))
PLAYWRIGHT_USE_EDGE_PROFILE = os.getenv("PLAYWRIGHT_USE_EDGE_PROFILE", "false").lower() == "true"
PLAYWRIGHT_EDGE_PROFILE_DIR = os.getenv(
    "PLAYWRIGHT_EDGE_PROFILE_DIR",
    str(Path(os.getenv("LOCALAPPDATA", "")) / "Microsoft" / "Edge" / "User Data" / "Default"),
).strip()
EXTERNAL_GROUP_MESSAGE_SCAN_LIMIT = int(os.getenv("EXTERNAL_GROUP_MESSAGE_SCAN_LIMIT", 300))
EXTERNAL_GROUP_SCROLL_ITERATIONS = int(os.getenv("EXTERNAL_GROUP_SCROLL_ITERATIONS", 18))
EXTERNAL_GROUP_REPORT_KEYWORDS = [
    item.strip()
    for item in os.getenv(
        "EXTERNAL_GROUP_REPORT_KEYWORDS",
        "周报,汇报,会议纪要,会议总结,会议记录,复盘,任务,临时任务,需求,问题,weekly report",
    ).split(",")
    if item.strip()
]
CUSTOM_BOT_WEBHOOK_URL = os.getenv("CUSTOM_BOT_WEBHOOK_URL", "").strip()

# Service
PORT = int(os.getenv("PORT", 8080))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
