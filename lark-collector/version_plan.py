import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

import requests

DEFAULT_API_BASE = "https://open.feishu.cn/open-apis"
DEFAULT_TIMEOUT = 15
DEFAULT_PAGE_SIZE = 100

TOKEN_PROVIDER = Callable[[], str]

logger = logging.getLogger(__name__)


def load_version_plan_config(env: Optional[Mapping[str, str]] = None) -> dict[str, Any]:
    source = env or os.environ
    field_map = _load_field_map(source)
    return {
        "app_token": _getenv(source, "VERSION_PLAN_APP_TOKEN"),
        "table_id": _getenv(source, "VERSION_PLAN_TABLE_ID"),
        "view_id": _getenv(source, "VERSION_PLAN_VIEW_ID"),
        "api_base": _getenv(source, "VERSION_PLAN_API_BASE") or DEFAULT_API_BASE,
        "timeout": _to_int(_getenv(source, "VERSION_PLAN_TIMEOUT"), DEFAULT_TIMEOUT),
        "page_size": _to_int(_getenv(source, "VERSION_PLAN_PAGE_SIZE"), DEFAULT_PAGE_SIZE),
        "field_map": field_map,
    }


def is_version_plan_configured(config: Optional[Mapping[str, Any]] = None) -> bool:
    config = config or load_version_plan_config()
    return bool(config.get("app_token") and config.get("table_id"))


class LarkBitableClient:
    def __init__(
        self,
        *,
        app_token: str,
        table_id: str,
        view_id: str = "",
        token_provider: Optional[TOKEN_PROVIDER] = None,
        api_base: str = DEFAULT_API_BASE,
        timeout: int = DEFAULT_TIMEOUT,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> None:
        self.app_token = app_token
        self.table_id = table_id
        self.view_id = view_id
        self.api_base = api_base.rstrip("/")
        self.timeout = max(1, timeout)
        self.page_size = min(max(1, page_size), 500)
        self.token_provider = token_provider or _default_token_provider
        self.session = requests.Session()

    def list_records(self) -> list[dict[str, Any]]:
        page_token = ""
        records: list[dict[str, Any]] = []

        while True:
            data = self._list_records_page(page_token=page_token)
            items = data.get("items", [])
            if isinstance(items, list):
                records.extend(item for item in items if isinstance(item, dict))

            has_more = bool(data.get("has_more"))
            page_token = str(data.get("page_token") or "")
            if not has_more or not page_token:
                break

        return records

    def _list_records_page(self, *, page_token: str = "") -> dict[str, Any]:
        token = self.token_provider()
        if not token:
            raise RuntimeError("empty tenant access token")

        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        params: dict[str, Any] = {"page_size": self.page_size}
        if self.view_id:
            params["view_id"] = self.view_id
        if page_token:
            params["page_token"] = page_token

        resp = self.session.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != 0:
            raise RuntimeError(
                f"bitable records api failed code={payload.get('code')} msg={payload.get('msg')}"
            )
        data = payload.get("data", {})
        return data if isinstance(data, dict) else {}


def normalize_version_plan_records(
    records: list[dict[str, Any]],
    field_map: Mapping[str, str],
) -> list[dict[str, Any]]:
    title_field = field_map.get("title", "title")
    owner_field = field_map.get("owner", "owner")
    status_field = field_map.get("status", "status")
    due_field = field_map.get("due", "due_date")

    items: list[dict[str, Any]] = []
    for record in records:
        fields = record.get("fields", {})
        if not isinstance(fields, dict):
            fields = {}

        item = {
            "title": _to_text(fields.get(title_field)),
            "owner": _to_owner(fields.get(owner_field)),
            "status": _to_text(fields.get(status_field)),
            "due_date": _to_due_date(fields.get(due_field)),
            "record_id": _to_text(record.get("record_id")),
            "raw_record": record,
        }
        if item["title"]:
            items.append(item)
    return items


def fetch_version_plan_items(
    *,
    env: Optional[Mapping[str, str]] = None,
    token_provider: Optional[TOKEN_PROVIDER] = None,
) -> list[dict[str, Any]]:
    config = load_version_plan_config(env)
    if not is_version_plan_configured(config):
        return []

    try:
        client = LarkBitableClient(
            app_token=str(config["app_token"]),
            table_id=str(config["table_id"]),
            view_id=str(config.get("view_id") or ""),
            token_provider=token_provider,
            api_base=str(config.get("api_base") or DEFAULT_API_BASE),
            timeout=int(config.get("timeout") or DEFAULT_TIMEOUT),
            page_size=int(config.get("page_size") or DEFAULT_PAGE_SIZE),
        )
        records = client.list_records()
        field_map = config.get("field_map", {})
        if not isinstance(field_map, dict):
            field_map = {}
        return normalize_version_plan_records(records, field_map)
    except Exception as exc:
        logger.warning("failed to fetch version plan items: %s", exc)
        return []


def build_plan_context(limit: int = 20) -> str:
    items = fetch_version_plan_items()
    if not items:
        return ""

    lines = ["当前版本计划参考："]
    for item in items[:limit]:
        lines.append(
            "- {title} | 负责人: {owner} | 状态: {status} | 截止: {due}".format(
                title=item.get("title") or "未命名",
                owner=item.get("owner") or "未提供",
                status=item.get("status") or "未提供",
                due=item.get("due_date") or "未提供",
            )
        )
    return "\n".join(lines)


def _default_token_provider() -> str:
    from feishu import get_tenant_access_token

    return get_tenant_access_token()


def _load_field_map(source: Mapping[str, str]) -> dict[str, str]:
    defaults = {
        "title": _getenv(source, "VERSION_PLAN_TITLE_FIELD") or "标题",
        "owner": _getenv(source, "VERSION_PLAN_OWNER_FIELD") or "负责人",
        "status": _getenv(source, "VERSION_PLAN_STATUS_FIELD") or "状态",
        "due": _getenv(source, "VERSION_PLAN_DUE_FIELD") or "截止时间",
    }
    raw = _getenv(source, "VERSION_PLAN_FIELD_MAP")
    if not raw:
        return defaults

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return defaults

    if not isinstance(payload, dict):
        return defaults

    merged = defaults.copy()
    for key in ("title", "owner", "status", "due"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            merged[key] = value.strip()
    return merged


def _getenv(source: Mapping[str, str], name: str) -> str:
    value = source.get(name, "")
    return value.strip() if isinstance(value, str) else ""


def _to_int(value: str, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        parts = [_to_text(item) for item in value]
        return ", ".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("name", "text", "title", "value"):
            if key in value:
                text = _to_text(value.get(key))
                if text:
                    return text
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _to_owner(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        names: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = _to_text(item.get("name") or item.get("en_name") or item.get("id"))
            else:
                name = _to_text(item)
            if name:
                names.append(name)
        return ", ".join(names)
    if isinstance(value, dict):
        return _to_text(value.get("name") or value.get("en_name") or value.get("id"))
    return _to_text(value)


def _to_due_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return str(value)
    if isinstance(value, dict):
        for key in ("timestamp", "value", "start"):
            if key in value:
                text = _to_due_date(value.get(key))
                if text:
                    return text
        return _to_text(value)
    if isinstance(value, list):
        parts = [_to_due_date(item) for item in value]
        return ", ".join(part for part in parts if part)
    return _to_text(value)


__all__ = [
    "LarkBitableClient",
    "build_plan_context",
    "fetch_version_plan_items",
    "is_version_plan_configured",
    "load_version_plan_config",
    "normalize_version_plan_records",
]
