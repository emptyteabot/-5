import json
import logging
import time
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import BOT_DISPLAY_NAME, FEISHU_APP_ID, FEISHU_APP_SECRET

logger = logging.getLogger(__name__)

_token_cache = {"token": "", "expires_at": 0}

_session = requests.Session()
_retry = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST", "GET"],
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))


def get_tenant_access_token() -> str:
    """Get and locally cache tenant_access_token."""
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = _session.post(
        url,
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 token 失败: {data}")
    _token_cache["token"] = data["tenant_access_token"]
    _token_cache["expires_at"] = now + data.get("expire", 7200) - 60
    logger.info("tenant_access_token 已刷新")
    return _token_cache["token"]


def get_message(message_id: str) -> dict[str, Any]:
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}"
    resp = _session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取消息失败: {data}")
    return data


def list_chat_messages(
    chat_id: str,
    *,
    start_at: str | None = None,
    end_at: str | None = None,
    page_size: int = 50,
) -> list[dict[str, Any]]:
    token = get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}"}
    base_params: dict[str, Any] = {
        "container_id_type": "chat",
        "container_id": chat_id,
        "sort_type": "ByCreateTimeAsc",
        "page_size": min(max(1, page_size), 50),
    }
    if start_at:
        base_params["start_time"] = _to_unix_seconds(start_at)
    if end_at:
        base_params["end_time"] = _to_unix_seconds(end_at)

    items: list[dict[str, Any]] = []
    page_token = ""
    while True:
        params = dict(base_params)
        if page_token:
            params["page_token"] = page_token
        resp = _session.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"获取会话历史消息失败: {data}")
        payload = data.get("data", {})
        batch = payload.get("items", [])
        if isinstance(batch, list):
            items.extend(item for item in batch if isinstance(item, dict))
        if not payload.get("has_more"):
            break
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            break
    return items


def get_message_resource(message_id: str, file_key: str, resource_type: str = "file") -> bytes:
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/resources/{file_key}"
    resp = _session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={"type": resource_type},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.content


def get_docx_raw_content(document_id: str, lang: int = 0) -> str:
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}/raw_content"
    resp = _session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={"lang": lang},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取文档纯文本失败: {data}")
    return data.get("data", {}).get("content", "")


def get_docx_document_meta(document_id: str) -> dict[str, Any]:
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}"
    resp = _session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取文档信息失败: {data}")
    return data


def get_wiki_node_info(token_or_id: str) -> dict[str, Any]:
    token = get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node"
    resp = _session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={"token": token_or_id},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 Wiki 节点失败: {data}")
    return data


def get_document_raw_content(kind: str, token_or_id: str) -> str:
    kind = (kind or "").strip().lower()
    if kind not in {"docx", "document", "wiki"}:
        raise ValueError(f"unsupported document kind: {kind}")
    if kind == "wiki":
        data = get_wiki_node_info(token_or_id)
        node = data.get("data", {}).get("node", {})
        obj_type = (node.get("obj_type") or "").strip().lower()
        obj_token = (node.get("obj_token") or "").strip()
        if obj_type == "docx" and obj_token:
            return get_docx_raw_content(obj_token)
        raise RuntimeError(f"Wiki 节点暂不支持直接读取正文: obj_type={obj_type or 'unknown'}")
    if kind == "document":
        logger.warning("检测到 legacy document token，尝试按 docx raw_content 读取 token=%s", token_or_id)
    return get_docx_raw_content(token_or_id)


def download_message_resource(
    message_id: str,
    file_key: str,
    resource_type: str = "file",
) -> tuple[bytes, str]:
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/resources/{file_key}"
    resp = _session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={"type": resource_type},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.content, resp.headers.get("Content-Type", "")


def _build_post_content(text: str) -> dict[str, Any]:
    paragraphs = []
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped:
            paragraphs.append([{"tag": "text", "text": stripped}])
        else:
            paragraphs.append([{"tag": "text", "text": " "}])
    return {
        "zh_cn": {
            "title": BOT_DISPLAY_NAME,
            "content": paragraphs,
        }
    }


def reply_message(message_id: str, text: str) -> dict[str, Any]:
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
    payload = {
        "msg_type": "post",
        "content": json.dumps(_build_post_content(text), ensure_ascii=False),
    }
    resp = _session.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    if result.get("code") != 0:
        logger.error("飞书回复失败: %s", result)
    else:
        logger.info("飞书回复成功 message_id=%s", message_id)
    return result


def send_post_to_chat(chat_id: str, title: str, text: str) -> dict[str, Any]:
    token = get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    post_content = _build_post_content(text)
    post_content["zh_cn"]["title"] = title
    payload = {
        "receive_id": chat_id,
        "msg_type": "post",
        "content": json.dumps(post_content, ensure_ascii=False),
    }
    resp = _session.post(
        url,
        params={"receive_id_type": "chat_id"},
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    if result.get("code") != 0:
        logger.error("飞书发送富文本消息失败: %s", result)
    return result


def send_text_to_chat(chat_id: str, text: str) -> dict[str, Any]:
    return send_text_message(receive_id=chat_id, text=text, receive_id_type="chat_id")


def send_text_message(receive_id: str, text: str, receive_id_type: str = "chat_id") -> dict[str, Any]:
    token = get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    payload = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    resp = _session.post(
        url,
        params={"receive_id_type": receive_id_type},
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    if result.get("code") != 0:
        logger.error("飞书发送消息失败: %s", result)
    return result


def _to_unix_seconds(value: str) -> str:
    return str(int(datetime.fromisoformat(value).timestamp()))
