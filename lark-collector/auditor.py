from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from typing import Any

import anthropic
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_BASE_URL,
    API_TIMEOUT,
    CLAUDE_AUTH_MODE,
    CLAUDE_MODEL,
    GROUP_SUMMARY_REASONING_EFFORT,
    MODEL_NAME,
    MODEL_PROVIDER,
    MODEL_REASONING_EFFORT,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_WIRE_API,
)

logger = logging.getLogger(__name__)

DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com"
PROMPT_VERSION = "judge_v3"

_session = requests.Session()
_session.trust_env = False
_retry = Retry(
    total=2,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"],
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

SINGLE_REPORT_PROMPT = """
你是执行审计官。你必须输出“对账结论”，不是摘要。

绝对规则：
1. 禁止空词：信息孤岛、机制补全、规范缺失、效能低下、持续优化、建议加强协同。
2. 只要缺失 owner 或 ETA 或验收结果，不能判定为有效推进。
3. 只有“跟进中/处理中/已同步/在排期”一律判定为状态冲突-假性推进。
4. 没有研发侧证据时，只能写“研发侧证据不足（待核对）”，不得脑补。

输出要求：
- 只写中文。
- 不要使用 Markdown 语法符号。
- 严格按以下字段逐行输出：
BYDFi 单条跨源对账单
事件：
业务侧诉求源：
研发/计划核对源：
AI核对结论：
归因：
追责动作：
"""

GROUP_SUMMARY_PROMPT = """
你是执行审计官，任务是出具“BYDFi 产研跨源对账单 (Cross-Check Report)”。

绝对规则：
1. 禁止空词：信息孤岛、机制补全、规范缺失、效能低下、持续优化、建议加强协同。
2. 每条事件必须同时给出：业务侧诉求源、研发/计划核对源、AI核对结论、归因、追责建议。
3. 判定标准：
 - 缺 owner 或缺 ETA 或缺验收结果 => 断链/空转
 - 仅有“跟进中/处理中/已同步/在排期” => 状态冲突-假性推进
 - 同一问题跨周期复现且无交付证据 => 高危断链
4. 若未检索到研发侧证据，必须写“研发侧证据不足（待核对）”，不能下绝对重判。
5. 输出必须短句、可执行，适合管理层手机阅读。

输出格式（严格遵守，不要 Markdown）：
BYDFi 产研跨源对账单 (Cross-Check Report)
一、高危断链项（AI双向核查确权）
事件1：
业务侧诉求源：
研发侧核对源：
AI核对结论：
归因：
追责建议：

二、待核对项（证据不足，不下重判）
事件1：
缺失证据：
需要谁补：

三、本周必须落地动作（最多3条）
1.
2.
3.
"""


def _raise_for_provider_error(resp: requests.Response, provider_name: str) -> None:
    if resp.status_code < 400:
        return
    body = resp.text[:500]
    logger.error("%s request failed status=%s body=%s", provider_name, resp.status_code, body)
    if resp.status_code == 403 and "没有可用的账号" in body:
        raise RuntimeError(f"{provider_name} 代理当前没有可用账号，请更换可用 token 或切换提供商。")
    resp.raise_for_status()


def _extract_openai_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str) and data["output_text"].strip():
        return data["output_text"].strip()

    texts: list[str] = []
    output = data.get("output", [])
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content", [])
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    texts.append(text.strip())
    return "\n".join(texts).strip()


def _extract_anthropic_sse_text(body: str) -> str:
    texts: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if not payload or payload == "[DONE]":
            continue
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            continue
        delta = event.get("delta") or {}
        text_delta = delta.get("text")
        if isinstance(text_delta, str) and text_delta:
            texts.append(text_delta)
            continue
        content = event.get("content_block") or {}
        text_value = content.get("text")
        if isinstance(text_value, str) and text_value:
            texts.append(text_value)
            continue
        message = event.get("message") or {}
        for item in message.get("content", []) or []:
            text = item.get("text")
            if isinstance(text, str) and text:
                texts.append(text)
    return "".join(texts).strip()


def _run_with_openai(
    system_prompt: str,
    user_message: str,
    *,
    reasoning_effort: str | None = None,
    model_name: str | None = None,
) -> str:
    base_url = (OPENAI_BASE_URL or DEFAULT_OPENAI_BASE_URL).rstrip("/")
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY 为空，无法调用 OpenAI 兼容服务。")

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    if OPENAI_WIRE_API == "responses":
        payload = {
            "model": model_name or MODEL_NAME,
            "reasoning": {"effort": reasoning_effort or MODEL_REASONING_EFFORT},
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
                {"role": "user", "content": [{"type": "input_text", "text": user_message}]},
            ],
        }
        url = f"{base_url}/v1/responses"
    else:
        payload = {
            "model": model_name or MODEL_NAME,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        }
        url = f"{base_url}/v1/chat/completions"

    resp = _session.post(url, headers=headers, json=payload, timeout=API_TIMEOUT)
    _raise_for_provider_error(resp, "OpenAI")
    data = resp.json()

    if OPENAI_WIRE_API == "responses":
        return _extract_openai_text(data) or "模型返回为空，请重试。"

    choices = data.get("choices", [])
    if choices:
        message = choices[0].get("message", {})
        content = message.get("content", "")
        if isinstance(content, str) and content.strip():
            return content.strip()
    return "模型返回为空，请重试。"


def _run_with_anthropic(
    system_prompt: str,
    user_message: str,
    *,
    model_name: str | None = None,
) -> str:
    base_url = (ANTHROPIC_BASE_URL or DEFAULT_ANTHROPIC_BASE_URL).rstrip("/")
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY 为空，无法调用 Anthropic / Claude。")

    headers = {
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    auth_mode = CLAUDE_AUTH_MODE
    if auth_mode == "auto":
        auth_mode = "x-api-key" if ANTHROPIC_API_KEY.startswith("sk-ant-") or "anthropic.com" in base_url else "bearer"

    if auth_mode == "x-api-key":
        headers["x-api-key"] = ANTHROPIC_API_KEY
    else:
        headers["Authorization"] = f"Bearer {ANTHROPIC_API_KEY}"

    payload = {
        "model": model_name or CLAUDE_MODEL,
        "max_tokens": 4096,
        "stream": False,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}],
    }
    url = f"{base_url}/v1/messages"
    resp = _session.post(url, headers=headers, json=payload, timeout=API_TIMEOUT)
    _raise_for_provider_error(resp, "Claude")
    content_type = (resp.headers.get("Content-Type") or "").lower()
    body = resp.content.decode("utf-8", errors="replace")
    if "text/event-stream" in content_type or body.lstrip().startswith("event:"):
        text = _extract_anthropic_sse_text(body)
        return text or "模型返回为空，请重试。"
    data = resp.json()

    texts: list[str] = []
    for item in data.get("content", []):
        if isinstance(item, dict) and item.get("type") == "text":
            text = item.get("text", "")
            if isinstance(text, str) and text.strip():
                texts.append(text.strip())
    return "\n".join(texts).strip() or "模型返回为空，请重试。"


def build_async_anthropic_client(*, timeout_s: float | None = None) -> anthropic.AsyncAnthropic:
    base_url = (ANTHROPIC_BASE_URL or DEFAULT_ANTHROPIC_BASE_URL).rstrip("/")
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY 为空，无法调用 Anthropic / Claude。")

    auth_mode = CLAUDE_AUTH_MODE
    if auth_mode == "auto":
        auth_mode = "x-api-key" if ANTHROPIC_API_KEY.startswith("sk-ant-") or "anthropic.com" in base_url else "bearer"

    client_kwargs: dict[str, Any] = {
        "base_url": base_url,
        "timeout": timeout_s or API_TIMEOUT,
        "max_retries": 0,
    }
    if auth_mode == "x-api-key":
        client_kwargs["api_key"] = ANTHROPIC_API_KEY
    else:
        client_kwargs["auth_token"] = ANTHROPIC_API_KEY
    return anthropic.AsyncAnthropic(**client_kwargs)


def _is_retryable_provider_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and status_code in RETRYABLE_STATUS_CODES:
        return True
    body = str(exc).lower()
    return any(code in body for code in ("429", "500", "502", "503", "504", "rate limit", "timeout"))


async def _run_with_anthropic_async(
    system_prompt: str,
    user_message: str,
    *,
    model_name: str | None = None,
    max_attempts: int = 3,
    client: anthropic.AsyncAnthropic | None = None,
) -> str:
    return await asyncio.to_thread(
        _run_with_anthropic,
        system_prompt,
        user_message,
        model_name=model_name,
    )


def _run_model(
    system_prompt: str,
    user_message: str,
    *,
    purpose: str,
    provider_override: str | None = None,
    reasoning_effort_override: str | None = None,
) -> str:
    provider = (provider_override or MODEL_PROVIDER).strip().lower()
    logger.info("calling model purpose=%s provider=%s model=%s text_len=%s", purpose, provider, MODEL_NAME, len(user_message))
    if provider == "openai":
        reasoning_effort = reasoning_effort_override or (
            GROUP_SUMMARY_REASONING_EFFORT if purpose == "group_summary" else MODEL_REASONING_EFFORT
        )
        return _run_with_openai(system_prompt, user_message, reasoning_effort=reasoning_effort)
    return _run_with_anthropic(system_prompt, user_message)


async def _run_model_async(
    system_prompt: str,
    user_message: str,
    *,
    purpose: str,
    provider_override: str | None = None,
    reasoning_effort_override: str | None = None,
    anthropic_client: anthropic.AsyncAnthropic | None = None,
) -> str:
    provider = (provider_override or MODEL_PROVIDER).strip().lower()
    logger.info("calling async model purpose=%s provider=%s model=%s text_len=%s", purpose, provider, MODEL_NAME, len(user_message))
    if provider == "openai":
        reasoning_effort = reasoning_effort_override or (
            GROUP_SUMMARY_REASONING_EFFORT if purpose == "group_summary" else MODEL_REASONING_EFFORT
        )
        return await asyncio.to_thread(
            _run_with_openai,
            system_prompt,
            user_message,
            reasoning_effort=reasoning_effort,
        )
    return await _run_with_anthropic_async(system_prompt, user_message, client=anthropic_client)


def _build_single_user_message(
    report_text: str,
    report_type: str,
    *,
    source_type: str = "",
    sender_name: str = "",
    title: str = "",
    plan_context: str = "",
    focus: str = "",
) -> str:
    lines = [f"请审计以下{report_type}。"]
    if source_type:
        lines.append(f"来源类型：{source_type}")
    if title:
        lines.append(f"标题：{title}")
    if sender_name:
        lines.append(f"发送者：{sender_name}")
    if focus:
        lines.append(f"重点关注：{focus}")
    if plan_context:
        lines.append("版本计划参考：")
        lines.append(plan_context)
    lines.append("")
    lines.append("原始内容：")
    lines.append(report_text)
    return "\n".join(lines)


def run_audit(
    report_text: str,
    report_type: str = "周报",
    *,
    source_type: str = "",
    sender_name: str = "",
    title: str = "",
    plan_context: str = "",
    focus: str = "",
) -> str:
    user_message = _build_single_user_message(
        report_text,
        report_type,
        source_type=source_type,
        sender_name=sender_name,
        title=title,
        plan_context=plan_context,
        focus=focus,
    )
    return _run_model(SINGLE_REPORT_PROMPT, user_message, purpose="single_report")


def _build_group_summary_user_message(
    grouped_report_text: str,
    *,
    window_label: str,
    plan_context: str = "",
) -> str:
    lines = [f"请基于以下执行事实，输出 {window_label} 的群执行审计。"]
    if plan_context:
        lines.append("版本计划参考：")
        lines.append(plan_context)
    lines.append("")
    lines.append("执行事实材料：")
    lines.append(grouped_report_text)
    return "\n".join(lines)


def run_group_summary(
    grouped_report_text: str,
    *,
    window_label: str,
    plan_context: str = "",
) -> str:
    user_message = _build_group_summary_user_message(
        grouped_report_text,
        window_label=window_label,
        plan_context=plan_context,
    )
    provider_override = "anthropic" if ANTHROPIC_API_KEY else None
    return _run_model(
        GROUP_SUMMARY_PROMPT,
        user_message,
        purpose="group_summary",
        provider_override=provider_override,
    )


def run_group_summary_ab(
    grouped_report_text: str,
    *,
    window_label: str,
    plan_context: str = "",
) -> dict[str, Any]:
    user_message = _build_group_summary_user_message(
        grouped_report_text,
        window_label=window_label,
        plan_context=plan_context,
    )
    input_hash = hashlib.sha256(user_message.encode("utf-8")).hexdigest()
    runs: list[dict[str, Any]] = []

    candidates = [
        ("anthropic", CLAUDE_MODEL, None),
        ("openai", MODEL_NAME, GROUP_SUMMARY_REASONING_EFFORT),
    ]

    for provider, model_name, reasoning_effort in candidates:
        started = time.perf_counter()
        try:
            output = _run_model(
                GROUP_SUMMARY_PROMPT,
                user_message,
                purpose="group_summary",
                provider_override=provider,
                reasoning_effort_override=reasoning_effort,
            )
            error = ""
        except Exception as exc:
            output = ""
            error = str(exc)
        latency_ms = int((time.perf_counter() - started) * 1000)
        runs.append(
            {
                "provider": provider,
                "model": model_name,
                "reasoning_effort": reasoning_effort or "",
                "latency_ms": latency_ms,
                "output_text": output,
                "error": error,
            }
        )

    return {
        "task_type": "group_summary",
        "prompt_version": PROMPT_VERSION,
        "input_hash": input_hash,
        "window_label": window_label,
        "runs": runs,
    }
