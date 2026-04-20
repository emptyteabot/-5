import json
import re
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple
from urllib.parse import unquote, urlparse


DocumentFetcher = Callable[[str, str], Optional[str]]

_URL_PATTERN = re.compile(r"https?://[^\s<>\]\)\"']+")
_MD_LINK_PATTERN = re.compile(r"\[([^\]]+)\]\((https?://[^\s\)]+)\)")
_DOC_TOKEN_PATTERN = re.compile(r"/(docx|document|wiki)/([A-Za-z0-9]+)")


def parse_message_content(message_type: str, content: Any) -> Dict[str, Any]:
    """Parse Feishu message content into a dictionary."""
    if isinstance(content, dict):
        return content

    if isinstance(content, bytes):
        try:
            content = content.decode("utf-8", errors="replace")
        except Exception:
            return {}

    if isinstance(content, str):
        text = content.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            # Fallback: text messages might be raw text in abnormal pipelines.
            if message_type == "text":
                return {"text": text}
            return {}

    return {}


def extract_text_and_links(message_type: str, parsed_content: Mapping[str, Any]) -> Tuple[str, List[str]]:
    """Extract normalized text body and links from supported message types."""
    if message_type == "text":
        text = _normalize_whitespace(_extract_text_message(parsed_content))
        return text, _dedupe_keep_order(_extract_links_from_text(text))

    if message_type == "post":
        title = str(parsed_content.get("title") or "").strip()
        lines, links = _extract_post_lines_and_links(parsed_content.get("content"))
        combined = "\n".join(_drop_empty([title] + lines))
        return _normalize_whitespace(combined), _dedupe_keep_order(links)

    if message_type == "interactive":
        title = str(parsed_content.get("title") or "").strip()
        elements = parsed_content.get("elements")
        lines, links = _extract_interactive_lines_and_links(elements)
        combined = "\n".join(_drop_empty([title] + lines))
        return _normalize_whitespace(combined), _dedupe_keep_order(links)

    if message_type == "file":
        file_name = str(parsed_content.get("file_name") or "").strip()
        return file_name, []

    return "", []


def extract_feishu_document_tokens(links: Iterable[str]) -> List[Dict[str, str]]:
    """Detect Feishu docx/document tokens from URLs."""
    out: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    for link in links:
        for kind, token in _extract_doc_tokens_from_url(link):
            key = (kind, token)
            if key in seen:
                continue
            seen.add(key)
            out.append({"kind": kind, "token": token, "url": link})
    return out


def build_source_payload(
    message_meta: Mapping[str, Any],
    fetch_document_text: Optional[DocumentFetcher] = None,
) -> Dict[str, Any]:
    """
    Build a unified source payload:
    {
      "source_type": ...,
      "title": ...,
      "body": ...,
      "metadata": {...}
    }
    """
    message_type = str(message_meta.get("message_type") or message_meta.get("msg_type") or "").strip().lower()
    message_id = str(message_meta.get("message_id") or "")
    chat_id = str(message_meta.get("chat_id") or "")

    parsed_content = parse_message_content(message_type, message_meta.get("content"))
    body_text, links = extract_text_and_links(message_type, parsed_content)
    doc_tokens = extract_feishu_document_tokens(links)

    title = _derive_title(message_type, parsed_content, body_text)
    resolved_body = body_text
    fetch_logs: List[Dict[str, Any]] = []

    if fetch_document_text and doc_tokens:
        fetched_texts: List[str] = []
        for item in doc_tokens:
            kind = item["kind"]
            token = item["token"]
            try:
                content = fetch_document_text(kind, token)
                if content:
                    fetched_texts.append(content.strip())
                    fetch_logs.append(
                        {
                            "kind": kind,
                            "token": token,
                            "status": "ok",
                            "length": len(content),
                        }
                    )
                else:
                    fetch_logs.append({"kind": kind, "token": token, "status": "empty"})
            except Exception as exc:
                fetch_logs.append(
                    {
                        "kind": kind,
                        "token": token,
                        "status": "error",
                        "error": str(exc),
                    }
                )

        if fetched_texts:
            resolved_body = "\n\n".join(fetched_texts)

    source_type = _derive_source_type(message_type, doc_tokens, fetch_logs)

    return {
        "source_type": source_type,
        "title": title,
        "body": resolved_body,
        "metadata": {
            "message_id": message_id,
            "chat_id": chat_id,
            "message_type": message_type,
            "links": links,
            "doc_tokens": doc_tokens,
            "document_fetch": fetch_logs,
            "raw_content": parsed_content,
        },
    }


def _extract_text_message(content: Mapping[str, Any]) -> str:
    value = content.get("text")
    return str(value) if value is not None else ""


def _extract_post_lines_and_links(content: Any) -> Tuple[List[str], List[str]]:
    lines: List[str] = []
    links: List[str] = []
    if not isinstance(content, list):
        return lines, links

    for row in content:
        row_text_parts: List[str] = []
        if not isinstance(row, list):
            continue
        for cell in row:
            if not isinstance(cell, dict):
                continue
            tag = str(cell.get("tag") or "").strip().lower()
            if tag == "text":
                text = str(cell.get("text") or "")
                row_text_parts.append(text)
                links.extend(_extract_links_from_text(text))
            elif tag == "a":
                text = str(cell.get("text") or "")
                href = str(cell.get("href") or "")
                if text:
                    row_text_parts.append(text)
                if href:
                    links.append(href)
            elif tag == "at":
                name = str(cell.get("user_name") or "").strip()
                row_text_parts.append(f"@{name}" if name else "@user")
            elif tag == "code_block":
                code = str(cell.get("text") or "")
                if code:
                    row_text_parts.append(code)
            elif tag == "note":
                elements = cell.get("elements")
                note_text, note_links = _extract_interactive_lines_and_links(elements)
                if note_text:
                    row_text_parts.extend(note_text)
                if note_links:
                    links.extend(note_links)
        if row_text_parts:
            lines.append(" ".join(_drop_empty(row_text_parts)))

    return lines, links


def _extract_interactive_lines_and_links(elements: Any) -> Tuple[List[str], List[str]]:
    lines: List[str] = []
    links: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return

        for key in ("url", "href", "default_url"):
            value = node.get(key)
            if isinstance(value, str) and value.startswith("http"):
                links.append(value)

        tag = str(node.get("tag") or "").strip().lower()
        if tag == "text":
            text_val = node.get("text")
            if isinstance(text_val, dict):
                value = str(text_val.get("content") or text_val.get("text") or "")
            else:
                value = str(text_val or "")
            if value:
                lines.append(value)
                links.extend(_extract_links_from_text(value))
        elif tag == "markdown":
            value = str(node.get("content") or "")
            if value:
                lines.append(value)
                links.extend(_extract_links_from_text(value))
        elif tag in {"button", "plain_text"}:
            text_val = node.get("text")
            if isinstance(text_val, dict):
                value = str(text_val.get("content") or text_val.get("text") or "")
            else:
                value = str(text_val or "")
            if value:
                lines.append(value)
                links.extend(_extract_links_from_text(value))
        elif tag in {"lark_md", "note"}:
            value = str(node.get("content") or "")
            if value:
                lines.append(value)
                links.extend(_extract_links_from_text(value))

        for value in node.values():
            if isinstance(value, (list, dict)):
                walk(value)

    walk(elements)
    return lines, links


def _extract_links_from_text(text: str) -> List[str]:
    links: List[str] = []
    for match in _MD_LINK_PATTERN.findall(text):
        if len(match) >= 2 and match[1]:
            links.append(match[1])
    links.extend(_URL_PATTERN.findall(text))
    return [link.strip() for link in links if link.strip()]


def _extract_doc_tokens_from_url(url: str) -> List[Tuple[str, str]]:
    result: List[Tuple[str, str]] = []
    try:
        parsed = urlparse(url)
    except Exception:
        return result

    if not parsed.path:
        return result

    decoded_path = unquote(parsed.path)
    for match in _DOC_TOKEN_PATTERN.finditer(decoded_path):
        kind = match.group(1).strip().lower()
        token = match.group(2).strip()
        if kind and token:
            result.append((kind, token))
    return result


def _derive_source_type(
    message_type: str,
    doc_tokens: List[Dict[str, str]],
    fetch_logs: List[Dict[str, Any]],
) -> str:
    if message_type == "file":
        return "file_message"
    if doc_tokens:
        if any(item.get("status") == "ok" for item in fetch_logs):
            return "document_message"
        return "document_link_message"
    if message_type in {"text", "post", "interactive"}:
        return f"{message_type}_message"
    return "unsupported_message"


def _derive_title(message_type: str, parsed_content: Mapping[str, Any], body_text: str) -> str:
    if message_type == "file":
        return str(parsed_content.get("file_name") or "File")

    for key in ("title", "header", "name"):
        value = parsed_content.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    fallback = _first_non_empty_line(body_text)
    if fallback:
        return fallback[:120]
    return f"{message_type or 'unknown'} message"


def _first_non_empty_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def _drop_empty(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    for item in items:
        stripped = item.strip()
        if stripped:
            out.append(stripped)
    return out


def _normalize_whitespace(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(_trim_empty_edges(lines)).strip()


def _trim_empty_edges(lines: List[str]) -> List[str]:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


__all__ = [
    "DocumentFetcher",
    "parse_message_content",
    "extract_text_and_links",
    "extract_feishu_document_tokens",
    "build_source_payload",
]
