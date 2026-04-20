import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import zipfile

from config import MAX_AUDIT_INPUT_CHARS, TRIGGER_KEYWORDS
from feishu import download_message_resource, get_docx_document_meta, get_docx_raw_content
from ocr_helper import ocr_image_text, ocr_pdf_text
from storage import upsert_document

logger = logging.getLogger(__name__)

MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\((https?://[^)]+)\)")
URL_RE = re.compile(r"https?://[^\s)>\"]+")
DOCX_TOKEN_RE = re.compile(r"\b(dox[a-zA-Z0-9]{24})\b")
TEXT_FILE_SUFFIXES = {".txt", ".md", ".csv", ".json", ".log", ".yaml", ".yml"}
IMAGE_FILE_SUFFIXES = {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff"}


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _attachment_document_key(message_id: str, file_key: str, file_name: str) -> str:
    base = f"attachment_{message_id}_{file_key or file_name}"
    return re.sub(r"[^\w.-]", "_", base)
WORD_NAMESPACE = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


@dataclass
class NormalizedMessage:
    message_id: str
    chat_id: str
    thread_id: str
    root_id: str
    parent_id: str
    chat_type: str
    message_type: str
    sender_open_id: str
    sender_user_id: str
    sender_union_id: str
    source_type: str
    report_type: str
    should_audit: bool
    extracted_text: str
    audit_input: str
    extracted_links: list[str] = field(default_factory=list)
    document_id: str = ""
    document_title: str = ""
    file_key: str = ""
    file_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def normalize_message_event(event: dict[str, Any]) -> NormalizedMessage:
    sender = event.get("sender", {})
    sender_id = sender.get("sender_id", {})
    message = event.get("message", {})
    message_id = message.get("message_id", "")
    message_type = message.get("message_type", "")

    try:
        content = json.loads(message.get("content", "{}"))
    except (TypeError, json.JSONDecodeError):
        content = {}

    text = ""
    links: list[str] = []
    metadata: dict[str, Any] = {"raw_message_type": message_type}
    file_key = ""
    file_name = ""

    if message_type == "text":
        text = _normalize_whitespace(content.get("text", ""))
        links = _extract_links_from_text(text)
    elif message_type in {"post", "interactive"}:
        text, links = _flatten_structured_content(content)
    elif message_type == "file":
        file_key = content.get("file_key", "")
        file_name = content.get("file_name", "")
        text, file_meta = _extract_file_text(
            message_id=message_id,
            file_key=file_key,
            file_name=file_name,
        )
        metadata.update(file_meta)
    elif message_type == "image":
        file_key = content.get("image_key", "")
        file_name = content.get("file_name", "") or content.get("image_name", "") or (f"{file_key}.png" if file_key else "")
        text, file_meta = _extract_image_text(
            message_id=message_id,
            image_key=file_key,
            image_name=file_name,
        )
        metadata.update(file_meta)
    else:
        metadata["unsupported_message_type"] = True

    document_id = _extract_document_id(links)
    document_title = ""
    if document_id:
        metadata["document_id"] = document_id
        document_title = _fetch_docx_title(document_id, metadata)
        doc_text = _fetch_docx_text(document_id, metadata)
        if doc_text:
            text = _join_parts([document_title, doc_text])

    text = _normalize_whitespace(text)
    report_type = _detect_report_type(text)
    source_type = _detect_source_type(
        message_type=message_type,
        report_type=report_type,
        document_id=document_id,
        file_name=file_name,
    )
    should_audit = _should_audit(
        text=text,
        message_type=message_type,
        document_id=document_id,
        file_name=file_name,
    )
    audit_input = _truncate_for_audit(
        _build_audit_input(
            source_type=source_type,
            report_type=report_type,
            message_type=message_type,
            text=text,
            document_title=document_title,
            file_name=file_name,
        )
    )

    return NormalizedMessage(
        message_id=message_id,
        chat_id=message.get("chat_id", ""),
        thread_id=message.get("thread_id", ""),
        root_id=message.get("root_id", ""),
        parent_id=message.get("parent_id", ""),
        chat_type=message.get("chat_type", ""),
        message_type=message_type,
        sender_open_id=sender_id.get("open_id", ""),
        sender_user_id=sender_id.get("user_id", ""),
        sender_union_id=sender_id.get("union_id", ""),
        source_type=source_type,
        report_type=report_type,
        should_audit=should_audit,
        extracted_text=text,
        audit_input=audit_input,
        extracted_links=links,
        document_id=document_id,
        document_title=document_title,
        file_key=file_key,
        file_name=file_name,
        metadata=metadata,
    )


def _build_audit_input(
    *,
    source_type: str,
    report_type: str,
    message_type: str,
    text: str,
    document_title: str,
    file_name: str,
) -> str:
    header = [
        f"来源类型：{source_type}",
        f"汇报类型：{report_type}",
        f"消息类型：{message_type}",
    ]
    if document_title:
        header.append(f"文档标题：{document_title}")
    if file_name:
        header.append(f"附件名称：{file_name}")
    header.append("正文：")
    header.append(text)
    return "\n".join(part for part in header if part)


def _flatten_structured_content(content: dict[str, Any]) -> tuple[str, list[str]]:
    parts: list[str] = []
    links: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            href = node.get("href")
            if isinstance(href, str) and href:
                links.append(href)
            for key in ("title", "text", "user_name", "summary"):
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())
            for value in node.values():
                walk(value)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(content)
    flattened = _join_parts(parts)
    links.extend(_extract_links_from_text(flattened))
    return flattened, _unique_preserve(links)


def _extract_attachment_text(
    message_id: str,
    resource_key: str,
    file_name: str,
    *,
    resource_type: str,
) -> tuple[str, dict[str, Any]]:
    if not resource_key:
        return "", {"file_parse": "missing_file_key"}

    effective_name = file_name or (
        f"{resource_key}.png" if resource_type == "image" else resource_key
    )
    data, content_type = download_message_resource(message_id, resource_key, resource_type=resource_type)
    suffix = Path(effective_name).suffix.lower()

    if suffix in TEXT_FILE_SUFFIXES:
        return _decode_text_bytes(data), {
            "file_parse": "text",
            "file_content_type": content_type,
            "downloaded_bytes": len(data),
        }
    if suffix == ".docx":
        return _parse_docx_bytes(data), {
            "file_parse": "docx",
            "file_content_type": content_type,
            "downloaded_bytes": len(data),
        }
    if suffix == ".pdf":
        pdf_text = _parse_pdf_bytes(data)
        ocr_text, ocr_ok, ocr_error = ocr_pdf_text(data)
        parts = [part.strip() for part in (pdf_text, ocr_text) if part and part.strip()]
        combined = _join_parts(parts)
        document_key = _attachment_document_key(message_id, resource_key, effective_name)
        fetch_status = "success" if combined else ("empty" if ocr_ok else "unsupported")
        upsert_document(
            document_key,
            document_url="",
            document_type="attachment_pdf",
            title=effective_name,
            content_text=combined,
            content_hash=_hash_text(combined) if combined else "",
            fetch_status=fetch_status,
            metadata={
                "ocr_available": ocr_ok,
                "ocr_error": ocr_error,
                "pdf_text_available": bool(pdf_text),
                "file_key": resource_key,
                "resource_type": resource_type,
                "message_id": message_id,
            },
        )
        if combined:
            return combined, {
                "file_parse": "pdf",
                "file_content_type": content_type,
                "downloaded_bytes": len(data),
                "ocr_available": ocr_ok,
                "ocr_error": ocr_error,
                "document_key": document_key,
            }

    if suffix in IMAGE_FILE_SUFFIXES:
        ocr_text, ocr_ok, ocr_error = ocr_image_text(data)
        document_key = _attachment_document_key(message_id, resource_key, effective_name)
        fetch_status = "success" if ocr_text else ("empty" if ocr_ok else "unsupported")
        upsert_document(
            document_key,
            document_url="",
            document_type="attachment_image",
            title=effective_name,
            content_text=ocr_text,
            content_hash=_hash_text(ocr_text) if ocr_text else "",
            fetch_status=fetch_status,
            metadata={
                "ocr_available": ocr_ok,
                "ocr_error": ocr_error,
                "file_key": resource_key,
                "resource_type": resource_type,
                "message_id": message_id,
            },
        )
        return ocr_text, {
            "file_parse": "image",
            "file_content_type": content_type,
            "downloaded_bytes": len(data),
            "ocr_available": ocr_ok,
            "ocr_error": ocr_error,
            "document_key": document_key,
        }

    return "", {
        "file_parse": "unsupported",
        "file_content_type": content_type,
        "downloaded_bytes": len(data),
    }


def _extract_file_text(message_id: str, file_key: str, file_name: str) -> tuple[str, dict[str, Any]]:
    if not file_name:
        return "", {"file_parse": "missing_file_name"}
    return _extract_attachment_text(
        message_id,
        file_key,
        file_name,
        resource_type="file",
    )


def _extract_image_text(message_id: str, image_key: str, image_name: str = "") -> tuple[str, dict[str, Any]]:
    return _extract_attachment_text(
        message_id,
        image_key,
        image_name or f"{image_key}.png",
        resource_type="image",
    )


def _parse_docx_bytes(data: bytes) -> str:
    try:
        with zipfile.ZipFile(BytesIO(data)) as archive:
            xml_bytes = archive.read("word/document.xml")
    except (KeyError, zipfile.BadZipFile):
        return ""

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return ""

    paragraphs = []
    for paragraph in root.findall(".//w:p", WORD_NAMESPACE):
        texts = [
            node.text.strip()
            for node in paragraph.findall(".//w:t", WORD_NAMESPACE)
            if node.text and node.text.strip()
        ]
        if texts:
            paragraphs.append("".join(texts))
    return "\n".join(paragraphs)


def _parse_pdf_bytes(data: bytes) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except ImportError:
        return ""

    try:
        reader = PdfReader(BytesIO(data))
    except Exception:
        return ""

    texts = [page.extract_text() or "" for page in reader.pages]
    return _normalize_whitespace("\n".join(texts))


def _decode_text_bytes(data: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _extract_links_from_text(text: str) -> list[str]:
    links = MARKDOWN_LINK_RE.findall(text)
    links.extend(URL_RE.findall(text))
    cleaned = [link.rstrip(".,)") for link in links]
    return _unique_preserve(cleaned)


def _extract_document_id(links: list[str]) -> str:
    for link in links:
        match = DOCX_TOKEN_RE.search(link)
        if match:
            return match.group(1)

        parsed = urlparse(link)
        for fragment in (parsed.path, parsed.query):
            match = DOCX_TOKEN_RE.search(fragment)
            if match:
                return match.group(1)
    return ""


def _fetch_docx_title(document_id: str, metadata: dict[str, Any]) -> str:
    try:
        result = get_docx_document_meta(document_id)
    except Exception as exc:
        logger.warning("读取文档标题失败 document_id=%s error=%s", document_id, exc)
        metadata["docx_title_error"] = str(exc)
        return ""

    data = result.get("data", {})
    document = data.get("document", {})
    title = document.get("title", "")
    if title:
        metadata["docx_title_fetched"] = True
    return title


def _fetch_docx_text(document_id: str, metadata: dict[str, Any]) -> str:
    try:
        result = get_docx_raw_content(document_id)
    except Exception as exc:
        logger.warning("读取文档正文失败 document_id=%s error=%s", document_id, exc)
        metadata["docx_fetch_error"] = str(exc)
        return ""

    data = result.get("data", {})
    content = _normalize_whitespace(data.get("content", ""))
    if content:
        metadata["docx_raw_content_fetched"] = True
    return content


def _detect_report_type(text: str) -> str:
    if any(keyword in text for keyword in ("会议纪要", "会议总结", "会议记录", "周会", "复盘会", "项目周会", "OKR进度复盘")):
        return "会议纪要"
    if "月报" in text:
        return "月报"
    return "周报"


def _detect_source_type(
    *,
    message_type: str,
    report_type: str,
    document_id: str,
    file_name: str,
) -> str:
    if document_id and report_type == "会议纪要":
        return "meeting_doc"
    if message_type == "interactive":
        return "meeting_card" if report_type == "会议纪要" else "interactive_report"
    if message_type == "post":
        return "rich_text_report"
    if message_type in {"file", "image"}:
        return "file_attachment"
    if message_type == "text":
        return "weekly_text" if report_type != "会议纪要" else "meeting_text"
    if file_name:
        return "file_attachment"
    return "unknown"


def _should_audit(
    *,
    text: str,
    message_type: str,
    document_id: str,
    file_name: str,
) -> bool:
    if document_id:
        return True
    if any(keyword in text for keyword in TRIGGER_KEYWORDS):
        return True
    if message_type in {"post", "interactive"} and _detect_report_type(text) == "会议纪要":
        return True
    if file_name and any(keyword in file_name for keyword in TRIGGER_KEYWORDS):
        return True
    return False


def _truncate_for_audit(text: str) -> str:
    if len(text) <= MAX_AUDIT_INPUT_CHARS:
        return text
    suffix = "\n\n[内容过长，已截断]"
    return text[: MAX_AUDIT_INPUT_CHARS - len(suffix)] + suffix


def _normalize_whitespace(text: str) -> str:
    return "\n".join(line.strip() for line in str(text).splitlines() if line.strip())


def _join_parts(parts: list[str]) -> str:
    return "\n".join(part for part in _unique_preserve(parts) if part)


def _unique_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique
