from __future__ import annotations

import json
import os
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import paramiko
import requests


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"
SCHEDULED_DIR = ROOT / "output" / "scheduled"
DELIVERY_DIR = ROOT / "output" / "delivery"

ENV_HOST = "BYDFI_TENCENT_HOST"
ENV_USER = "BYDFI_TENCENT_USER"
ENV_PASSWORD = "BYDFI_TENCENT_PASSWORD"
ENV_REMOTE_DIR = "BYDFI_REPORT_REMOTE_DIR"
ENV_BASE_URL = "BYDFI_REPORT_BASE_URL"
ENV_WEBHOOK = "BYDFI_LARK_WEBHOOK_URL"
WEBHOOK_RETRY_DELAYS = (8, 20, 45)
WEBHOOK_RATE_LIMIT_CODES = {11232, 99991663}


@dataclass
class DeliverySettings:
    host: str
    user: str
    password: str
    remote_dir: str
    base_url: str
    webhook_url: str


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, "").strip() or default


def load_delivery_settings() -> DeliverySettings | None:
    settings = DeliverySettings(
        host=_env(ENV_HOST),
        user=_env(ENV_USER, "ubuntu"),
        password=_env(ENV_PASSWORD),
        remote_dir=_env(ENV_REMOTE_DIR, "/opt/bydfi-agent-web/public/reports"),
        base_url=_env(ENV_BASE_URL),
        webhook_url=_env(ENV_WEBHOOK),
    )
    required = [settings.host, settings.user, settings.password, settings.remote_dir, settings.base_url, settings.webhook_url]
    if all(required):
        return settings
    return None


def ensure_delivery_dir() -> None:
    DELIVERY_DIR.mkdir(parents=True, exist_ok=True)


def latest_ceo_pdf(period: str, ceo_paths: dict[str, Any] | None = None) -> Path | None:
    if ceo_paths:
        latest_pdf = str(ceo_paths.get("latest_pdf_path", "")).strip()
        if latest_pdf:
            path = Path(latest_pdf)
            if path.exists():
                return path.resolve()
    scheduled_latest = SCHEDULED_DIR / f"{period}_ceo_brief_latest.pdf"
    if scheduled_latest.exists():
        return scheduled_latest.resolve()
    files = sorted(REPORTS_DIR.glob("ceo_brief_*.pdf"), key=lambda item: item.stat().st_mtime, reverse=True)
    if files:
        return files[0].resolve()
    return None


def latest_digest_asset(period: str, suffix: str) -> Path | None:
    target = SCHEDULED_DIR / f"{period}_ops_digest_latest{suffix}"
    return target if target.exists() else None


def build_delivery_package(period: str, *, digest_pdf: Path, digest_json: Path | None, digest_md: Path | None, ceo_pdf: Path | None) -> Path:
    ensure_delivery_dir()
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    package_path = DELIVERY_DIR / f"bydfi_{period}_ops_package_{stamp}.zip"
    with zipfile.ZipFile(package_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.write(digest_pdf, arcname=digest_pdf.name)
        if digest_json and digest_json.exists():
            archive.write(digest_json, arcname=digest_json.name)
        if digest_md and digest_md.exists():
            archive.write(digest_md, arcname=digest_md.name)
        if ceo_pdf and ceo_pdf.exists():
            archive.write(ceo_pdf, arcname=ceo_pdf.name)
    return package_path


def _mkdir_p(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    current = ""
    for segment in [part for part in remote_dir.replace("\\", "/").split("/") if part]:
        current = f"{current}/{segment}"
        try:
            sftp.stat(current)
        except IOError:
            sftp.mkdir(current)


def upload_file(local_path: Path, remote_name: str, settings: DeliverySettings) -> str:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=settings.host, username=settings.user, password=settings.password, timeout=20)
    try:
        sftp = client.open_sftp()
        try:
            _mkdir_p(sftp, settings.remote_dir)
            remote_path = f"{settings.remote_dir.rstrip('/')}/{remote_name}"
            sftp.put(str(local_path), remote_path)
            sftp.chmod(remote_path, 0o644)
        finally:
            sftp.close()
    finally:
        client.close()
    return f"{settings.base_url.rstrip('/')}/{remote_name}"


def compose_webhook_text(period: str, payload: dict[str, Any], urls: dict[str, str]) -> str:
    required = payload.get("coverage", {}).get("summary", {}).get("required", {}) or {}
    focus = payload.get("focus_groups", []) or []
    risks = payload.get("delay_risk_groups", []) or []
    people = payload.get("top_output_people", []) or []
    publish_guard = payload.get("publish_guard", {}) or {}
    generated = str(payload.get("generated_at", "")).replace("T", " ")[:19]
    focus_names = "、".join(str(item.get("group_title", "")).strip() for item in focus[:3] if str(item.get("group_title", "")).strip()) or "-"
    risk_names = "、".join(str(item.get("group_title", "")).strip() for item in risks[:3] if str(item.get("group_title", "")).strip()) or "-"
    people_names = "、".join(str(item.get("name", "")).strip() for item in people[:3] if str(item.get("name", "")).strip()) or "-"
    title = "日报" if period == "daily" else "周报"
    headline = str(payload.get("one_liner", "")).strip() or "采集与分析已完成，请查看 PDF。"

    guard_status = str(publish_guard.get("guard_status", "")).strip()
    if guard_status == "pass":
        review_status = "可转发给 CEO"
        ceo_line = f"CEO版 PDF：{urls.get('ceo_pdf_latest', urls.get('digest_pdf_latest', '-'))}"
    elif guard_status == "review_only":
        review_status = "先发你审阅，不建议直接转 CEO"
        ceo_line = f"审阅版 CEO PDF：{urls.get('ceo_pdf_latest', '-')}"
    else:
        review_status = "存在阻断，但仍先发你审阅"
        ceo_line = f"审阅版 CEO PDF：{urls.get('ceo_pdf_latest', '-')}"

    lines = [
        f"BYDFI {title}管理分析已生成",
        f"时间：{generated}",
        f"状态：{review_status}",
        f"一句话：{headline}",
        f"覆盖：covered={required.get('covered', 0)} / missing={required.get('missing', 0)} / stale={required.get('stale', 0)} / unverified={required.get('unverified', 0)}",
        f"重点业务：{focus_names}",
        f"延期风险：{risk_names}",
        f"输出前排：{people_names}",
        ceo_line,
        f"内部 digest：{urls.get('digest_pdf_latest', '-')}",
        f"打包文件：{urls.get('package_latest', '-')}",
    ]

    for reason in publish_guard.get("hard_reasons", [])[:3]:
        lines.append(f"阻断原因：{reason}")
    for note in publish_guard.get("review_notes", [])[:2]:
        lines.append(f"提示：{note}")
    return "\n".join(lines)


def send_webhook(text: str, settings: DeliverySettings) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    total_attempts = len(WEBHOOK_RETRY_DELAYS) + 1
    for index in range(total_attempts):
        try:
            response = requests.post(
                settings.webhook_url,
                json={"msg_type": "text", "content": {"text": text}},
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            attempts.append({"attempt": index + 1, "error": str(exc)})
            if index >= len(WEBHOOK_RETRY_DELAYS):
                raise RuntimeError(f"Webhook delivery failed after retries: {attempts}") from exc
            time.sleep(WEBHOOK_RETRY_DELAYS[index])
            continue

        code = payload.get("StatusCode")
        if code in (None, ""):
            code = payload.get("code")
        if code in (0, None):
            attempts.append(
                {
                    "attempt": index + 1,
                    "code": code,
                    "msg": str(payload.get("msg") or payload.get("StatusMessage") or "").strip(),
                }
            )
            payload["attempts"] = attempts
            return payload

        attempts.append(
            {
                "attempt": index + 1,
                "code": code,
                "msg": str(payload.get("msg") or payload.get("StatusMessage") or "").strip(),
            }
        )
        if int(code) not in WEBHOOK_RATE_LIMIT_CODES or index >= len(WEBHOOK_RETRY_DELAYS):
            raise RuntimeError(f"Webhook delivery failed: {payload}")
        time.sleep(WEBHOOK_RETRY_DELAYS[index])

    raise RuntimeError(f"Webhook delivery failed after retries: {attempts}")


def write_delivery_record(period: str, payload: dict[str, Any]) -> dict[str, str]:
    ensure_delivery_dir()
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    record_path = DELIVERY_DIR / f"{period}_delivery_{stamp}.json"
    latest_path = DELIVERY_DIR / f"{period}_delivery_latest.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    record_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return {
        "record_path": str(record_path.resolve()),
        "latest_record_path": str(latest_path.resolve()),
    }


def deliver_digest_report(period: str, payload: dict[str, Any], pdf_paths: dict[str, str], ceo_paths: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = load_delivery_settings()
    if settings is None:
        raise RuntimeError("Delivery settings are incomplete. Set BYDFI_TENCENT_* and BYDFI_LARK_WEBHOOK_URL first.")

    period_key = str(period).strip().lower() or "daily"
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    digest_pdf = Path(pdf_paths["latest_pdf_path"]).resolve()
    digest_json = latest_digest_asset(period_key, ".json")
    digest_md = latest_digest_asset(period_key, ".md")
    ceo_pdf = latest_ceo_pdf(period_key, ceo_paths=ceo_paths)
    package_path = build_delivery_package(period_key, digest_pdf=digest_pdf, digest_json=digest_json, digest_md=digest_md, ceo_pdf=ceo_pdf)

    urls = {
        "digest_pdf": upload_file(digest_pdf, f"bydfi_{period_key}_ops_{stamp}.pdf", settings),
        "digest_pdf_latest": upload_file(digest_pdf, f"bydfi_{period_key}_ops_latest.pdf", settings),
        "package": upload_file(package_path, f"bydfi_{period_key}_ops_package_{stamp}.zip", settings),
        "package_latest": upload_file(package_path, f"bydfi_{period_key}_ops_package_latest.zip", settings),
    }
    if ceo_pdf and ceo_pdf.exists():
        urls["ceo_pdf"] = upload_file(ceo_pdf, f"bydfi_{period_key}_ceo_brief_{stamp}.pdf", settings)
        urls["ceo_pdf_latest"] = upload_file(ceo_pdf, f"bydfi_{period_key}_ceo_brief_latest.pdf", settings)
        urls["ceo_pdf_global_latest"] = upload_file(ceo_pdf, "bydfi_ceo_brief_latest.pdf", settings)

    webhook_text = compose_webhook_text(period_key, payload, urls)
    webhook_result = send_webhook(webhook_text, settings)
    record = {
        "period": period_key,
        "generated_at": payload.get("generated_at", ""),
        "digest_pdf_path": str(digest_pdf),
        "package_path": str(package_path),
        "ceo_pdf_path": str(ceo_pdf.resolve()) if ceo_pdf and ceo_pdf.exists() else "",
        "urls": urls,
        "webhook_result": webhook_result,
    }
    record_paths = write_delivery_record(period_key, record)
    record.update(record_paths)
    return record
