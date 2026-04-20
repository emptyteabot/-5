from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from config import DB_PATH

STORAGE_PATH = DB_PATH


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _normalize_db_path(db_path: Optional[str]) -> Path:
    target = db_path or DB_PATH
    return Path(target).expanduser()


def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = _normalize_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    for key in ("raw_event_json", "raw_message_json", "metadata_json"):
        raw = payload.get(key)
        if raw:
            try:
                payload[key] = json.loads(raw)
            except json.JSONDecodeError:
                pass
    return payload


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row["name"] for row in rows}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db(db_path: Optional[str] = None) -> str:
    path = _normalize_db_path(db_path)
    with _connect(str(path)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                chat_id TEXT DEFAULT '',
                sender_id TEXT DEFAULT '',
                reporter_name TEXT DEFAULT '',
                message_type TEXT DEFAULT '',
                source_type TEXT DEFAULT '',
                source_title TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                document_key TEXT DEFAULT '',
                source_content_hash TEXT DEFAULT '',
                parsed_text TEXT DEFAULT '',
                audit_result TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'received',
                error_message TEXT DEFAULT '',
                raw_event_json TEXT,
                raw_message_json TEXT,
                message_timestamp TEXT DEFAULT '',
                collected_at TEXT DEFAULT '',
                is_backfill INTEGER NOT NULL DEFAULT 0,
                run_id TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        for column, ddl in (
            ("chat_id", "TEXT DEFAULT ''"),
            ("sender_id", "TEXT DEFAULT ''"),
            ("message_type", "TEXT DEFAULT ''"),
            ("source_type", "TEXT DEFAULT ''"),
            ("source_title", "TEXT DEFAULT ''"),
            ("parsed_text", "TEXT DEFAULT ''"),
            ("audit_result", "TEXT DEFAULT ''"),
            ("status", "TEXT NOT NULL DEFAULT 'received'"),
            ("error_message", "TEXT DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ("raw_event_json", "TEXT"),
            ("raw_message_json", "TEXT"),
            ("reporter_name", "TEXT DEFAULT ''"),
            ("source_url", "TEXT DEFAULT ''"),
            ("document_key", "TEXT DEFAULT ''"),
            ("source_content_hash", "TEXT DEFAULT ''"),
            ("message_timestamp", "TEXT DEFAULT ''"),
            ("collected_at", "TEXT DEFAULT ''"),
            ("is_backfill", "INTEGER NOT NULL DEFAULT 0"),
            ("run_id", "TEXT DEFAULT ''"),
        ):
            _ensure_column(conn, "audit_records", column, ddl)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_key TEXT NOT NULL UNIQUE,
                document_url TEXT DEFAULT '',
                document_type TEXT DEFAULT '',
                title TEXT DEFAULT '',
                content_text TEXT DEFAULT '',
                content_hash TEXT DEFAULT '',
                last_fetched_at TEXT DEFAULT '',
                fetch_status TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                metadata_json TEXT DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS collection_runs (
                run_id TEXT PRIMARY KEY,
                chat_id TEXT DEFAULT '',
                started_at TEXT NOT NULL,
                finished_at TEXT DEFAULT '',
                since_timestamp TEXT DEFAULT '',
                new_messages INTEGER NOT NULL DEFAULT 0,
                updated_messages INTEGER NOT NULL DEFAULT 0,
                fetched_documents INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                error_message TEXT DEFAULT ''
            )
            """
        )
        for column, ddl in (
            ("document_url", "TEXT DEFAULT ''"),
            ("title", "TEXT DEFAULT ''"),
            ("content_text", "TEXT DEFAULT ''"),
            ("content_hash", "TEXT DEFAULT ''"),
            ("last_fetched_at", "TEXT DEFAULT ''"),
            ("fetch_status", "TEXT DEFAULT ''"),
            ("error_message", "TEXT DEFAULT ''"),
            ("metadata_json", "TEXT DEFAULT ''"),
        ):
            _ensure_column(conn, "source_documents", column, ddl)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_records_status ON audit_records(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_records_updated_at ON audit_records(updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_records_chat_time ON audit_records(chat_id, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_records_document_key ON audit_records(document_key)")
        conn.commit()
    return str(path)


def get_record(message_id: str, db_path: Optional[str] = None) -> dict[str, Any] | None:
    init_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM audit_records WHERE message_id = ?",
            (message_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def get_document(document_key: str, db_path: Optional[str] = None) -> dict[str, Any] | None:
    if not document_key:
        return None
    init_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM source_documents WHERE document_key = ?",
            (document_key,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def upsert_document(
    document_key: str,
    *,
    document_url: str = "",
    document_type: str = "",
    title: str = "",
    content_text: str = "",
    content_hash: str = "",
    last_fetched_at: str | None = None,
    fetch_status: str = "",
    error_message: str = "",
    metadata: Any = None,
    db_path: Optional[str] = None,
) -> dict[str, Any]:
    if not document_key:
        raise ValueError("document_key is required")

    init_db(db_path)
    fetched_at = last_fetched_at or _utc_now()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO source_documents (
                document_key, document_url, document_type, title, content_text,
                content_hash, last_fetched_at, fetch_status, error_message, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(document_key) DO UPDATE SET
                document_url = CASE WHEN excluded.document_url <> '' THEN excluded.document_url ELSE source_documents.document_url END,
                document_type = CASE WHEN excluded.document_type <> '' THEN excluded.document_type ELSE source_documents.document_type END,
                title = CASE WHEN excluded.title <> '' THEN excluded.title ELSE source_documents.title END,
                content_text = CASE WHEN excluded.content_text <> '' THEN excluded.content_text ELSE source_documents.content_text END,
                content_hash = CASE WHEN excluded.content_hash <> '' THEN excluded.content_hash ELSE source_documents.content_hash END,
                last_fetched_at = excluded.last_fetched_at,
                fetch_status = CASE WHEN excluded.fetch_status <> '' THEN excluded.fetch_status ELSE source_documents.fetch_status END,
                error_message = excluded.error_message,
                metadata_json = COALESCE(excluded.metadata_json, source_documents.metadata_json)
            """,
            (
                document_key,
                document_url,
                document_type,
                title,
                content_text,
                content_hash,
                fetched_at,
                fetch_status,
                error_message,
                _to_json(metadata),
            ),
        )
        conn.commit()

    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM source_documents WHERE document_key = ?",
            (document_key,),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"failed to persist document for document_key={document_key}")
    return _row_to_dict(row)


def create_collection_run(
    run_id: str,
    *,
    chat_id: str,
    since_timestamp: str = "",
    db_path: Optional[str] = None,
) -> None:
    init_db(db_path)
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO collection_runs (
                run_id, chat_id, started_at, since_timestamp, status
            ) VALUES (?, ?, ?, ?, 'running')
            """,
            (run_id, chat_id, _utc_now(), since_timestamp),
        )
        conn.commit()


def finish_collection_run(
    run_id: str,
    *,
    new_messages: int = 0,
    updated_messages: int = 0,
    fetched_documents: int = 0,
    status: str = "completed",
    error_message: str = "",
    db_path: Optional[str] = None,
) -> None:
    init_db(db_path)
    with _connect(db_path) as conn:
        conn.execute(
            """
            UPDATE collection_runs
            SET finished_at = ?, new_messages = ?, updated_messages = ?, fetched_documents = ?,
                status = ?, error_message = ?
            WHERE run_id = ?
            """,
            (_utc_now(), new_messages, updated_messages, fetched_documents, status, error_message, run_id),
        )
        conn.commit()


def upsert_message(
    message_id: str,
    *,
    chat_id: str = "",
    sender_id: str = "",
    reporter_name: str = "",
    message_type: str = "",
    raw_event: Any = None,
    raw_message: Any = None,
    status: str = "received",
    created_at: str | None = None,
    source_url: str = "",
    document_key: str = "",
    source_content_hash: str = "",
    message_timestamp: str | None = None,
    collected_at: str | None = None,
    is_backfill: bool = False,
    run_id: str = "",
    db_path: Optional[str] = None,
) -> dict[str, Any]:
    if not message_id:
        raise ValueError("message_id is required")

    init_db(db_path)
    now = _utc_now()
    created_at_value = created_at or now
    collected_at_value = collected_at or now
    message_timestamp_value = message_timestamp or created_at_value

    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO audit_records (
                message_id, chat_id, sender_id, reporter_name, message_type, status,
                raw_event_json, raw_message_json, source_url, document_key, source_content_hash,
                message_timestamp, collected_at, is_backfill, run_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                chat_id = CASE WHEN excluded.chat_id <> '' THEN excluded.chat_id ELSE audit_records.chat_id END,
                sender_id = CASE WHEN excluded.sender_id <> '' THEN excluded.sender_id ELSE audit_records.sender_id END,
                reporter_name = CASE WHEN excluded.reporter_name <> '' THEN excluded.reporter_name ELSE audit_records.reporter_name END,
                message_type = CASE WHEN excluded.message_type <> '' THEN excluded.message_type ELSE audit_records.message_type END,
                status = CASE WHEN excluded.status <> '' THEN excluded.status ELSE audit_records.status END,
                raw_event_json = COALESCE(excluded.raw_event_json, audit_records.raw_event_json),
                raw_message_json = COALESCE(excluded.raw_message_json, audit_records.raw_message_json),
                source_url = CASE WHEN excluded.source_url <> '' THEN excluded.source_url ELSE audit_records.source_url END,
                document_key = CASE WHEN excluded.document_key <> '' THEN excluded.document_key ELSE audit_records.document_key END,
                source_content_hash = CASE WHEN excluded.source_content_hash <> '' THEN excluded.source_content_hash ELSE audit_records.source_content_hash END,
                message_timestamp = CASE WHEN excluded.message_timestamp <> '' THEN excluded.message_timestamp ELSE audit_records.message_timestamp END,
                collected_at = CASE WHEN excluded.collected_at <> '' THEN excluded.collected_at ELSE audit_records.collected_at END,
                is_backfill = CASE WHEN excluded.is_backfill <> 0 THEN excluded.is_backfill ELSE audit_records.is_backfill END,
                run_id = CASE WHEN excluded.run_id <> '' THEN excluded.run_id ELSE audit_records.run_id END,
                created_at = CASE WHEN audit_records.created_at = '' THEN excluded.created_at ELSE audit_records.created_at END,
                updated_at = excluded.updated_at
            """,
            (
                message_id,
                chat_id,
                sender_id,
                reporter_name,
                message_type,
                status,
                _to_json(raw_event),
                _to_json(raw_message),
                source_url,
                document_key,
                source_content_hash,
                message_timestamp_value,
                collected_at_value,
                1 if is_backfill else 0,
                run_id,
                created_at_value,
                now,
            ),
        )
        conn.commit()

    record = get_record(message_id, db_path)
    if record is None:
        raise RuntimeError(f"failed to persist record for message_id={message_id}")
    return record


def set_parsed_content(
    message_id: str,
    *,
    source_type: str | None = None,
    source_title: str | None = None,
    parsed_text: str | None = None,
    source_url: str | None = None,
    document_key: str | None = None,
    source_content_hash: str | None = None,
    reporter_name: str | None = None,
    status: str | None = None,
    db_path: Optional[str] = None,
) -> bool:
    init_db(db_path)
    updates = ["updated_at = ?"]
    params: list[Any] = [_utc_now()]

    if source_type is not None:
        updates.append("source_type = ?")
        params.append(source_type)
    if source_title is not None:
        updates.append("source_title = ?")
        params.append(source_title)
    if parsed_text is not None:
        updates.append("parsed_text = ?")
        params.append(parsed_text)
    if source_url is not None:
        updates.append("source_url = ?")
        params.append(source_url)
    if document_key is not None:
        updates.append("document_key = ?")
        params.append(document_key)
    if source_content_hash is not None:
        updates.append("source_content_hash = ?")
        params.append(source_content_hash)
    if reporter_name is not None:
        updates.append("reporter_name = ?")
        params.append(reporter_name)
    if status is not None:
        updates.append("status = ?")
        params.append(status)

    params.append(message_id)
    with _connect(db_path) as conn:
        cursor = conn.execute(
            f"UPDATE audit_records SET {', '.join(updates)} WHERE message_id = ?",
            params,
        )
        conn.commit()
        return cursor.rowcount > 0


def set_audit_result(
    message_id: str,
    audit_result: str,
    *,
    status: str = "audited",
    db_path: Optional[str] = None,
) -> bool:
    init_db(db_path)
    with _connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE audit_records
            SET audit_result = ?, status = ?, error_message = '', updated_at = ?
            WHERE message_id = ?
            """,
            (audit_result, status, _utc_now(), message_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def set_error(
    message_id: str,
    error_text: str,
    *,
    status: str = "error",
    db_path: Optional[str] = None,
) -> bool:
    init_db(db_path)
    with _connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE audit_records
            SET error_message = ?, status = ?, updated_at = ?
            WHERE message_id = ?
            """,
            (error_text, status, _utc_now(), message_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def list_recent_records(limit: int = 20, db_path: Optional[str] = None) -> list[dict[str, Any]]:
    init_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM audit_records ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def list_records_for_chat(
    chat_id: str,
    *,
    start_at: Optional[str] = None,
    end_at: Optional[str] = None,
    statuses: Optional[list[str]] = None,
    db_path: Optional[str] = None,
) -> list[dict[str, Any]]:
    init_db(db_path)
    sql = "SELECT * FROM audit_records WHERE chat_id = ?"
    params: list[Any] = [chat_id]

    if start_at:
        sql += " AND created_at >= ?"
        params.append(start_at)
    if end_at:
        sql += " AND created_at <= ?"
        params.append(end_at)
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        sql += f" AND status IN ({placeholders})"
        params.extend(statuses)

    sql += " ORDER BY created_at ASC"

    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_latest_collection_run(chat_id: str, db_path: Optional[str] = None) -> dict[str, Any] | None:
    init_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM collection_runs
            WHERE chat_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None
