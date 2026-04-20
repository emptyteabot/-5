CREATE INDEX idx_audit_records_chat_time ON audit_records(chat_id, created_at)

CREATE INDEX idx_audit_records_document_key ON audit_records(document_key)

CREATE INDEX idx_audit_records_status ON audit_records(status)

CREATE INDEX idx_audit_records_updated_at ON audit_records(updated_at)

CREATE INDEX idx_audit_runs_message_id
            ON audit_runs(message_id)

CREATE INDEX idx_dept_time ON dept_meeting_history(dept, meeting_time)

CREATE INDEX idx_inbound_messages_source_type
            ON inbound_messages(source_type)

CREATE INDEX idx_inbound_messages_status
            ON inbound_messages(status)

CREATE TABLE audit_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                chat_id TEXT DEFAULT '',
                sender_id TEXT DEFAULT '',
                message_type TEXT DEFAULT '',
                source_type TEXT DEFAULT '',
                source_title TEXT DEFAULT '',
                parsed_text TEXT DEFAULT '',
                audit_result TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'received',
                error_message TEXT DEFAULT '',
                raw_event_json TEXT,
                raw_message_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            , reporter_name TEXT DEFAULT '', source_url TEXT DEFAULT '', document_key TEXT DEFAULT '', source_content_hash TEXT DEFAULT '', message_timestamp TEXT DEFAULT '', collected_at TEXT DEFAULT '', is_backfill INTEGER NOT NULL DEFAULT 0, run_id TEXT DEFAULT '')

CREATE TABLE audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                report_type TEXT NOT NULL,
                model TEXT NOT NULL,
                input_text TEXT NOT NULL,
                output_text TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(message_id) REFERENCES inbound_messages(message_id)
            )

CREATE TABLE collection_runs (
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

CREATE TABLE dept_meeting_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dept TEXT NOT NULL,
                meeting_title TEXT NOT NULL,
                section_title TEXT NOT NULL,
                meeting_time TEXT NOT NULL,
                content TEXT NOT NULL,
                source_key TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            )

CREATE TABLE inbound_messages (
                message_id TEXT PRIMARY KEY,
                chat_id TEXT NOT NULL,
                thread_id TEXT,
                root_id TEXT,
                parent_id TEXT,
                chat_type TEXT,
                message_type TEXT NOT NULL,
                source_type TEXT NOT NULL DEFAULT 'unknown',
                report_type TEXT NOT NULL DEFAULT '',
                should_audit INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'received',
                sender_open_id TEXT,
                sender_user_id TEXT,
                sender_union_id TEXT,
                message_text TEXT,
                document_id TEXT,
                document_title TEXT,
                file_key TEXT,
                file_name TEXT,
                extracted_links TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                raw_content TEXT NOT NULL DEFAULT '',
                raw_event TEXT NOT NULL DEFAULT '',
                error_text TEXT NOT NULL DEFAULT '',
                audit_result TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )

CREATE TABLE source_documents (
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

CREATE TABLE sqlite_sequence(name,seq)
