from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from db_infrastructure import connect

LOGGER = logging.getLogger("aurora_wms.sheets")
_THREADS: dict[str, threading.Thread] = {}
_LOCK = threading.RLock()
_STOP_EVENTS: dict[str, threading.Event] = {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class SheetsSyncConfig:
    webhook_url: str
    token: str
    interval_seconds: int = 20
    batch_size: int = 200
    timeout_seconds: int = 20
    max_attempts: int = 100

    @property
    def enabled(self) -> bool:
        # El Apps Script actual no exige token. Basta con una URL válida.
        return bool(self.webhook_url)


def config_from_env(overrides: dict[str, Any] | None = None) -> SheetsSyncConfig:
    values = dict(overrides or {})
    return SheetsSyncConfig(
        webhook_url=str(values.get("webhook_url") or os.getenv("AURORA_SHEETS_WEBHOOK_URL", "")).strip(),
        token=str(values.get("token") or os.getenv("AURORA_SHEETS_TOKEN", "")).strip(),
        interval_seconds=max(5, int(values.get("interval_seconds") or os.getenv("AURORA_SHEETS_INTERVAL_SECONDS", "20"))),
        batch_size=max(10, min(500, int(values.get("batch_size") or os.getenv("AURORA_SHEETS_BATCH_SIZE", "200")))),
        timeout_seconds=max(5, int(values.get("timeout_seconds") or os.getenv("AURORA_SHEETS_TIMEOUT_SECONDS", "20"))),
        max_attempts=max(1, int(values.get("max_attempts") or os.getenv("AURORA_SHEETS_MAX_ATTEMPTS", "100"))),
    )


def ensure_sync_schema(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sheets_sync_state (
                id INTEGER PRIMARY KEY CHECK (id=1),
                enabled INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                last_success_at TEXT,
                last_error TEXT,
                last_batch_count INTEGER NOT NULL DEFAULT 0,
                total_synced INTEGER NOT NULL DEFAULT 0,
                worker_started_at TEXT
            );
            INSERT OR IGNORE INTO sheets_sync_state(id,enabled,total_synced,last_batch_count)
            VALUES(1,0,0,0);
            CREATE INDEX IF NOT EXISTS idx_audit_pending_retry
            ON audit_events(sync_status, sync_attempts, id);
            """
        )
        # Recuperación de un proceso que murió después de marcar un lote como SYNCING.
        conn.execute("UPDATE audit_events SET sync_status='PENDING' WHERE sync_status='SYNCING'")
        conn.commit()


def _event_rows(conn: sqlite3.Connection, batch_size: int, max_attempts: int) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT id,event_uuid,created_at,module,action,entity_type,entity_id,
               user_name,session_id,before_json,after_json,sync_attempts
          FROM audit_events
         WHERE sync_status IN ('PENDING','ERROR')
           AND sync_attempts < ?
         ORDER BY id
         LIMIT ?
        """,
        (max_attempts, batch_size),
    ).fetchall()


def _serialize_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "local_id": int(row["id"]),
        "event_uuid": row["event_uuid"],
        "created_at": row["created_at"],
        "module": row["module"],
        "action": row["action"],
        "entity_type": row["entity_type"],
        "entity_id": row["entity_id"],
        "user_name": row["user_name"],
        "session_id": row["session_id"],
        "before_json": row["before_json"],
        "after_json": row["after_json"],
    }


def sync_once(db_path: str, config: SheetsSyncConfig) -> int:
    if not config.enabled:
        return 0
    ensure_sync_schema(db_path)
    ids: list[int] = []
    events: list[dict[str, Any]] = []
    attempt_at = _utc_now()

    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = _event_rows(conn, config.batch_size, config.max_attempts)
        if not rows:
            conn.execute(
                "UPDATE sheets_sync_state SET enabled=1,last_attempt_at=?,last_error=NULL WHERE id=1",
                (attempt_at,),
            )
            conn.commit()
            return 0
        ids = [int(r["id"]) for r in rows]
        events = [_serialize_event(r) for r in rows]
        marks = ",".join("?" for _ in ids)
        conn.execute(
            f"UPDATE audit_events SET sync_status='SYNCING', sync_attempts=sync_attempts+1, sync_error=NULL WHERE id IN ({marks})",
            ids,
        )
        conn.execute(
            "UPDATE sheets_sync_state SET enabled=1,last_attempt_at=?,last_batch_count=? WHERE id=1",
            (attempt_at, len(ids)),
        )
        conn.commit()

    try:
        response = requests.post(
            config.webhook_url,
            json={"action": "append_events", "events": events},
            timeout=config.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(str(payload.get("error") or "Respuesta inválida de Apps Script"))
        accepted = set(payload.get("accepted_event_uuids") or [e["event_uuid"] for e in events])
        success_ids = [e["local_id"] for e in events if e["event_uuid"] in accepted]
        rejected_ids = [e["local_id"] for e in events if e["event_uuid"] not in accepted]

        with connect(db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if success_ids:
                marks = ",".join("?" for _ in success_ids)
                conn.execute(
                    f"UPDATE audit_events SET sync_status='SYNCED', sync_error=NULL WHERE id IN ({marks})",
                    success_ids,
                )
            if rejected_ids:
                marks = ",".join("?" for _ in rejected_ids)
                conn.execute(
                    f"UPDATE audit_events SET sync_status='ERROR', sync_error='Servidor no confirmó UUID' WHERE id IN ({marks})",
                    rejected_ids,
                )
            conn.execute(
                """UPDATE sheets_sync_state
                      SET last_success_at=?,last_error=NULL,
                          total_synced=total_synced+?,last_batch_count=?
                    WHERE id=1""",
                (_utc_now(), len(success_ids), len(ids)),
            )
            conn.commit()
        return len(success_ids)
    except Exception as exc:
        LOGGER.exception("Falló sincronización con Google Sheets")
        error = f"{type(exc).__name__}: {exc}"[:1000]
        with connect(db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if ids:
                marks = ",".join("?" for _ in ids)
                conn.execute(
                    f"UPDATE audit_events SET sync_status='ERROR', sync_error=? WHERE id IN ({marks})",
                    [error, *ids],
                )
            conn.execute(
                "UPDATE sheets_sync_state SET last_error=?,last_batch_count=? WHERE id=1",
                (error, len(ids)),
            )
            conn.commit()
        return 0


def _worker(db_path: str, config: SheetsSyncConfig, stop_event: threading.Event) -> None:
    LOGGER.info("Worker de Google Sheets iniciado")
    while not stop_event.is_set():
        synced = sync_once(db_path, config)
        # Si había backlog, procesa el siguiente lote pronto; si no, respeta intervalo.
        wait_seconds = 1 if synced >= config.batch_size else config.interval_seconds
        stop_event.wait(wait_seconds)


def start_sheets_sync(db_path: str, overrides: dict[str, Any] | None = None) -> bool:
    config = config_from_env(overrides)
    ensure_sync_schema(db_path)
    with connect(db_path) as conn:
        conn.execute("UPDATE sheets_sync_state SET enabled=? WHERE id=1", (1 if config.enabled else 0,))
        conn.commit()
    if not config.enabled:
        return False

    key = os.path.abspath(db_path)
    with _LOCK:
        thread = _THREADS.get(key)
        if thread and thread.is_alive():
            return True
        stop_event = threading.Event()
        _STOP_EVENTS[key] = stop_event
        thread = threading.Thread(
            target=_worker,
            args=(db_path, config, stop_event),
            daemon=True,
            name="aurora-sheets-sync",
        )
        _THREADS[key] = thread
        with connect(db_path) as conn:
            conn.execute(
                "UPDATE sheets_sync_state SET worker_started_at=?,last_error=NULL WHERE id=1",
                (_utc_now(),),
            )
            conn.commit()
        thread.start()
        return True


def get_sync_status(db_path: str) -> dict[str, Any]:
    ensure_sync_schema(db_path)
    with connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        state = conn.execute("SELECT * FROM sheets_sync_state WHERE id=1").fetchone()
        counts = conn.execute(
            """SELECT
                 SUM(CASE WHEN sync_status='SYNCED' THEN 1 ELSE 0 END) synced,
                 SUM(CASE WHEN sync_status IN ('PENDING','ERROR','SYNCING') THEN 1 ELSE 0 END) pending,
                 SUM(CASE WHEN sync_status='ERROR' THEN 1 ELSE 0 END) errors
               FROM audit_events"""
        ).fetchone()
        result = dict(state) if state else {}
        result.update({"synced": counts[0] or 0, "pending": counts[1] or 0, "errors": counts[2] or 0})
        return result
