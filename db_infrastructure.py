from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

LOGGER = logging.getLogger("aurora_wms.db")
_LOCK = threading.RLock()
_INITIALIZED_DATABASES: set[str] = set()
_BACKUP_THREADS: dict[str, threading.Thread] = {}

OPERATIONAL_TABLES = [
    "orders", "order_items", "pickers", "picking_ots", "picking_tasks",
    "picking_incidences", "cortes_tasks", "ot_orders", "sku_barcodes",
    "sku_publications", "s2_manifests", "s2_files", "s2_page_assign",
    "s2_mesa_status", "s2_sales", "s2_items", "s2_labels", "s2_pack_ship",
]


def configure_logging(base_dir: str | Path) -> Path:
    log_dir = Path(os.getenv("AURORA_LOG_DIR", str(Path(base_dir) / "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "aurora_wms.log"
    root = logging.getLogger()
    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(log_file) for h in root.handlers):
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        root.addHandler(handler)
        root.setLevel(logging.INFO)
    return log_file


def connect(db_path: str, *, timeout: float = 30.0) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


@contextmanager
def transaction(db_path: str):
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE;")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        LOGGER.exception("Transacción SQLite revertida")
        raise
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if column not in _columns(conn, table):
        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {ddl}')


def _migration_1(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS audit_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_uuid TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        module TEXT NOT NULL,
        action TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT,
        user_name TEXT,
        session_id TEXT,
        before_json TEXT,
        after_json TEXT,
        sync_status TEXT NOT NULL DEFAULT 'PENDING',
        sync_attempts INTEGER NOT NULL DEFAULT 0,
        sync_error TEXT
    );
    CREATE TABLE IF NOT EXISTS integrity_issues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detected_at TEXT NOT NULL,
        issue_type TEXT NOT NULL,
        table_name TEXT NOT NULL,
        entity_id TEXT,
        details TEXT,
        status TEXT NOT NULL DEFAULT 'OPEN'
    );
    CREATE TABLE IF NOT EXISTS backup_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        bytes INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        remote_status TEXT NOT NULL DEFAULT 'LOCAL',
        remote_id TEXT,
        error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_events(created_at);
    CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_events(entity_type, entity_id);
    CREATE INDEX IF NOT EXISTS idx_audit_sync ON audit_events(sync_status, id);
    CREATE INDEX IF NOT EXISTS idx_backup_created ON backup_history(created_at);
    CREATE INDEX IF NOT EXISTS idx_integrity_open ON integrity_issues(status, detected_at);
    """)


def _migration_2(conn: sqlite3.Connection) -> None:
    # Índices no destructivos para las consultas operativas más frecuentes.
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_picking_ots_picker_status ON picking_ots(picker_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_picking_tasks_ot_status ON picking_tasks(ot_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_picking_tasks_sku ON picking_tasks(sku_ml)",
        "CREATE INDEX IF NOT EXISTS idx_picking_incidences_ot ON picking_incidences(ot_id)",
        "CREATE INDEX IF NOT EXISTS idx_cortes_tasks_ot ON cortes_tasks(ot_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_ot_orders_pair ON ot_orders(ot_id, order_id)",
        "CREATE INDEX IF NOT EXISTS idx_ot_orders_order ON ot_orders(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_s2_sales_manifest_status ON s2_sales(manifest_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_s2_sales_shipment ON s2_sales(shipment_id)",
        "CREATE INDEX IF NOT EXISTS idx_s2_items_manifest_sale ON s2_items(manifest_id, sale_id)",
        "CREATE INDEX IF NOT EXISTS idx_s2_items_sku ON s2_items(sku)",
        "CREATE INDEX IF NOT EXISTS idx_s2_page_assign_mesa ON s2_page_assign(manifest_id, mesa)",
        "CREATE INDEX IF NOT EXISTS idx_s2_pack_ship_shipment ON s2_pack_ship(manifest_id, shipment_id)",
    ]
    for sql in statements:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as exc:
            # La tabla Sorting puede crearse más tarde; se reintentará en cada arranque.
            if "no such table" not in str(exc).lower():
                raise
        except sqlite3.IntegrityError:
            # No bloquea producción por duplicados históricos; queda visible en integridad.
            LOGGER.warning("No se pudo crear índice único por datos duplicados: %s", sql)


def _migration_3(conn: sqlite3.Connection) -> None:
    # Columnas de trazabilidad sin cambiar las pantallas existentes.
    for table in ("picking_ots", "picking_tasks", "picking_incidences", "s2_sales", "s2_items"):
        if _table_exists(conn, table):
            _ensure_column(conn, table, "updated_at", "TEXT")


def _json_expr(prefix: str, columns: list[str]) -> str:
    usable = [c for c in columns if c not in {"control_pdf", "labels_txt", "raw"}]
    if not usable:
        return "NULL"
    args = []
    for col in usable:
        safe = col.replace('"', '""')
        args.extend([f"'{safe}'", f'{prefix}."{safe}"'])
    return "json_object(" + ",".join(args) + ")"


def install_audit_triggers(conn: sqlite3.Connection, tables: Iterable[str] = OPERATIONAL_TABLES) -> None:
    for table in tables:
        if not _table_exists(conn, table) or table in {"audit_events", "backup_history", "schema_migrations"}:
            continue
        info = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        columns = [r[1] for r in info]
        pk_cols = [r[1] for r in info if r[5]]
        entity_new = " || ':' || ".join([f"COALESCE(CAST(NEW.\"{c}\" AS TEXT),'')" for c in pk_cols]) if pk_cols else "CAST(NEW.rowid AS TEXT)"
        entity_old = " || ':' || ".join([f"COALESCE(CAST(OLD.\"{c}\" AS TEXT),'')" for c in pk_cols]) if pk_cols else "CAST(OLD.rowid AS TEXT)"
        new_json = _json_expr("NEW", columns)
        old_json = _json_expr("OLD", columns)
        conn.executescript(f"""
        CREATE TRIGGER IF NOT EXISTS "audit_{table}_ai" AFTER INSERT ON "{table}" BEGIN
          INSERT INTO audit_events(event_uuid,created_at,module,action,entity_type,entity_id,after_json)
          VALUES(lower(hex(randomblob(16))),strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                 CASE WHEN '{table}' LIKE 's2_%' THEN 'SORTING' ELSE 'PICKING' END,
                 'INSERT','{table}',{entity_new},{new_json});
        END;
        CREATE TRIGGER IF NOT EXISTS "audit_{table}_au" AFTER UPDATE ON "{table}" BEGIN
          INSERT INTO audit_events(event_uuid,created_at,module,action,entity_type,entity_id,before_json,after_json)
          VALUES(lower(hex(randomblob(16))),strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                 CASE WHEN '{table}' LIKE 's2_%' THEN 'SORTING' ELSE 'PICKING' END,
                 'UPDATE','{table}',{entity_new},{old_json},{new_json});
        END;
        CREATE TRIGGER IF NOT EXISTS "audit_{table}_ad" AFTER DELETE ON "{table}" BEGIN
          INSERT INTO audit_events(event_uuid,created_at,module,action,entity_type,entity_id,before_json)
          VALUES(lower(hex(randomblob(16))),strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                 CASE WHEN '{table}' LIKE 's2_%' THEN 'SORTING' ELSE 'PICKING' END,
                 'DELETE','{table}',{entity_old},{old_json});
        END;
        """)


def run_integrity_checks(conn: sqlite3.Connection) -> int:
    """Registra inconsistencias existentes sin borrar ni corregir datos automáticamente."""
    if not _table_exists(conn, "integrity_issues"):
        return 0
    conn.execute("UPDATE integrity_issues SET status='RESOLVED' WHERE status='OPEN'")
    checks = [
        ("ORPHAN", "order_items", "id", "SELECT oi.id, 'order_id='||COALESCE(oi.order_id,'') FROM order_items oi LEFT JOIN orders o ON o.id=oi.order_id WHERE o.id IS NULL"),
        ("ORPHAN", "picking_tasks", "id", "SELECT pt.id, 'ot_id='||COALESCE(pt.ot_id,'') FROM picking_tasks pt LEFT JOIN picking_ots po ON po.id=pt.ot_id WHERE po.id IS NULL"),
        ("ORPHAN", "ot_orders", "id", "SELECT x.id, 'ot_id='||COALESCE(x.ot_id,'')||', order_id='||COALESCE(x.order_id,'') FROM ot_orders x LEFT JOIN picking_ots po ON po.id=x.ot_id LEFT JOIN orders o ON o.id=x.order_id WHERE po.id IS NULL OR o.id IS NULL"),
        ("DUPLICATE", "ot_orders", "ot_id", "SELECT CAST(ot_id AS TEXT), 'order_id='||COALESCE(order_id,'')||', count='||COUNT(*) FROM ot_orders GROUP BY ot_id,order_id HAVING COUNT(*)>1"),
        ("INVALID_QTY", "picking_tasks", "id", "SELECT id, 'qty_total='||COALESCE(qty_total,'')||', qty_picked='||COALESCE(qty_picked,'') FROM picking_tasks WHERE COALESCE(qty_total,0)<0 OR COALESCE(qty_picked,0)<0 OR COALESCE(qty_picked,0)>COALESCE(qty_total,0)"),
        ("INVALID_QTY", "s2_items", "rowid", "SELECT CAST(rowid AS TEXT), 'qty='||COALESCE(qty,'')||', picked='||COALESCE(picked,'') FROM s2_items WHERE COALESCE(qty,0)<0 OR COALESCE(picked,0)<0 OR COALESCE(picked,0)>COALESCE(qty,0)"),
    ]
    count = 0
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for kind, table, _, sql in checks:
        if not _table_exists(conn, table):
            continue
        try:
            for entity_id, details in conn.execute(sql).fetchall():
                conn.execute("INSERT INTO integrity_issues(detected_at,issue_type,table_name,entity_id,details) VALUES(?,?,?,?,?)",
                             (now, kind, table, str(entity_id), str(details)))
                count += 1
        except sqlite3.OperationalError:
            LOGGER.exception("No se pudo ejecutar control de integridad para %s", table)
    return count


MIGRATIONS = [
    (1, "infraestructura_auditoria_respaldo", _migration_1),
    (2, "indices_operativos", _migration_2),
    (3, "columnas_trazabilidad", _migration_3),
]


def apply_migrations(db_path: str, *, force: bool = False) -> list[int]:
    """Aplica el esquema una sola vez por proceso.

    ``force=True`` se reserva para tareas administrativas o para tablas creadas
    dinámicamente después del arranque (Sorting). Nunca ejecuta integridad.
    """
    db_key = str(Path(db_path).resolve())
    with _LOCK:
        if not force and db_key in _INITIALIZED_DATABASES:
            return []
        applied: list[int] = []
        with transaction(db_path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)")
            done = {r[0] for r in conn.execute("SELECT version FROM schema_migrations").fetchall()}
            for version, name, fn in MIGRATIONS:
                if version not in done:
                    fn(conn)
                    conn.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES(?,?,?)",
                                 (version, name, datetime.now(timezone.utc).isoformat(timespec="seconds")))
                    applied.append(version)
            # IF NOT EXISTS: barato, idempotente y sin eliminar objetos activos.
            _migration_2(conn)
            install_audit_triggers(conn)
        _INITIALIZED_DATABASES.add(db_key)
    if applied:
        LOGGER.info("Migraciones aplicadas: %s", applied)
    return applied


def ensure_runtime_objects(db_path: str) -> None:
    """Instala índices/triggers faltantes tras crear tablas dinámicas.

    Se usa inmediatamente después de crear las tablas de Sorting, no en cada
    interacción general de Streamlit.
    """
    with _LOCK, transaction(db_path) as conn:
        _migration_2(conn)
        install_audit_triggers(conn)


def prune_audit_events(db_path: str, *, keep_days: int = 90, keep_minimum: int = 50000) -> int:
    """Elimina auditoría antigua conservando siempre los eventos más recientes."""
    keep_days = max(7, int(keep_days))
    keep_minimum = max(1000, int(keep_minimum))
    with transaction(db_path) as conn:
        if not _table_exists(conn, "audit_events"):
            return 0
        cursor = conn.execute(
            """DELETE FROM audit_events
               WHERE created_at < datetime('now', ?)
                 AND id < COALESCE((SELECT MAX(id)-? FROM audit_events), 0)""",
            (f"-{keep_days} days", keep_minimum),
        )
        return max(0, cursor.rowcount)


def create_backup(db_path: str, backup_dir: str | Path, *, keep: int = 30) -> Path:
    source = Path(db_path)
    if not source.exists():
        raise FileNotFoundError(source)
    target_dir = Path(backup_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = target_dir / f"aurora_ml_{stamp}.db"
    fd, tmp_name = tempfile.mkstemp(prefix="aurora_backup_", suffix=".db", dir=str(target_dir))
    os.close(fd)
    try:
        src = connect(str(source))
        dst = sqlite3.connect(tmp_name)
        try:
            src.backup(dst)
        finally:
            dst.close(); src.close()
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    with transaction(str(source)) as conn:
        conn.execute("INSERT INTO backup_history(created_at,file_name,file_path,bytes,sha256) VALUES(?,?,?,?,?)",
                     (datetime.now(timezone.utc).isoformat(timespec="seconds"), target.name, str(target), target.stat().st_size, digest))
    backups = sorted(target_dir.glob("aurora_ml_*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in backups[max(1, keep):]:
        try: old.unlink()
        except OSError: LOGGER.exception("No se pudo eliminar respaldo antiguo %s", old)
    return target


def maybe_create_backup(db_path: str, backup_dir: str | Path, *, interval_minutes: int = 60, keep: int = 30) -> Path | None:
    marker = Path(backup_dir) / ".last_backup"
    now = time.time()
    try:
        last = float(marker.read_text().strip()) if marker.exists() else 0.0
    except Exception:
        last = 0.0
    if now - last < max(5, interval_minutes) * 60:
        return None
    with _LOCK:
        try:
            last = float(marker.read_text().strip()) if marker.exists() else 0.0
        except Exception:
            last = 0.0
        if now - last < max(5, interval_minutes) * 60:
            return None
        backup = create_backup(db_path, backup_dir, keep=keep)
        upload_backup_to_google_drive(db_path, backup)
        marker.write_text(str(now), encoding="utf-8")
        LOGGER.info("Respaldo automático creado: %s", backup)
        return backup


def schedule_backup_async(db_path: str, backup_dir: str | Path, *, interval_minutes: int = 60, keep: int = 30) -> bool:
    """Programa un respaldo en segundo plano sin bloquear la interfaz.

    Devuelve True solo cuando se inició un nuevo hilo. Un marcador reclamado
    antes del hilo evita respaldos duplicados entre reruns del mismo proceso.
    """
    db_key = str(Path(db_path).resolve())
    marker = Path(backup_dir) / ".last_backup"
    now = time.time()
    interval = max(5, int(interval_minutes)) * 60
    try:
        last = float(marker.read_text().strip()) if marker.exists() else 0.0
    except Exception:
        last = 0.0
    if now - last < interval:
        return False

    with _LOCK:
        running = _BACKUP_THREADS.get(db_key)
        if running is not None and running.is_alive():
            return False
        try:
            last = float(marker.read_text().strip()) if marker.exists() else 0.0
        except Exception:
            last = 0.0
        if now - last < interval:
            return False
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        # Reclamar la ventana antes de iniciar el hilo evita duplicados por rerun.
        marker.write_text(str(now), encoding="utf-8")

        def worker() -> None:
            try:
                backup = create_backup(db_path, backup_dir, keep=keep)
                upload_backup_to_google_drive(db_path, backup)
                LOGGER.info("Respaldo automático en segundo plano creado: %s", backup)
            except Exception:
                LOGGER.exception("Falló el respaldo automático en segundo plano")
                # Permite reintentar pronto si la creación local falló.
                try:
                    marker.unlink(missing_ok=True)
                except OSError:
                    pass
            finally:
                with _LOCK:
                    _BACKUP_THREADS.pop(db_key, None)

        thread = threading.Thread(target=worker, name="aurora-backup", daemon=True)
        _BACKUP_THREADS[db_key] = thread
        thread.start()
        return True


def upload_backup_to_google_drive(db_path: str, backup_path: str | Path) -> str | None:
    """Sube un respaldo a Drive cuando existen las variables de configuración.

    Requiere AURORA_GDRIVE_FOLDER_ID y GOOGLE_SERVICE_ACCOUNT_JSON.
    Si no están configuradas, no intenta subir y mantiene el respaldo local.
    """
    folder_id = os.getenv("AURORA_GDRIVE_FOLDER_ID", "").strip()
    raw_credentials = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not folder_id or not raw_credentials:
        return None
    path = Path(backup_path)
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        info = json.loads(raw_credentials)
        creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/drive.file"])
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        media = MediaFileUpload(str(path), mimetype="application/x-sqlite3", resumable=True)
        result = service.files().create(
            body={"name": path.name, "parents": [folder_id]}, media_body=media, fields="id"
        ).execute()
        remote_id = str(result.get("id", ""))
        with transaction(db_path) as conn:
            conn.execute("UPDATE backup_history SET remote_status='UPLOADED', remote_id=?, error=NULL WHERE file_name=?",
                         (remote_id, path.name))
        LOGGER.info("Respaldo subido a Google Drive: %s", remote_id)
        return remote_id
    except Exception as exc:
        LOGGER.exception("No se pudo subir respaldo a Google Drive")
        try:
            with transaction(db_path) as conn:
                conn.execute("UPDATE backup_history SET remote_status='ERROR', error=? WHERE file_name=?",
                             (str(exc)[:1000], path.name))
        except Exception:
            LOGGER.exception("No se pudo registrar el error de respaldo remoto")
        return None
