
import io
import os
import re
import json
import sqlite3
import hashlib
import shutil
from datetime import date, timedelta, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st


st.set_page_config(page_title="Centro de Control Comercial Aurora", layout="wide")


# =========================================================
# Helpers
# =========================================================
DB_PATH = "aurora_control_history.sqlite3"
BASE_DATA_DIR = Path("data")
ACTIVE_FILES_DIR = BASE_DATA_DIR / "activos"
BACKUP_FILES_DIR = BASE_DATA_DIR / "respaldo_archivos"

ADS_TARGET_MARGIN_PCT = 20.0
ADS_GLOBAL_ACOS_ALERT_PCT = 5.09
ADS_GLOBAL_ROAS_MIN = 10.0
ADS_OPPORTUNITY_EXTRA_MARGIN_PCT = 5.0

FILE_SPECS = {
    "master": {"label": "Maestra de precios", "filename": "maestra.xlsx", "required": True},
    "ventas": {"label": "Reporte de ventas", "filename": "ventas.xlsx", "required": True},
    "compras": {"label": "Reporte de compras", "filename": "compras.xlsx", "required": False},
    "pubs": {"label": "Maestro publicaciones ML", "filename": "publicaciones_ml.xlsx", "required": True},
    "ads": {"label": "Product Ads", "filename": "product_ads.xlsx", "required": False},
    "keywords": {"label": "Keywords / Brand Ads", "filename": "keywords.xlsx", "required": False},
}


class StoredUploadedFile:
    def __init__(self, path: Path, data: bytes, original_name: str | None = None):
        self.path = Path(path)
        self._data = data
        self.name = original_name or self.path.name
        self.size = len(data)

    def getvalue(self):
        return self._data


def ensure_storage_dirs():
    ACTIVE_FILES_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_FILES_DIR.mkdir(parents=True, exist_ok=True)


def active_file_path(file_key: str) -> Path:
    return ACTIVE_FILES_DIR / FILE_SPECS[file_key]["filename"]


def backup_file_path(file_key: str) -> Path:
    active = Path(FILE_SPECS[file_key]["filename"])
    return BACKUP_FILES_DIR / f"{active.stem}_prev{active.suffix}"


def load_active_file(file_key: str):
    path = active_file_path(file_key)
    if not path.exists():
        return None
    data = path.read_bytes()
    return StoredUploadedFile(path=path, data=data, original_name=path.name)


def archive_existing_active_file(file_key: str):
    active_path = active_file_path(file_key)
    if not active_path.exists():
        return None
    backup_path = backup_file_path(file_key)
    shutil.copy2(active_path, backup_path)
    return backup_path


def ensure_source_files_table():
    ensure_history_db()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            file_key TEXT NOT NULL,
            active_filename TEXT NOT NULL,
            archived_filename TEXT,
            original_filename TEXT,
            file_sig TEXT,
            file_size INTEGER
        )
    """)
    conn.commit()
    conn.close()


def log_source_file_event(file_key: str, active_filename: str, archived_filename=None, original_filename=None, file_sig: str = "", file_size: int = 0):
    ensure_source_files_table()
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO source_files (created_at, file_key, active_filename, archived_filename, original_filename, file_sig, file_size)
        VALUES (datetime('now'), ?, ?, ?, ?, ?, ?)
        """,
        (file_key, active_filename, archived_filename or "", original_filename or active_filename, file_sig, int(file_size or 0)),
    )
    conn.commit()
    conn.close()


def list_source_file_events():
    ensure_source_files_table()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query("SELECT * FROM source_files ORDER BY id DESC", conn)
    finally:
        conn.close()


def ensure_app_meta_table():
    ensure_history_db()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def get_app_meta(key: str, default: str = "") -> str:
    ensure_app_meta_table()
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default
    finally:
        conn.close()


def set_app_meta(key: str, value: str):
    ensure_app_meta_table()
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO app_meta (key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
        """,
        (key, value),
    )
    conn.commit()
    conn.close()


def bump_shared_version(reason: str = ""):
    current = int(get_app_meta("shared_version", "0") or "0")
    new_version = current + 1
    set_app_meta("shared_version", str(new_version))
    set_app_meta("shared_reason", reason or "actualización")
    set_app_meta("shared_updated_at", datetime.now().isoformat(timespec="seconds"))
    return new_version


def get_shared_version() -> int:
    return int(get_app_meta("shared_version", "0") or "0")


def get_shared_status():
    return {
        "version": get_shared_version(),
        "reason": get_app_meta("shared_reason", "inicio"),
        "updated_at": get_app_meta("shared_updated_at", ""),
        "last_snapshot_sig": get_app_meta("last_snapshot_sig", ""),
    }


def persist_uploaded_file(file_key: str, uploaded_file):
    ensure_storage_dirs()
    data = uploaded_file.getvalue()
    active_path = active_file_path(file_key)
    archived_path = archive_existing_active_file(file_key)
    active_path.write_bytes(data)
    stored = StoredUploadedFile(path=active_path, data=data, original_name=getattr(uploaded_file, "name", active_path.name))
    log_source_file_event(
        file_key=file_key,
        active_filename=active_path.name,
        archived_filename=archived_path.name if archived_path else "",
        original_filename=getattr(uploaded_file, "name", active_path.name),
        file_sig=file_signature(stored),
        file_size=len(data),
    )
    bump_shared_version(f"archivo {FILE_SPECS[file_key]['label']} actualizado")
    return stored



def staged_uploaded_file(file_key: str, uploaded_file):
    data = uploaded_file.getvalue()
    return StoredUploadedFile(
        path=active_file_path(file_key),
        data=data,
        original_name=getattr(uploaded_file, "name", active_file_path(file_key).name),
    )


def validate_uploaded_file(file_key: str, stored_file: StoredUploadedFile):
    try:
        data = stored_file.getvalue()
        if not data:
            return False, "Archivo vacío."
        if file_key == "master":
            wb = load_master_workbook(data)
            master_df, _ = normalize_master(wb["master_df"], wb["bridge_df"])
            if master_df.empty:
                return False, "La maestra quedó vacía tras normalizar."
        elif file_key == "ventas":
            df = load_sales(data)
            if df.empty:
                return False, "El reporte de ventas no trae filas válidas."
        elif file_key == "compras":
            df = load_purchases(data)
            if df.empty:
                return False, "El reporte de compras no trae filas válidas."
        elif file_key == "pubs":
            df = load_publications(data)
            if df.empty:
                return False, "El reporte de publicaciones no trae filas válidas."
        elif file_key == "ads":
            df = load_product_ads(data)
            if df.empty:
                return False, "El reporte Product Ads no trae filas válidas."
        elif file_key == "keywords":
            df = load_keywords(data)
            if df.empty:
                return False, "El reporte Keywords no trae filas válidas."
        return True, "OK"
    except Exception as e:
        return False, str(e)


def apply_uploaded_updates(uploaders: dict):
    staged = {}
    errors = []
    updated_labels = []
    for file_key, uploaded in uploaders.items():
        if uploaded is None:
            continue
        staged_file = staged_uploaded_file(file_key, uploaded)
        ok, msg = validate_uploaded_file(file_key, staged_file)
        if not ok:
            errors.append(f"{FILE_SPECS[file_key]['label']}: {msg}")
        else:
            staged[file_key] = staged_file
    if errors:
        return False, errors, []
    for file_key, staged_file in staged.items():
        persist_uploaded_file(file_key, staged_file)
        updated_labels.append(FILE_SPECS[file_key]["label"])
    build_model_cached.clear()
    load_master_workbook.clear()
    load_sales.clear()
    load_purchases.clear()
    load_publications.clear()
    load_product_ads.clear()
    load_keywords.clear()
    return True, [], updated_labels



def resolve_input_file(file_key: str, uploaded_file=None):
    if uploaded_file is not None:
        return staged_uploaded_file(file_key, uploaded_file), "pendiente"
    active = load_active_file(file_key)
    if active is not None:
        return active, "activo"
    return None, "faltante"


def storage_status_df():
    rows = []
    for file_key, spec in FILE_SPECS.items():
        active = load_active_file(file_key)
        backup = backup_file_path(file_key)
        rows.append({
            "Archivo": spec["label"],
            "Estado": "Activo" if active is not None else "Faltante",
            "Nombre": active.path.name if active is not None else "—",
            "Última versión": datetime.fromtimestamp(active.path.stat().st_mtime).strftime("%d/%m/%Y %H:%M") if active is not None else "—",
            "Respaldo": backup.name if backup.exists() else "—",
        })
    return pd.DataFrame(rows)



def file_signature(uploaded_file) -> str:
    data = uploaded_file.getvalue()
    return hashlib.md5(data).hexdigest()


def payload_signature(df: pd.DataFrame, extra: str = "") -> str:
    base = df.copy()
    for col in base.columns:
        if pd.api.types.is_datetime64_any_dtype(base[col]):
            base[col] = base[col].astype("string")
    base = base.replace([np.inf, -np.inf], np.nan).fillna("")
    csv_bytes = base.sort_values(list(base.columns[:1])).to_csv(index=False).encode("utf-8")
    return hashlib.md5(csv_bytes + extra.encode("utf-8")).hexdigest()


def safe_float(value, default=np.nan):
    try:
        if value is None:
            return default
        if isinstance(value, str):
            s = value.strip().replace("$", "").replace(".", "").replace(",", ".")
            if s in ("", "-", "nan", "None"):
                return default
            return float(s)
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def norm_sku(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    if re.fullmatch(r"-?\d+(\.\d+)?", s):
        try:
            f = float(s.replace(",", "."))
            if int(f) == f:
                return str(int(f))
        except Exception:
            pass
    return s


def norm_mlc(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    s = str(value).strip().upper().replace(" ", "")
    if not s or s == "NAN":
        return ""
    if s.isdigit():
        s = f"MLC{s}"
    return s


def to_date_only(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return pd.NaT
    try:
        return pd.to_datetime(value, errors="coerce", dayfirst=True).normalize()
    except Exception:
        return pd.NaT


def fmt_date(value) -> str:
    dt = to_date_only(value)
    if pd.isna(dt):
        return "—"
    return dt.strftime("%d/%m/%Y")


def fmt_money(value) -> str:
    x = safe_float(value, np.nan)
    if np.isnan(x):
        return "—"
    return f"${x:,.0f}".replace(",", ".")


def fmt_int(value) -> str:
    x = safe_float(value, np.nan)
    if np.isnan(x):
        return "—"
    return f"{int(round(x)):,}".replace(",", ".")


def fmt_pct(value, decimals=1) -> str:
    x = safe_float(value, np.nan)
    if np.isnan(x):
        return "—"
    return f"{x:.{decimals}f}%"


def _find_sheet(sheet_names, wanted):
    for name in sheet_names:
        if name.lower().strip() == wanted.lower().strip():
            return name
    for name in sheet_names:
        if wanted.lower().strip() in name.lower().strip():
            return name
    return None


def _canon_label(value: str) -> str:
    s = str(value or "").strip().upper()
    replacements = {
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
        "(": "", ")": "", ".": "", ",": "", ":": "", ";": "", "-": " ", "_": " "
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def pick_existing_column(df: pd.DataFrame, *candidates: str):
    canon_map = {_canon_label(col): col for col in df.columns}
    for cand in candidates:
        col = canon_map.get(_canon_label(cand))
        if col is not None:
            return col
    return None


def detect_channel(vendedor: str) -> str:
    s = str(vendedor).strip().upper()
    return "ML" if "MERCADO LIBRE" in s else "TIENDA"


def detect_buyer_type(documento: str) -> str:
    s = str(documento).strip().upper()
    if "FACTURA" in s:
        return "EMPRESA"
    if "BOLETA" in s:
        return "PERSONA"
    return "OTRO"


def classify_cost_gap_pct(pct):
    if pd.isna(pct):
        return "SIN DATOS"
    if pct >= 5:
        return "CRÍTICO"
    if pct >= 2:
        return "ALERTA"
    if pct <= -2:
        return "BAJÓ COSTO"
    return "OK"


def classify_margin_delta_pp(delta_pp):
    if pd.isna(delta_pp):
        return "SIN HISTÓRICO"
    if delta_pp <= -5:
        return "CRÍTICO"
    if delta_pp <= -2:
        return "ALERTA"
    if delta_pp >= 2:
        return "MEJORA"
    return "ESTABLE"


def classify_ads_reason(row, target_margin_pct: float = ADS_TARGET_MARGIN_PCT, global_acos_alert_pct: float = ADS_GLOBAL_ACOS_ALERT_PCT, min_roas: float = ADS_GLOBAL_ROAS_MIN):
    ads_inversion = safe_float(row.get("ads_inversion"), 0.0)
    ads_ingresos = safe_float(row.get("ads_ingresos"), 0.0)
    ads_clicks = safe_float(row.get("ads_clics"), 0.0)
    ads_ventas = safe_float(row.get("ads_ventas"), 0.0)
    ads_active = (ads_inversion > 0) or (ads_ingresos > 0) or (ads_clicks > 0)

    margen_base = safe_float(row.get("margen_ads_base_pct", row.get("margen_ml_actual")), np.nan)
    margen_con_ads = safe_float(row.get("margen_ml_con_ads"), np.nan)
    acos_real = safe_float(row.get("ads_acos"), np.nan)
    acos_max = safe_float(row.get("acos_max_permitido_pct"), np.nan)
    roas_real = safe_float(row.get("ads_roas"), np.nan)

    if pd.notna(margen_base) and margen_base < target_margin_pct:
        return "Margen base bajo objetivo"
    if not ads_active:
        if pd.notna(margen_base) and margen_base >= target_margin_pct + 10:
            return "Sin Ads y con holgura para probar"
        return "Sin Ads activos"
    if ads_inversion > 0 and ads_ingresos <= 0:
        return "Inversión sin ingresos"
    if pd.notna(margen_con_ads) and margen_con_ads < target_margin_pct:
        return "Margen con Ads bajo objetivo"
    if pd.notna(acos_real) and pd.notna(acos_max) and acos_real > acos_max:
        return "ACOS sobre máximo permitido"
    if pd.notna(roas_real) and roas_real < min_roas:
        return "ROAS bajo mínimo"
    if ads_clicks >= 15 and ads_ventas <= 0:
        return "Clicks altos sin ventas Ads"
    if pd.notna(acos_real) and acos_real > global_acos_alert_pct:
        return "ACOS sobre alerta global"
    if pd.notna(margen_con_ads) and margen_con_ads < target_margin_pct + 3:
        return "Margen con Ads muy ajustado"
    if pd.notna(roas_real) and roas_real < 12:
        return "ROAS ajustado"
    if pd.notna(acos_real) and pd.notna(margen_con_ads) and pd.notna(roas_real) and acos_real <= global_acos_alert_pct and roas_real >= 12 and margen_con_ads >= max(target_margin_pct + ADS_OPPORTUNITY_EXTRA_MARGIN_PCT, 25):
        return "Escalable con margen holgado"
    return "Bajo control"


def classify_ads_state(row, target_margin_pct: float = ADS_TARGET_MARGIN_PCT, global_acos_alert_pct: float = ADS_GLOBAL_ACOS_ALERT_PCT, min_roas: float = ADS_GLOBAL_ROAS_MIN):
    ads_inversion = safe_float(row.get("ads_inversion"), 0.0)
    ads_ingresos = safe_float(row.get("ads_ingresos"), 0.0)
    ads_clicks = safe_float(row.get("ads_clics"), 0.0)
    ads_ventas = safe_float(row.get("ads_ventas"), 0.0)
    ads_active = (ads_inversion > 0) or (ads_ingresos > 0) or (ads_clicks > 0)

    margen_base = safe_float(row.get("margen_ads_base_pct", row.get("margen_ml_actual")), np.nan)
    margen_con_ads = safe_float(row.get("margen_ml_con_ads"), np.nan)
    acos_real = safe_float(row.get("ads_acos"), np.nan)
    acos_max = safe_float(row.get("acos_max_permitido_pct"), np.nan)
    roas_real = safe_float(row.get("ads_roas"), np.nan)

    if pd.notna(margen_base) and margen_base < target_margin_pct:
        return "NO USAR ADS"
    if not ads_active:
        if pd.notna(margen_base) and margen_base >= target_margin_pct + 10:
            return "OPORTUNIDAD"
        return "SIN ADS"
    if ads_inversion > 0 and ads_ingresos <= 0:
        return "CRÍTICO"
    if pd.notna(margen_con_ads) and margen_con_ads < target_margin_pct:
        return "CRÍTICO"
    if pd.notna(acos_real) and pd.notna(acos_max) and acos_real > acos_max:
        return "CRÍTICO"
    if pd.notna(roas_real) and roas_real < min_roas:
        return "CRÍTICO"
    if ads_clicks >= 15 and ads_ventas <= 0:
        return "CRÍTICO"
    if pd.notna(acos_real) and acos_real > global_acos_alert_pct:
        return "ALERTA"
    if pd.notna(margen_con_ads) and margen_con_ads < target_margin_pct + 3:
        return "ALERTA"
    if pd.notna(roas_real) and roas_real < 12:
        return "ALERTA"
    if pd.notna(acos_real) and pd.notna(margen_con_ads) and pd.notna(roas_real) and acos_real <= global_acos_alert_pct and roas_real >= 12 and margen_con_ads >= max(target_margin_pct + ADS_OPPORTUNITY_EXTRA_MARGIN_PCT, 25):
        return "OPORTUNIDAD"
    return "OK"


def suggest_ads_action(row, target_margin_pct: float = ADS_TARGET_MARGIN_PCT):
    estado_ads = str(row.get("estado_ads", "")).upper()
    ads_inversion = safe_float(row.get("ads_inversion"), 0.0)
    ads_ingresos = safe_float(row.get("ads_ingresos"), 0.0)
    if estado_ads == "NO USAR ADS":
        return "NO INVERTIR / APAGAR"
    if estado_ads == "SIN ADS":
        return "SIN ACCIÓN ADS"
    if estado_ads == "OPORTUNIDAD":
        if ads_inversion > 0 and ads_ingresos > 0:
            return "ESCALAR PRESUPUESTO"
        return "PROBAR ADS"
    if estado_ads == "CRÍTICO":
        if ads_inversion > 0 and ads_ingresos <= 0:
            return "PAUSAR / CORTAR GASTO"
        return "BAJAR PUJA O REVISAR PRECIO"
    if estado_ads == "ALERTA":
        return "OPTIMIZAR ADS"
    return "MANTENER ADS"


def parse_dimensions(dim_str):
    out = {
        "dimensiones": "—",
        "largo_cm": np.nan,
        "ancho_cm": np.nan,
        "alto_cm": np.nan,
        "peso_grs": np.nan,
        "peso_volumetrico_kg": np.nan,
    }
    if not isinstance(dim_str, str) or not dim_str.strip():
        return out
    s = dim_str.lower().replace("cms", "cm").replace(" ", "")
    m = re.search(r"(\d+(?:[.,]\d+)?)x(\d+(?:[.,]\d+)?)x(\d+(?:[.,]\d+)?)cm", s)
    if m:
        a, b, c = [float(x.replace(",", ".")) for x in m.groups()]
        out["alto_cm"], out["ancho_cm"], out["largo_cm"] = a, b, c
        out["dimensiones"] = f"{a:g} x {b:g} x {c:g} cm"
        out["peso_volumetrico_kg"] = (a * b * c) / 4000.0
    m2 = re.search(r"(\d+(?:[.,]\d+)?)(grs|g|kg)", s)
    if m2:
        val = float(m2.group(1).replace(",", "."))
        unit = m2.group(2)
        out["peso_grs"] = val * 1000 if unit == "kg" else val
    return out


def calc_margin_from_bruto(cost, bruto):
    cost = safe_float(cost, np.nan)
    bruto = safe_float(bruto, np.nan)
    if np.isnan(cost) or np.isnan(bruto) or bruto <= 0:
        return np.nan
    neto = bruto / 1.19
    if neto <= 0:
        return np.nan
    return ((neto - cost) / neto) * 100


def calc_margin_from_monto_sim(cost, monto_sim):
    cost = safe_float(cost, np.nan)
    monto_sim = safe_float(monto_sim, np.nan)
    if np.isnan(cost) or np.isnan(monto_sim) or monto_sim <= 0:
        return np.nan
    neto = monto_sim / 1.19
    if neto <= 0:
        return np.nan
    return ((neto - cost) / neto) * 100


def choose_primary_publication(df):
    if df is None or df.empty:
        return None
    tmp = df.copy()
    tmp["status_rank"] = np.where(tmp["status"].astype(str).str.upper().eq("ACTIVA"), 0, 1)
    tmp["ventas_rank"] = pd.to_numeric(tmp["ventas_hist_pub"], errors="coerce").fillna(0)
    tmp = tmp.sort_values(["status_rank", "ventas_rank"], ascending=[True, False])
    return tmp.iloc[0]


def format_mlc_list(values) -> str:
    if not isinstance(values, list) or not values:
        return "—"
    cleaned = []
    for v in values:
        s = str(v).strip().upper().replace(" ", "")
        if not s or s == "NAN":
            continue
        s = re.sub(r"\.0$", "", s)
        if s.isdigit():
            s = f"MLC{s}"
        elif s.startswith("MLC") and s[3:].isdigit():
            s = f"MLC{s[3:]}"
        cleaned.append(s)
    cleaned = list(dict.fromkeys(cleaned))
    return ", ".join(cleaned) if cleaned else "—"


def build_ads_report_detail_for_sku(sku: str, product_ads: pd.DataFrame | None, publications: pd.DataFrame | None) -> pd.DataFrame:
    base_cols = [
        "campana", "mlc", "titulo", "estado", "inversion_ads", "ingresos_ads", "acos", "roas", "ventas_ads", "impresiones", "clics"
    ]
    if not sku or product_ads is None or publications is None:
        return pd.DataFrame(columns=base_cols)
    if not isinstance(product_ads, pd.DataFrame) or not isinstance(publications, pd.DataFrame):
        return pd.DataFrame(columns=base_cols)
    if product_ads.empty or publications.empty or "mlc" not in product_ads.columns:
        return pd.DataFrame(columns=base_cols)

    pubs_sku = publications[publications.get("sku", pd.Series(dtype=str)) == sku].copy()
    if pubs_sku.empty:
        return pd.DataFrame(columns=base_cols)

    ads = product_ads.copy()
    if "mlc" not in ads.columns:
        return pd.DataFrame(columns=base_cols)

    ads = ads[ads["mlc"].isin(pubs_sku["mlc"].dropna().astype(str).tolist())].copy()
    if ads.empty:
        return pd.DataFrame(columns=base_cols)

    for col in base_cols:
        if col not in ads.columns:
            ads[col] = np.nan

    ads = ads[base_cols].copy()
    ads = ads.sort_values(["inversion_ads", "ingresos_ads", "campana", "mlc"], ascending=[False, False, True, True], na_position="last")
    return ads


def ensure_history_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            master_sig TEXT,
            ventas_sig TEXT,
            compras_sig TEXT,
            pubs_sig TEXT,
            ads_sig TEXT,
            keywords_sig TEXT,
            notes TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshot_producto (
            run_id INTEGER,
            sku TEXT,
            descripcion TEXT,
            costo_maestra REAL,
            ultimo_costo_compra REAL,
            brecha_costo_pct REAL,
            precio_bruto REAL,
            monto_sim REAL,
            precio_ml_actual REAL,
            ingreso_estimado_ml REAL,
            brecha_precio_pct REAL,
            brecha_monto_sim_pct REAL,
            margen_ml_actual REAL,
            margen_hist_30d REAL,
            margen_hist_90d REAL,
            margen_hist_total REAL,
            delta_margen_30d_pp REAL,
            ventas_ml_30d REAL,
            ventas_tienda_30d REAL,
            ads_inversion REAL,
            ads_ingresos REAL,
            ads_acos REAL,
            PRIMARY KEY (run_id, sku)
        )
    """)
    conn.commit()
    conn.close()


def save_snapshot_to_db(payload_df, sigs):
    ensure_history_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO runs (created_at, master_sig, ventas_sig, compras_sig, pubs_sig, ads_sig, keywords_sig, notes)
        VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sigs.get("master_sig", ""),
            sigs.get("ventas_sig", ""),
            sigs.get("compras_sig", ""),
            sigs.get("pubs_sig", ""),
            sigs.get("ads_sig", ""),
            sigs.get("keywords_sig", ""),
            "snapshot automático",
        ),
    )
    run_id = cur.lastrowid
    insert_df = payload_df.copy()
    insert_df["sku"] = insert_df["sku"].map(norm_sku)
    insert_df = insert_df[insert_df["sku"] != ""].copy()
    if "ventas_ml_30d" in insert_df.columns:
        insert_df = insert_df.sort_values(["sku", "ventas_ml_30d"], ascending=[True, False])
    insert_df = insert_df.drop_duplicates(subset=["sku"], keep="first")
    insert_df = insert_df.replace([np.inf, -np.inf], np.nan)
    insert_df = insert_df.where(pd.notnull(insert_df), None)
    insert_df["run_id"] = run_id
    cols = [
        "run_id", "sku", "descripcion", "costo_maestra", "ultimo_costo_compra", "brecha_costo_pct",
        "precio_bruto", "monto_sim", "precio_ml_actual", "ingreso_estimado_ml", "brecha_precio_pct",
        "brecha_monto_sim_pct", "margen_ml_actual", "margen_hist_30d", "margen_hist_90d", "margen_hist_total",
        "delta_margen_30d_pp", "ventas_ml_30d", "ventas_tienda_30d", "ads_inversion", "ads_ingresos", "ads_acos",
    ]
    insert_df = insert_df.reindex(columns=cols)
    insert_df.to_sql("snapshot_producto", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    return run_id



def list_runs():
    ensure_history_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM runs ORDER BY id DESC", conn)
    finally:
        conn.close()
    return df



def load_snapshot_history_for_skus(skus):
    skus = [norm_sku(s) for s in (skus or []) if norm_sku(s)]
    if not skus:
        return pd.DataFrame()
    ensure_history_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        placeholders = ",".join(["?"] * len(skus))
        query = f"""
            SELECT sp.*, r.created_at
            FROM snapshot_producto sp
            JOIN runs r ON r.id = sp.run_id
            WHERE sp.sku IN ({placeholders})
            ORDER BY sp.sku, sp.run_id
        """
        return pd.read_sql_query(query, conn, params=skus)
    finally:
        conn.close()


def load_snapshot_history_for_sku(sku):
    sku = norm_sku(sku)
    if not sku:
        return pd.DataFrame()
    return load_snapshot_history_for_skus([sku])


def enrich_action_table_with_snapshot_history(action_table: pd.DataFrame) -> pd.DataFrame:
    if action_table is None or action_table.empty:
        return action_table
    hist = load_snapshot_history_for_skus(action_table["sku"].dropna().unique().tolist())
    if hist.empty:
        out = action_table.copy()
        for col in [
            "brecha_costo_inicial_pct", "brecha_costo_previa_pct", "delta_brecha_costo_vs_inicial_pp", "delta_brecha_costo_vs_previa_pp",
            "brecha_precio_inicial_pct", "brecha_precio_previa_pct", "delta_brecha_precio_vs_inicial_pp", "delta_brecha_precio_vs_previa_pp",
            "brecha_ingreso_inicial_pct", "brecha_ingreso_previa_pct", "delta_brecha_ingreso_vs_inicial_pp", "delta_brecha_ingreso_vs_previa_pp",
            "runs_count", "primera_corrida", "ultima_corrida_previa"
        ]:
            out[col] = np.nan
        return out

    rows = []
    for sku, grp in hist.groupby("sku", sort=False):
        grp = grp.sort_values("run_id")
        first = grp.iloc[0]
        prev = grp.iloc[-2] if len(grp) >= 2 else grp.iloc[-1]
        rows.append({
            "sku": sku,
            "brecha_costo_inicial_pct": first.get("brecha_costo_pct"),
            "brecha_costo_previa_pct": prev.get("brecha_costo_pct"),
            "brecha_precio_inicial_pct": first.get("brecha_precio_pct"),
            "brecha_precio_previa_pct": prev.get("brecha_precio_pct"),
            "brecha_ingreso_inicial_pct": first.get("brecha_monto_sim_pct"),
            "brecha_ingreso_previa_pct": prev.get("brecha_monto_sim_pct"),
            "runs_count": len(grp),
            "primera_corrida": first.get("created_at"),
            "ultima_corrida_previa": prev.get("created_at"),
        })
    base = pd.DataFrame(rows)
    out = action_table.merge(base, on="sku", how="left")
    out["delta_brecha_costo_vs_inicial_pp"] = out["brecha_costo_pct"] - out["brecha_costo_inicial_pct"]
    out["delta_brecha_costo_vs_previa_pp"] = out["brecha_costo_pct"] - out["brecha_costo_previa_pct"]
    out["delta_brecha_precio_vs_inicial_pp"] = out["brecha_precio_pct"] - out["brecha_precio_inicial_pct"]
    out["delta_brecha_precio_vs_previa_pp"] = out["brecha_precio_pct"] - out["brecha_precio_previa_pct"]
    out["delta_brecha_ingreso_vs_inicial_pp"] = out["brecha_monto_sim_pct"] - out["brecha_ingreso_inicial_pct"]
    out["delta_brecha_ingreso_vs_previa_pp"] = out["brecha_monto_sim_pct"] - out["brecha_ingreso_previa_pct"]
    return out


def build_validation_layers(master, ventas, compras, pubs, product_ads, promos, action_table=None):
    details = {}
    summary_rows = []

    def register(name, severity, df, detail):
        df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        details[name] = df
        summary_rows.append({
            "Capa": name,
            "Severidad": severity,
            "Hallazgos": len(df),
            "Detalle": detail,
        })

    master = master.copy() if isinstance(master, pd.DataFrame) else pd.DataFrame()
    ventas = ventas.copy() if isinstance(ventas, pd.DataFrame) else pd.DataFrame()
    compras = compras.copy() if isinstance(compras, pd.DataFrame) else pd.DataFrame()
    pubs = pubs.copy() if isinstance(pubs, pd.DataFrame) else pd.DataFrame()
    product_ads = product_ads.copy() if isinstance(product_ads, pd.DataFrame) else pd.DataFrame()
    promos = promos.copy() if isinstance(promos, pd.DataFrame) else pd.DataFrame()

    master_skus = set(master.get("sku", pd.Series(dtype=str)).dropna().astype(str))
    sales_skus = set(ventas.get("sku", pd.Series(dtype=str)).dropna().astype(str))
    purchase_skus = set(compras.get("sku", pd.Series(dtype=str)).dropna().astype(str))
    pub_skus = set(pubs.get("sku", pd.Series(dtype=str)).dropna().astype(str))
    pub_mlcs = set(pubs.get("mlc", pd.Series(dtype=str)).dropna().astype(str))

    dup_master = master[master.get("sku", pd.Series(dtype=str)).duplicated(keep=False)].sort_values("sku") if not master.empty else pd.DataFrame()
    register("Maestra duplicada por SKU", "CRÍTICO" if not dup_master.empty else "OK", dup_master[[c for c in ["sku", "descripcion", "costo_maestra", "precio_bruto", "monto_sim"] if c in dup_master.columns]], "Un SKU no debería repetirse en la maestra consolidada.")

    missing_core = master[(master.get("costo_maestra").isna()) | (master.get("precio_bruto").isna()) | (master.get("monto_sim").isna())] if not master.empty else pd.DataFrame()
    register("Maestra incompleta", "ALERTA" if not missing_core.empty else "OK", missing_core[[c for c in ["sku", "descripcion", "costo_maestra", "precio_bruto", "monto_sim"] if c in missing_core.columns]], "La maestra debería quedar alimentada por reportes con costo, precio tienda y monto simulación.")

    sales_out = ventas[~ventas.get("sku", pd.Series(dtype=str)).isin(master_skus)].sort_values(["sku", "fecha"], ascending=[True, False]) if not ventas.empty else pd.DataFrame()
    register("Ventas fuera de maestra", "CRÍTICO" if not sales_out.empty else "OK", sales_out[[c for c in ["sku", "fecha", "producto", "total_linea", "canal"] if c in sales_out.columns]].drop_duplicates(), "El reporte de ventas está trayendo SKUs que no quedaron absorbidos por la maestra.")

    purchase_out = compras[~compras.get("sku", pd.Series(dtype=str)).isin(master_skus)].sort_values(["sku", "fecha"], ascending=[True, False]) if not compras.empty else pd.DataFrame()
    register("Compras fuera de maestra", "ALERTA" if not purchase_out.empty else "OK", purchase_out[[c for c in ["sku", "fecha", "proveedor", "precio_unitario"] if c in purchase_out.columns]].drop_duplicates(), "Hay costos del reporte de compras que no están cayendo a la maestra.")

    pubs_out = pubs[~pubs.get("sku", pd.Series(dtype=str)).isin(master_skus)].sort_values("sku") if not pubs.empty else pd.DataFrame()
    register("Publicaciones fuera de maestra", "CRÍTICO" if not pubs_out.empty else "OK", pubs_out[[c for c in ["sku", "mlc", "titulo", "precio_final", "status"] if c in pubs_out.columns]].drop_duplicates(), "Mercado Libre reporta publicaciones cuyo SKU no quedó consolidado en la maestra.")

    master_without_pub = master[~master.get("sku", pd.Series(dtype=str)).isin(pub_skus)].sort_values("sku") if not master.empty else pd.DataFrame()
    register("Maestra sin publicación ML", "ALERTA" if not master_without_pub.empty else "OK", master_without_pub[[c for c in ["sku", "descripcion", "precio_bruto", "monto_sim"] if c in master_without_pub.columns]], "Hay productos consolidados sin contraparte en el reporte de publicaciones.")

    dup_mlc = pubs[pubs.get("mlc", pd.Series(dtype=str)).duplicated(keep=False)].sort_values("mlc") if not pubs.empty else pd.DataFrame()
    register("MLC duplicado en reporte", "CRÍTICO" if not dup_mlc.empty else "OK", dup_mlc[[c for c in ["mlc", "sku", "titulo", "status"] if c in dup_mlc.columns]], "Un mismo MLC no debería repetirse en el reporte base.")

    multi_active = pubs[pubs.get("status", pd.Series(dtype=str)).astype(str).str.upper().eq("ACTIVA")].copy() if not pubs.empty else pd.DataFrame()
    multi_active = multi_active.groupby("sku").filter(lambda g: len(g) > 1) if not multi_active.empty else pd.DataFrame()
    register("Múltiples publicaciones activas por SKU", "ALERTA" if not multi_active.empty else "OK", multi_active[[c for c in ["sku", "mlc", "titulo", "precio_final", "ventas_hist_pub"] if c in multi_active.columns]], "Puede existir estrategia multilistado, pero se debe revisar porque afecta la publicación principal y el pricing.")

    promo_issues = promos[(promos.get("mlc", pd.Series(dtype=str)).astype(str).str.strip().eq("")) | (pd.isna(promos.get("fecha_venci")))] if not promos.empty else pd.DataFrame()
    register("Promociones incompletas", "ALERTA" if not promo_issues.empty else "OK", promo_issues[[c for c in ["sku", "slot", "mlc", "precio_b2c", "fecha_venci", "comentario"] if c in promo_issues.columns]], "Las promos deben tener MLC y fecha de vencimiento para ser operables.")

    ads_orphan = product_ads[~product_ads.get("mlc", pd.Series(dtype=str)).isin(pub_mlcs)].sort_values("mlc") if not product_ads.empty else pd.DataFrame()
    register("Ads sin publicación asociada", "ALERTA" if not ads_orphan.empty else "OK", ads_orphan[[c for c in ["mlc", "campana", "titulo", "inversion_ads", "ingresos_ads"] if c in ads_orphan.columns]], "Product Ads trae publicaciones que hoy no están en el reporte maestro ML.")

    future_purchases = compras[compras.get("fecha", pd.Series(dtype='datetime64[ns]')) > pd.Timestamp(date.today())] if not compras.empty else pd.DataFrame()
    register("Compras con fecha futura", "ALERTA" if not future_purchases.empty else "OK", future_purchases[[c for c in ["sku", "fecha", "proveedor", "precio_unitario"] if c in future_purchases.columns]], "Hay registros de compras con fecha posterior a hoy.")

    action_table = action_table.copy() if isinstance(action_table, pd.DataFrame) else pd.DataFrame()
    if not action_table.empty:
        ads_burning = action_table[(action_table.get("ads_inversion", 0).fillna(0) > 0) & (action_table.get("ads_ingresos", 0).fillna(0) <= 0)].copy() if "ads_inversion" in action_table.columns else pd.DataFrame()
        register(
            "Ads quemando gasto sin ventas",
            "CRÍTICO" if not ads_burning.empty else "OK",
            ads_burning[[c for c in ["sku", "descripcion", "ads_inversion", "ads_ingresos", "ads_clics", "accion_ads"] if c in ads_burning.columns]],
            "Hay inversión publicitaria sin ingresos atribuidos; conviene pausar o recortar mientras se revisa ficha, precio o targeting.",
        )

        ads_negative_margin = action_table[action_table.get("estado_ads", pd.Series(dtype=str)).astype(str).eq("CRÍTICO")].copy() if "estado_ads" in action_table.columns else pd.DataFrame()
        if not ads_negative_margin.empty and "margen_ml_con_ads" in ads_negative_margin.columns:
            ads_negative_margin = ads_negative_margin[(ads_negative_margin["margen_ml_con_ads"].fillna(0) < 0) | (ads_negative_margin.get("gap_acos_pct", np.nan).fillna(-np.inf) > 0)]
        register(
            "Ads incoherente vs rentabilidad",
            "CRÍTICO" if not ads_negative_margin.empty else "OK",
            ads_negative_margin[[c for c in ["sku", "descripcion", "ads_acos", "acos_max_permitido_pct", "margen_ml_con_ads", "accion_ads"] if c in ads_negative_margin.columns]],
            "El ACOS real ya superó el ACOS máximo permitido o el margen con Ads quedó negativo.",
        )

        ads_scale = action_table[action_table.get("estado_ads", pd.Series(dtype=str)).astype(str).eq("OPORTUNIDAD")].copy() if "estado_ads" in action_table.columns else pd.DataFrame()
        register(
            "Ads escalables",
            "OK" if ads_scale.empty else "ALERTA",
            ads_scale[[c for c in ["sku", "descripcion", "ads_acos", "margen_ml_con_ads", "ads_roas", "accion_ads"] if c in ads_scale.columns]],
            "Estos SKUs tienen margen con Ads holgado y ACOS bajo; podrían absorber más presupuesto.",
        )

    summary = pd.DataFrame(summary_rows)
    severity_rank = {"CRÍTICO": 3, "ALERTA": 2, "OK": 1}
    if not summary.empty:
        summary["_rank"] = summary["Severidad"].map(severity_rank).fillna(0)
        summary = summary.sort_values(["_rank", "Hallazgos", "Capa"], ascending=[False, False, True]).drop(columns=["_rank"])
    return {"summary": summary, "details": details}


def calc_ml_net_revenue(price, fee_pct, fixed_charge=0.0, ads_pct=0.0):
    price = safe_float(price, np.nan)
    fee_pct = safe_float(fee_pct, 0.0)
    fixed_charge = safe_float(fixed_charge, 0.0)
    ads_pct = safe_float(ads_pct, 0.0)
    if np.isnan(price) or price <= 0:
        return np.nan
    variable_rate = max(0.0, min(0.95, (fee_pct + ads_pct) / 100.0))
    revenue_gross = price * (1 - variable_rate) - fixed_charge
    if revenue_gross <= 0:
        return np.nan
    return revenue_gross / 1.19


def calc_margin_from_ml_price(cost, price, fee_pct, fixed_charge=0.0, ads_pct=0.0):
    cost = safe_float(cost, np.nan)
    net_rev = calc_ml_net_revenue(price, fee_pct, fixed_charge, ads_pct)
    if np.isnan(cost) or np.isnan(net_rev) or net_rev <= 0:
        return np.nan
    return ((net_rev - cost) / net_rev) * 100


def calc_price_for_target_ml_margin(cost, fee_pct, fixed_charge=0.0, target_margin_pct=15.0, ads_pct=0.0):
    cost = safe_float(cost, np.nan)
    fee_pct = safe_float(fee_pct, 0.0)
    fixed_charge = safe_float(fixed_charge, 0.0)
    target_margin_pct = safe_float(target_margin_pct, np.nan)
    ads_pct = safe_float(ads_pct, 0.0)
    if np.isnan(cost) or np.isnan(target_margin_pct):
        return np.nan
    target = target_margin_pct / 100.0
    variable_rate = max(0.0, min(0.95, (fee_pct + ads_pct) / 100.0))
    denominator = 1 - variable_rate
    if denominator <= 0 or target >= 1:
        return np.nan
    required_net_rev = cost / (1 - target)
    return ((required_net_rev * 1.19) + fixed_charge) / denominator


# =========================================================
# Loaders
# =========================================================
@st.cache_data(show_spinner=False)
def load_master_workbook(file_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    names = xls.sheet_names
    maestra_name = _find_sheet(names, "MAESTRA de precios")
    bridge_name = _find_sheet(names, "MLC -SKU")
    rel_name = _find_sheet(names, "Relampago mi pagina")

    if not maestra_name:
        raise ValueError("No encontré la hoja 'MAESTRA de precios'.")

    master_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=maestra_name)
    bridge_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=bridge_name) if bridge_name else pd.DataFrame()
    rel_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=rel_name, header=None) if rel_name else pd.DataFrame()

    return {
        "sheet_names": names,
        "maestra_name": maestra_name,
        "bridge_name": bridge_name,
        "rel_name": rel_name,
        "master_df": master_df,
        "bridge_df": bridge_df,
        "rel_df": rel_df,
        "file_bytes": file_bytes,
    }



def normalize_master(master_df, bridge_df):
    df = master_df.copy()

    alias_sets = {
        "SKU": ["SKU", "CODIGO", "CÓDIGO", "COD SKU"],
        "DESCRIPCIÓN": ["DESCRIPCIÓN", "DESCRIPCION", "PRODUCTO", "ARTICULO", "ARTÍCULO", "DETALLE", "NOMBRE"],
        "UBIC": ["UBIC", "UBICACION", "UBICACIÓN"],
        "ÚLTIMO COSTO": ["ÚLTIMO COSTO", "ULTIMO COSTO", "COSTO", "ULT COSTO"],
        "PRECIO BRUTO": ["PRECIO BRUTO", "PRECIO VENTA", "PVP", "PRECIO"],
        "PRECIO NETO": ["PRECIO NETO", "NETO"],
        "MARGEN LOCAL": ["MARGEN LOCAL"],
        "MARGEN MELI 1": ["MARGEN MELI 1"],
        "MONTO EN SIMULACIÓN": ["MONTO EN SIMULACIÓN", "MONTO EN SIMULACION", "MONTO SIMULACION", "MONTO SIMULADO"],
        "CAMPAÑA PADS": ["CAMPAÑA PADS", "CAMPAÑA PADS"],
        "MLC": ["MLC"],
        "MLC SINCRONIZADO": ["MLC SINCRONIZADO"],
        "PRECIO B2C PUBLICADO ": ["PRECIO B2C PUBLICADO ", "PRECIO B2C PUBLICADO"],
        "FECHA VENCI": ["FECHA VENCI", "FECHA VENCI", "VENCIMIENTO"],
        "COMENTARIO": ["COMENTARIO"],
        "MARGEN MELI 2": ["MARGEN MELI 2"],
        "VENTA BRUTO MELI 2": ["VENTA BRUTO MELI 2"],
        "MLC.1": ["MLC.1", "MLC 2"],
        "MLC SINCRONIZADO.1": ["MLC SINCRONIZADO.1", "MLC SINCRONIZADO 2"],
        "CAMPAÑA PADS.1": ["CAMPAÑA PADS.1", "CAMPAÑA PADS 2"],
        "PRECIO B2C": ["PRECIO B2C", "PRECIO B2C 2"],
        "FECHA VENCI.1": ["FECHA VENCI.1", "FECHA VENCI 2"],
        "COMENTARIO.1": ["COMENTARIO.1", "COMENTARIO 2"],
    }

    for target, aliases in alias_sets.items():
        found = pick_existing_column(df, *aliases)
        if found is None:
            df[target] = np.nan
        elif found != target:
            df[target] = df[found]

    df["sku"] = df["SKU"].map(norm_sku)
    df["descripcion"] = df["DESCRIPCIÓN"].fillna("").astype(str)
    df["costo_maestra"] = df["ÚLTIMO COSTO"].map(safe_float)
    df["precio_bruto"] = df["PRECIO BRUTO"].map(safe_float)
    df["precio_neto"] = df["PRECIO NETO"].map(safe_float)
    df["monto_sim"] = df["MONTO EN SIMULACIÓN"].map(safe_float)
    df["margen_local_maestra"] = df["MARGEN LOCAL"].map(lambda x: safe_float(x) * 100 if abs(safe_float(x, np.nan)) <= 2 else safe_float(x))
    df["margen_meli1_maestra"] = df["MARGEN MELI 1"].map(lambda x: safe_float(x) * 100 if abs(safe_float(x, np.nan)) <= 2 else safe_float(x))
    df["margen_meli2_maestra"] = df["MARGEN MELI 2"].map(lambda x: safe_float(x) * 100 if abs(safe_float(x, np.nan)) <= 2 else safe_float(x))

    for c in ["FECHA VENCI", "FECHA VENCI.1"]:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.normalize()

    df["mlc_1"] = df["MLC"].map(norm_mlc)
    df["mlc_sync_1"] = df["MLC SINCRONIZADO"].map(norm_mlc)
    df["mlc_2"] = df["MLC.1"].map(norm_mlc)
    df["mlc_sync_2"] = df["MLC SINCRONIZADO.1"].map(norm_mlc)
    df["ads_flag"] = (
        df["CAMPAÑA PADS"].astype(str).str.strip().ne("") & df["CAMPAÑA PADS"].notna()
    ) | (
        df["CAMPAÑA PADS.1"].astype(str).str.strip().ne("") & df["CAMPAÑA PADS.1"].notna()
    )

    mlc_bridge = {}
    if bridge_df is not None and not bridge_df.empty:
        tmp = bridge_df.copy()
        sku_col = pick_existing_column(tmp, "SKU", "CODIGO", "CÓDIGO") or tmp.columns[0]
        mlc_col = pick_existing_column(tmp, "Número de publicación", "Numero de publicacion", "MLC", "PUBLICACION") or tmp.columns[-1]
        tmp["sku"] = tmp[sku_col].map(norm_sku)
        tmp["mlc"] = tmp[mlc_col].map(norm_mlc)
        tmp = tmp[(tmp["sku"] != "") & (tmp["mlc"] != "")]
        mlc_bridge = tmp.groupby("sku")["mlc"].apply(lambda s: sorted(set(s))).to_dict()

    all_mlcs = []
    for _, row in df.iterrows():
        vals = [
            row["mlc_1"], row["mlc_sync_1"], row["mlc_2"], row["mlc_sync_2"],
        ]
        vals.extend(mlc_bridge.get(row["sku"], []))
        vals = [v for v in vals if v]
        all_mlcs.append(sorted(set(vals)))
    df["mlcs"] = all_mlcs

    promos = []
    for idx, row in df.iterrows():
        for slot, mlc_col, pads_col, price_col, date_col, comment_col in [
            (1, "mlc_1", "CAMPAÑA PADS", "PRECIO B2C PUBLICADO ", "FECHA VENCI", "COMENTARIO"),
            (2, "mlc_2", "CAMPAÑA PADS.1", "PRECIO B2C", "FECHA VENCI.1", "COMENTARIO.1"),
        ]:
            mlc = row[mlc_col]
            pads = row[pads_col]
            price = safe_float(row[price_col], np.nan)
            dt = row[date_col]
            comment = row[comment_col]
            if mlc or not pd.isna(price) or not pd.isna(dt) or (pd.notna(pads) and str(pads).strip()):
                promos.append({
                    "master_index": idx,
                    "sku": row["sku"],
                    "descripcion": row["descripcion"],
                    "slot": slot,
                    "mlc": mlc,
                    "campana_ads": pads if pd.notna(pads) else "",
                    "precio_b2c": price,
                    "fecha_venci": dt,
                    "comentario": comment if pd.notna(comment) else "",
                })
    promos_df = pd.DataFrame(promos)
    if not promos_df.empty:
        status_info = promos_df["fecha_venci"].apply(lambda x: pd.Series(promo_status(x), index=["status","status_order"]))
        promos_df = pd.concat([promos_df, status_info], axis=1)
    else:
        promos_df = pd.DataFrame(columns=["master_index","sku","descripcion","slot","mlc","campana_ads","precio_b2c","fecha_venci","comentario","status","status_order"])
    return df[df["sku"] != ""].copy(), promos_df



def ensure_promos_schema(promos_df: pd.DataFrame) -> pd.DataFrame:
    if promos_df is None or not isinstance(promos_df, pd.DataFrame):
        return pd.DataFrame(columns=["master_index","sku","descripcion","slot","mlc","campana_ads","precio_b2c","fecha_venci","comentario","status","status_order"])
    df = promos_df.copy()
    rename_map = {
        "STATUS": "status",
        "STATUS_ORDER": "status_order",
        "SKU_norm": "sku",
        "DESCRIPCIÓN": "descripcion",
        "MLC": "mlc",
        "PRECIO_B2C": "precio_b2c",
        "FECHA_VENCI": "fecha_venci",
        "COMENTARIO": "comentario",
    }
    df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns and v not in df.columns})
    required = ["master_index","sku","descripcion","slot","mlc","campana_ads","precio_b2c","fecha_venci","comentario"]
    for col in required:
        if col not in df.columns:
            df[col] = np.nan if col not in ["descripcion","mlc","campana_ads","comentario"] else ""
    if "status" not in df.columns or "status_order" not in df.columns:
        status_info = df["fecha_venci"].apply(lambda x: pd.Series(promo_status(x), index=["status","status_order"]))
        for col in ["status","status_order"]:
            df[col] = status_info[col]
    return df

def promo_status(dt):
    dt = to_date_only(dt)
    if pd.isna(dt):
        return "Vencen en 1 mes", 30
    today = pd.Timestamp(date.today())
    delta = (dt - today).days
    if delta < 0:
        return "Vencidas", -1
    if delta == 0:
        return "Vencen hoy", 0
    if delta == 1:
        return "Vencen mañana", 1
    if delta == 2:
        return "Vencen pasado mañana", 2
    if delta <= 7:
        return "Vencen en 7 días", 7
    if delta <= 15:
        return "Vencen en 15 días", 15
    return "Vencen en 1 mes", 30


def rebuild_promos_from_master(master: pd.DataFrame) -> pd.DataFrame:
    promos = []
    if master is None or master.empty:
        return pd.DataFrame(columns=["master_index","sku","descripcion","slot","mlc","campana_ads","precio_b2c","fecha_venci","comentario","status","status_order"])
    for idx, row in master.iterrows():
        for slot, mlc_col, pads_col, price_col, date_col, comment_col in [
            (1, "mlc_1", "CAMPAÑA PADS", "PRECIO B2C PUBLICADO ", "FECHA VENCI", "COMENTARIO"),
            (2, "mlc_2", "CAMPAÑA PADS.1", "PRECIO B2C", "FECHA VENCI.1", "COMENTARIO.1"),
        ]:
            mlc = row.get(mlc_col, "")
            pads = row.get(pads_col, "")
            price = safe_float(row.get(price_col), np.nan)
            dt = row.get(date_col, pd.NaT)
            comment = row.get(comment_col, "")
            if mlc or not pd.isna(price) or not pd.isna(dt) or (pd.notna(pads) and str(pads).strip()):
                status, order = promo_status(dt)
                promos.append({
                    "master_index": idx,
                    "sku": row.get("sku",""),
                    "descripcion": row.get("descripcion",""),
                    "slot": slot,
                    "mlc": mlc,
                    "campana_ads": pads if pd.notna(pads) else "",
                    "precio_b2c": price,
                    "fecha_venci": dt,
                    "comentario": comment if pd.notna(comment) else "",
                    "status": status,
                    "status_order": order,
                })
    return pd.DataFrame(promos)


def update_single_promo(model: dict, master_index: int, slot: int, price, dt, comment):
    master = model["master"]
    if slot == 1:
        price_col, date_col, comment_col = "PRECIO B2C PUBLICADO ", "FECHA VENCI", "COMENTARIO"
    else:
        price_col, date_col, comment_col = "PRECIO B2C", "FECHA VENCI.1", "COMENTARIO.1"
    master.at[master_index, price_col] = safe_float(price, np.nan)
    master.at[master_index, date_col] = pd.to_datetime(dt).normalize() if dt else pd.NaT
    master.at[master_index, comment_col] = comment
    model["promos"] = rebuild_promos_from_master(master)


def normalize_rel(rel_df):
    if rel_df is None or rel_df.empty:
        return pd.DataFrame(columns=["sku", "descripcion", "precio_b2c", "tipo", "estado"])
    df = rel_df.copy()
    while df.shape[1] < 6:
        df[df.shape[1]] = np.nan
    df = df.iloc[:, :6]
    df.columns = ["SKU_raw", "descripcion", "precio_b2c", "extra", "tipo", "estado"]
    df["sku"] = df["SKU_raw"].map(norm_sku)
    df["precio_b2c"] = df["precio_b2c"].map(safe_float)
    df = df[df["sku"] != ""].copy()
    return df[["sku", "descripcion", "precio_b2c", "tipo", "estado"]]


@st.cache_data(show_spinner=False)
def load_sales(file_bytes: bytes):
    raw = pd.read_excel(io.BytesIO(file_bytes))
    raw = raw.copy()
    for col in ["SKU", "Fecha", "Vendedor", "Documento", "Cantidad", "Precio Un.", "Total Línea", "Producto", "Rut", "Razón Social"]:
        if col not in raw.columns:
            raw[col] = np.nan
    raw["sku"] = raw["SKU"].map(norm_sku)
    raw["fecha"] = pd.to_datetime(raw["Fecha"], errors="coerce", dayfirst=True).dt.normalize()
    raw["canal"] = raw["Vendedor"].apply(detect_channel)
    raw["tipo_cliente"] = raw["Documento"].apply(detect_buyer_type)
    raw["cantidad"] = raw["Cantidad"].map(safe_float)
    raw["precio_unitario"] = raw["Precio Un."].map(safe_float)
    raw["total_linea"] = raw["Total Línea"].map(safe_float)
    raw["producto"] = raw["Producto"].fillna("").astype(str)
    raw["rut"] = raw["Rut"].fillna("").astype(str)
    raw["cliente"] = raw["Razón Social"].fillna("").astype(str)
    raw = raw[raw["sku"] != ""].copy()
    return raw


@st.cache_data(show_spinner=False)
def load_purchases(file_bytes: bytes):
    raw = pd.read_excel(io.BytesIO(file_bytes))
    raw = raw.copy()
    for col in ["SKU", "Fecha", "Razón Social", "Precio Un.", "Cantidad", "Documento", "Folio"]:
        if col not in raw.columns:
            raw[col] = np.nan
    raw["sku"] = raw["SKU"].map(norm_sku)
    raw["fecha"] = pd.to_datetime(raw["Fecha"], errors="coerce", dayfirst=True).dt.normalize()
    raw["proveedor"] = raw["Razón Social"].fillna("").astype(str)
    raw["precio_unitario"] = raw["Precio Un."].map(safe_float)
    raw["cantidad"] = raw["Cantidad"].map(safe_float)
    raw["documento"] = raw["Documento"].fillna("").astype(str)
    raw["folio"] = raw["Folio"].fillna("").astype(str)
    raw = raw[raw["sku"] != ""].copy()
    return raw


@st.cache_data(show_spinner=False)
def load_publications(file_bytes: bytes):
    raw = pd.read_excel(io.BytesIO(file_bytes))
    raw = raw.copy()
    rename = {
        "Id": "mlc",
        "SKU": "sku",
        "Título": "titulo",
        "Comision": "comision_pct",
        "Cargo cuotas": "cargo_cuotas_pct",
        "Total cargo": "total_cargo_pct",
        "Total cargo $": "total_cargo_monto",
        "Costo fijo": "costo_fijo",
        "Precio Final": "precio_final",
        "Precio Base": "precio_base",
        "Precio Oferta": "precio_oferta",
        "Ventas": "ventas_hist_pub",
        "Cantidad": "cantidad_pub",
        "Full": "full_stock",
        "Calidad": "calidad",
        "Categoría": "categoria",
        "Nombre Categoría": "categoria_nombre",
        "Fecha creación": "fecha_creacion",
        "Días publicados": "dias_publicado",
        "Ventas/Días pub.": "ventas_por_dia_pub",
        "Stock Real": "stock_real",
        "Status": "status",
        "Entrega": "entrega",
        "Dimensiones": "dimensiones_raw",
        "Link": "link",
    }
    for src, dst in rename.items():
        if src in raw.columns:
            raw[dst] = raw[src]
        else:
            raw[dst] = np.nan

    raw["sku"] = raw["sku"].map(norm_sku)
    raw["mlc"] = raw["mlc"].map(norm_mlc)
    raw["fecha_creacion"] = pd.to_datetime(raw["fecha_creacion"], errors="coerce", dayfirst=True).dt.normalize()
    for c in ["comision_pct", "cargo_cuotas_pct", "total_cargo_pct", "total_cargo_monto", "costo_fijo", "precio_final", "precio_base",
              "precio_oferta", "ventas_hist_pub", "cantidad_pub", "full_stock", "calidad", "dias_publicado", "ventas_por_dia_pub", "stock_real"]:
        raw[c] = raw[c].map(safe_float)
    dims = raw["dimensiones_raw"].apply(parse_dimensions).apply(pd.Series)
    raw = pd.concat([raw, dims], axis=1)
    raw["ingreso_estimado_ml"] = raw["precio_final"].map(safe_float) - raw["total_cargo_monto"].map(safe_float).fillna(0) - raw["costo_fijo"].map(safe_float).fillna(0)
    raw = raw[raw["sku"] != ""].copy()
    return raw


@st.cache_data(show_spinner=False)
def load_product_ads(file_bytes: bytes):
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name="Reporte por anuncios", header=1)
    df = df.copy()
    cols = {
        "Campaña": "campana",
        "Título de anuncio": "titulo",
        "Número de \npublicación": "mlc",
        "Estado": "estado",
        "Impresiones": "impresiones",
        "Clics": "clics",
        "Ingresos\n(Moneda local)": "ingresos_ads",
        "Inversión\n(Moneda local)": "inversion_ads",
        "ACOS\n(Inversión / Ingresos)": "acos",
        "ROAS\n(Ingresos / Inversión)": "roas",
        "Ventas por publicidad\n(Directas + Indirectas)": "ventas_ads",
    }
    out = pd.DataFrame()
    for src, dst in cols.items():
        out[dst] = df[src] if src in df.columns else np.nan
    out["mlc"] = out["mlc"].map(norm_mlc)
    for c in ["impresiones", "clics", "ingresos_ads", "inversion_ads", "acos", "roas", "ventas_ads"]:
        out[c] = out[c].map(safe_float)
    out = out[out["mlc"] != ""].copy()
    return out


@st.cache_data(show_spinner=False)
def load_keywords(file_bytes: bytes):
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name="Reporte por palabras clave", header=1)
    df = df.copy()
    cols = {
        "Campaña": "campana",
        "Palabra clave": "palabra_clave",
        "Segmentación": "segmentacion",
        "Impresiones": "impresiones",
        "Clics": "clics",
        "Ingresos\n(Moneda local)": "ingresos",
        "Inversión\n(Moneda local)": "inversion",
        "ACOS\n(Inversión / Ingresos)": "acos",
        "ROAS\n(Ingresos / Inversión)": "roas",
        "Ventas por publicidad": "ventas_ads",
    }
    out = pd.DataFrame()
    for src, dst in cols.items():
        out[dst] = df[src] if src in df.columns else np.nan
    for c in ["impresiones", "clics", "ingresos", "inversion", "acos", "roas", "ventas_ads"]:
        out[c] = out[c].map(safe_float)
    return out


# =========================================================
# Metrics engine
# =========================================================
def attach_historical_purchase_cost_to_sales(ml_sales, purchases, master):
    sales = ml_sales.copy()
    if sales.empty:
        sales["costo_unit_historico"] = np.nan
        return sales

    fallback_cost = master.set_index("sku")["costo_maestra"].to_dict()
    if purchases is None or purchases.empty:
        sales["costo_unit_historico"] = sales["sku"].map(fallback_cost)
        return sales

    sales = sales.sort_values(["sku", "fecha"]).copy()
    purchases = purchases.sort_values(["sku", "fecha"]).copy()
    merged_parts = []
    for sku, sgrp in sales.groupby("sku", sort=False):
        pgrp = purchases[purchases["sku"] == sku][["fecha", "precio_unitario"]].sort_values("fecha")
        sgrp = sgrp.sort_values("fecha").copy()
        if not pgrp.empty:
            mg = pd.merge_asof(
                sgrp,
                pgrp,
                on="fecha",
                direction="backward",
                suffixes=("", "_compra")
            )
            mg["costo_unit_historico"] = mg["precio_unitario_compra"].map(safe_float)
            mg.drop(columns=[c for c in ["precio_unitario_compra"] if c in mg.columns], inplace=True)
        else:
            mg = sgrp.copy()
            mg["costo_unit_historico"] = np.nan
        mg["costo_unit_historico"] = mg["costo_unit_historico"].fillna(fallback_cost.get(sku, np.nan))
        merged_parts.append(mg)
    return pd.concat(merged_parts, ignore_index=True) if merged_parts else sales


def summarize_sales_windows(sales, master, purchases, days_list=(30, 90)):
    out = {}
    today = pd.Timestamp(date.today())

    ml_sales = sales[sales["canal"] == "ML"].copy()
    ml_sales = attach_historical_purchase_cost_to_sales(ml_sales, purchases, master)
    ml_sales["utilidad_linea"] = ml_sales["total_linea"] - (ml_sales["cantidad"] * ml_sales["costo_unit_historico"])
    ml_sales["margen_linea"] = np.where(ml_sales["total_linea"] > 0, (ml_sales["utilidad_linea"] / ml_sales["total_linea"]) * 100, np.nan)

    def hist_margin(df):
        ingresos = df["total_linea"].sum()
        utilidad = df["utilidad_linea"].sum()
        if ingresos <= 0:
            return np.nan
        return (utilidad / ingresos) * 100

    total_hist = ml_sales.groupby("sku").apply(hist_margin).rename("margen_hist_total").reset_index()

    for d in days_list:
        cutoff = today - pd.Timedelta(days=d)
        sw = sales[sales["fecha"] >= cutoff].copy()
        mlw = ml_sales[ml_sales["fecha"] >= cutoff].copy()

        bysku = sw.groupby(["sku", "canal"]).agg(
            ingresos=("total_linea", "sum"),
            unidades=("cantidad", "sum"),
            ventas=("sku", "size")
        ).reset_index()

        rows = []
        for sku, grp in bysku.groupby("sku"):
            row = {"sku": sku}
            for canal in ["ML", "TIENDA"]:
                cgrp = grp[grp["canal"] == canal]
                row[f"ingresos_{canal.lower()}_{d}d"] = cgrp["ingresos"].sum() if not cgrp.empty else 0.0
                row[f"unidades_{canal.lower()}_{d}d"] = cgrp["unidades"].sum() if not cgrp.empty else 0.0
                row[f"ventas_{canal.lower()}_{d}d"] = cgrp["ventas"].sum() if not cgrp.empty else 0.0
            rows.append(row)
        base = pd.DataFrame(rows)

        # buyer split and purchase pattern
        sw_pos = sw[(sw["cantidad"] > 0) & (sw["total_linea"] > 0)].copy()
        sw_pos = sw_pos[sw_pos["tipo_cliente"].isin(["EMPRESA", "PERSONA"])].copy()
        if not sw_pos.empty:
            bt = sw_pos.groupby(["sku", "tipo_cliente"]).agg(
                ingresos=("total_linea", "sum"),
                unidades=("cantidad", "sum"),
                ventas=("sku", "size"),
                mediana_unidades=("cantidad", "median"),
                p90_unidades=("cantidad", lambda s: s.quantile(0.90))
            ).reset_index()
        else:
            bt = pd.DataFrame(columns=["sku", "tipo_cliente"])

        buyer_rows = []
        for sku, grp in bt.groupby("sku"):
            row = {"sku": sku}
            total_ing = grp["ingresos"].sum()
            for tipo in ["EMPRESA", "PERSONA"]:
                tgrp = grp[grp["tipo_cliente"] == tipo]
                ing = tgrp["ingresos"].sum() if not tgrp.empty else 0.0
                row[f"participacion_{tipo.lower()}_{d}d"] = (ing / total_ing * 100) if total_ing > 0 else np.nan
                row[f"mediana_unidades_{tipo.lower()}_{d}d"] = tgrp["mediana_unidades"].iloc[0] if not tgrp.empty else np.nan
                row[f"p90_unidades_{tipo.lower()}_{d}d"] = tgrp["p90_unidades"].iloc[0] if not tgrp.empty else np.nan
            buyer_rows.append(row)
        buyer_df = pd.DataFrame(buyer_rows)

        hist_d = mlw.groupby("sku").apply(hist_margin).rename(f"margen_hist_{d}d").reset_index() if not mlw.empty else pd.DataFrame(columns=["sku", f"margen_hist_{d}d"])

        out[d] = base.merge(buyer_df, on="sku", how="outer").merge(hist_d, on="sku", how="outer")
    return out, total_hist, ml_sales


def summarize_purchases(purchases):
    if purchases is None or purchases.empty:
        return pd.DataFrame(columns=[
            "sku", "ultima_fecha_compra", "ultimo_costo_compra", "ultimo_proveedor", "ultima_cantidad_compra", "brecha_doc", "compras_total"
        ]), {}
    by_sku = {}
    rows = []
    for sku, grp in purchases.groupby("sku", sort=False):
        grp = grp.sort_values("fecha")
        by_sku[sku] = grp.copy()
        last = grp.iloc[-1]
        rows.append({
            "sku": sku,
            "ultima_fecha_compra": last["fecha"],
            "ultimo_costo_compra": safe_float(last["precio_unitario"]),
            "ultimo_proveedor": last["proveedor"],
            "ultima_cantidad_compra": safe_float(last["cantidad"]),
            "compras_total": len(grp),
        })
    return pd.DataFrame(rows), by_sku


def aggregate_ads_by_sku(product_ads, publications):
    if product_ads is None or product_ads.empty or publications is None or publications.empty:
        return pd.DataFrame(columns=["sku", "ads_inversion", "ads_ingresos", "ads_acos", "ads_roas", "ads_ventas", "ads_impresiones", "ads_clics"])
    pubs_map = publications[["mlc", "sku"]].drop_duplicates()
    ads = product_ads.merge(pubs_map, on="mlc", how="left")
    ads = ads[ads["sku"].notna()].copy()
    out = ads.groupby("sku").agg(
        ads_inversion=("inversion_ads", "sum"),
        ads_ingresos=("ingresos_ads", "sum"),
        ads_ventas=("ventas_ads", "sum"),
        ads_impresiones=("impresiones", "sum"),
        ads_clics=("clics", "sum"),
    ).reset_index()
    out["ads_acos"] = np.where(out["ads_ingresos"] > 0, out["ads_inversion"] / out["ads_ingresos"] * 100, np.nan)
    out["ads_roas"] = np.where(out["ads_inversion"] > 0, out["ads_ingresos"] / out["ads_inversion"], np.nan)
    return out


def keywords_summary(keywords):
    if keywords is None or keywords.empty:
        return {
            "campanas": 0,
            "inversion": 0.0,
            "ingresos": 0.0,
            "acos": np.nan,
            "roas": np.nan,
            "top_keywords": pd.DataFrame(columns=["palabra_clave", "ingresos", "inversion", "acos", "roas", "clics", "impresiones"])
        }
    df = keywords.copy()
    inversion = df["inversion"].sum()
    ingresos = df["ingresos"].sum()
    return {
        "campanas": df["campana"].nunique(),
        "inversion": inversion,
        "ingresos": ingresos,
        "acos": (inversion / ingresos * 100) if ingresos > 0 else np.nan,
        "roas": (ingresos / inversion) if inversion > 0 else np.nan,
        "top_keywords": df.sort_values(["ingresos", "inversion"], ascending=[False, False]).head(20)[
            ["palabra_clave", "ingresos", "inversion", "acos", "roas", "clics", "impresiones"]
        ]
    }


def build_action_table(master, sales_windows, total_hist, purchase_summary, publications, ads_by_sku):
    base = master[[
        "sku", "descripcion", "costo_maestra", "precio_bruto", "monto_sim",
        "margen_local_maestra", "margen_meli1_maestra", "ads_flag", "mlcs"
    ]].copy()

    sw30 = sales_windows.get(30, pd.DataFrame(columns=["sku"]))
    sw90 = sales_windows.get(90, pd.DataFrame(columns=["sku"]))
    base = base.merge(sw30, on="sku", how="left").merge(sw90[["sku", "margen_hist_90d"]], on="sku", how="left").merge(total_hist, on="sku", how="left")
    base = base.merge(purchase_summary, on="sku", how="left").merge(ads_by_sku, on="sku", how="left")

    # current publication snapshot
    pub_primary_rows = []
    pub_map = {}
    if publications is not None and not publications.empty:
        for sku, grp in publications.groupby("sku", sort=False):
            pr = choose_primary_publication(grp)
            if pr is not None:
                pub_map[sku] = grp.copy()
                pub_primary_rows.append({
                    "sku": sku,
                    "mlc_principal": pr["mlc"],
                    "precio_ml_actual": safe_float(pr["precio_final"]),
                    "precio_ml_base": safe_float(pr["precio_base"]),
                    "precio_ml_oferta": safe_float(pr["precio_oferta"]),
                    "ingreso_estimado_ml": safe_float(pr["ingreso_estimado_ml"]),
                    "dias_publicado": safe_float(pr["dias_publicado"]),
                    "stock_real": safe_float(pr["stock_real"]),
                    "ventas_por_dia_pub": safe_float(pr["ventas_por_dia_pub"]),
                    "status_publicacion": pr["status"],
                    "dimensiones": pr["dimensiones"],
                    "peso_volumetrico_kg": safe_float(pr["peso_volumetrico_kg"]),
                    "comision_pct_ml": safe_float(pr.get("comision_pct", np.nan)),
                    "cargo_cuotas_pct_ml": safe_float(pr.get("cargo_cuotas_pct", np.nan)),
                    "total_cargo_pct_ml": safe_float(pr.get("total_cargo_pct", np.nan)),
                    "total_cargo_monto_ml": safe_float(pr.get("total_cargo_monto", np.nan)),
                    "costo_fijo_ml": safe_float(pr.get("costo_fijo", np.nan)),
                })
    pub_primary = pd.DataFrame(pub_primary_rows)
    base = base.merge(pub_primary, on="sku", how="left")

    base["ads_share_ml_pct"] = np.where(
        base["ingresos_ml_30d"].fillna(0) > 0,
        (base["ads_inversion"].fillna(0) / base["ingresos_ml_30d"].fillna(0)) * 100,
        0.0
    )
    base["monto_sim_neto"] = base["monto_sim"].map(safe_float) / 1.19
    base["margen_ads_base_pct"] = base.apply(lambda r: calc_margin_from_monto_sim(r["costo_maestra"], r["monto_sim"]), axis=1)
    base["margen_ml_actual"] = base["margen_ads_base_pct"]
    base["margen_ml_reportado"] = base.apply(lambda r: calc_margin_from_ml_price(r["costo_maestra"], r["precio_ml_actual"], r.get("total_cargo_pct_ml", np.nan), r.get("costo_fijo_ml", np.nan), 0.0), axis=1)
    base["margen_ml_con_ads"] = np.where(
        base["ads_acos"].notna(),
        base["margen_ads_base_pct"] - base["ads_acos"],
        base["margen_ads_base_pct"],
    )
    base["margen_tienda_actual"] = base.apply(lambda r: calc_margin_from_bruto(r["costo_maestra"], r["precio_bruto"]), axis=1)
    base["brecha_costo_pct"] = np.where(
        base["costo_maestra"].notna() & base["ultimo_costo_compra"].notna() & (base["costo_maestra"] != 0),
        ((base["ultimo_costo_compra"] - base["costo_maestra"]) / base["costo_maestra"]) * 100,
        np.nan
    )
    base["brecha_costo_clp"] = np.where(
        base["costo_maestra"].notna() & base["ultimo_costo_compra"].notna(),
        base["ultimo_costo_compra"] - base["costo_maestra"],
        np.nan
    )
    base["brecha_precio_pct"] = np.where(
        base["precio_bruto"].notna() & base["precio_ml_actual"].notna() & (base["precio_bruto"] != 0),
        ((base["precio_ml_actual"] - base["precio_bruto"]) / base["precio_bruto"]) * 100,
        np.nan
    )
    base["brecha_monto_sim_pct"] = np.where(
        base["monto_sim"].notna() & base["ingreso_estimado_ml"].notna() & (base["monto_sim"] != 0),
        ((base["ingreso_estimado_ml"] - base["monto_sim"]) / base["monto_sim"]) * 100,
        np.nan
    )
    base["delta_margen_30d_pp"] = base["margen_ml_actual"] - base["margen_hist_30d"]
    base["estado_brecha_costo"] = base["brecha_costo_pct"].apply(classify_cost_gap_pct)
    base["estado_margen"] = base["delta_margen_30d_pp"].apply(classify_margin_delta_pp)
    base["objetivo_margen_ads_pct"] = ADS_TARGET_MARGIN_PCT
    base["acos_alerta_global_pct"] = ADS_GLOBAL_ACOS_ALERT_PCT
    base["roas_minimo_ads"] = ADS_GLOBAL_ROAS_MIN
    base["acos_max_permitido_pct"] = base["margen_ads_base_pct"] - base["objetivo_margen_ads_pct"]
    base["gap_acos_pct"] = np.where(
        base["ads_acos"].notna() & base["acos_max_permitido_pct"].notna(),
        base["ads_acos"] - base["acos_max_permitido_pct"],
        np.nan,
    )
    base["brecha_ads_objetivo_pp"] = base["margen_ml_con_ads"] - base["objetivo_margen_ads_pct"]
    base["estado_ads"] = base.apply(classify_ads_state, axis=1)
    base["motivo_ads"] = base.apply(classify_ads_reason, axis=1)
    base["accion_ads"] = base.apply(suggest_ads_action, axis=1)
    base["ads_score"] = 0.0
    base["ads_score"] += base["brecha_ads_objetivo_pp"].fillna(0) * 0.6
    base["ads_score"] += base["ads_roas"].fillna(0) * 0.25
    base["ads_score"] -= base["ads_acos"].fillna(0) * 0.3

    def action(row):
        cost_state = row["estado_brecha_costo"]
        margin_state = row["estado_margen"]
        if cost_state == "CRÍTICO" and margin_state in ("CRÍTICO", "ALERTA"):
            return "REPRECIO URGENTE"
        if cost_state == "CRÍTICO":
            return "REVISAR COSTO Y PRECIO"
        if str(row.get("estado_ads", "")).upper() == "CRÍTICO":
            return "REVISAR ADS URGENTE"
        if str(row.get("estado_ads", "")).upper() == "ALERTA":
            return "OPTIMIZAR ADS / PRECIO"
        if row.get("ads_flag", False) and margin_state in ("CRÍTICO", "ALERTA"):
            return "REVISAR PRECIO / ADS"
        if margin_state == "CRÍTICO":
            return "REVISAR RENTABILIDAD"
        if cost_state == "BAJÓ COSTO":
            return "OPORTUNIDAD"
        return "MANTENER / MONITOREAR"

    def semaforo(row):
        cost_state = row["estado_brecha_costo"]
        margin_state = row["estado_margen"]
        ads_state = str(row.get("estado_ads", "")).upper()
        if cost_state == "CRÍTICO" or margin_state == "CRÍTICO" or ads_state == "CRÍTICO":
            return "CRÍTICO"
        if cost_state == "ALERTA" or margin_state == "ALERTA" or ads_state == "ALERTA":
            return "ALERTA"
        if cost_state == "BAJÓ COSTO" or margin_state == "MEJORA" or ads_state == "OPORTUNIDAD":
            return "OPORTUNIDAD"
        return "ESTABLE"

    base["estado_general"] = base.apply(semaforo, axis=1)
    base["accion_sugerida"] = base.apply(action, axis=1)

    state_score = {"CRÍTICO": 3, "ALERTA": 2, "OPORTUNIDAD": 1, "ESTABLE": 0}
    base["score"] = base["estado_general"].map(state_score).fillna(0) * 100
    base["score"] += base["ingresos_ml_30d"].fillna(0) / 10000
    base["score"] += base["ads_inversion"].fillna(0) / 10000
    base = base.sort_values(["score", "ingresos_ml_30d"], ascending=[False, False])

    return base, pub_map


def rel_to_sheet_df(rel_df: pd.DataFrame) -> pd.DataFrame:
    if rel_df is None or rel_df.empty:
        return pd.DataFrame(columns=list(range(6)))
    out = pd.DataFrame({
        0: rel_df["sku"],
        1: rel_df["descripcion"],
        2: rel_df["precio_b2c"],
        3: np.nan,
        4: rel_df["tipo"],
        5: rel_df["estado"],
    })
    return out


@st.cache_data(show_spinner=False)
def build_download_bytes(master_df: pd.DataFrame, rel_df: pd.DataFrame, original_bytes: bytes, maestra_name: str, rel_name: str):
    xls = pd.ExcelFile(io.BytesIO(original_bytes))
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for sheet in xls.sheet_names:
            if sheet == maestra_name:
                drop_cols = [
                    "sku", "descripcion", "costo_maestra", "precio_bruto", "precio_neto", "monto_sim", "margen_local_maestra",
                    "margen_meli1_maestra", "margen_meli2_maestra", "mlc_1", "mlc_2", "mlc_sync_1", "mlc_sync_2", "ads_flag", "mlcs"
                ]
                master_df.drop(columns=[c for c in drop_cols if c in master_df.columns], errors="ignore").to_excel(writer, sheet_name=sheet, index=False)
            elif rel_name and sheet == rel_name:
                rel_to_sheet_df(rel_df).to_excel(writer, sheet_name=sheet, index=False, header=False)
            else:
                pd.read_excel(io.BytesIO(original_bytes), sheet_name=sheet, header=None if "relampago" in sheet.lower() else 0).to_excel(
                    writer,
                    sheet_name=sheet,
                    index=False,
                    header=not ("relampago" in sheet.lower())
                )
    return out.getvalue()


# =========================================================
# Model
# =========================================================
def build_model(master_up, ventas_up, compras_up=None, pubs_up=None, ads_up=None, keywords_up=None):
    wb = load_master_workbook(master_up.getvalue())
    master, promos = normalize_master(wb["master_df"], wb["bridge_df"])
    rel = normalize_rel(wb["rel_df"])

    ventas = load_sales(ventas_up.getvalue()) if ventas_up else pd.DataFrame()
    compras = load_purchases(compras_up.getvalue()) if compras_up else pd.DataFrame()
    pubs = load_publications(pubs_up.getvalue()) if pubs_up else pd.DataFrame()
    product_ads = load_product_ads(ads_up.getvalue()) if ads_up else pd.DataFrame()
    keywords = load_keywords(keywords_up.getvalue()) if keywords_up else pd.DataFrame()

    sales_windows, total_hist, ml_sales = summarize_sales_windows(ventas, master, compras, days_list=(7, 15, 30, 90))
    purchase_summary, purchase_map = summarize_purchases(compras)
    ads_by_sku = aggregate_ads_by_sku(product_ads, pubs)
    kw_summary = keywords_summary(keywords)
    action_table, pub_map = build_action_table(master, sales_windows, total_hist, purchase_summary, pubs, ads_by_sku)
    validations = build_validation_layers(master, ventas, compras, pubs, product_ads, promos, action_table)

    product_options = action_table["sku"].dropna().tolist()
    sku_desc = action_table.set_index("sku")["descripcion"].to_dict()

    return {
        "wb": wb,
        "master": master,
        "promos": promos,
        "rel": rel,
        "ventas": ventas,
        "compras": compras,
        "pubs": pubs,
        "product_ads": product_ads,
        "keywords": keywords,
        "kw_summary": kw_summary,
        "sales_windows": sales_windows,
        "ml_sales": ml_sales,
        "purchase_summary": purchase_summary,
        "purchase_map": purchase_map,
        "ads_by_sku": ads_by_sku,
        "action_table": action_table,
        "pub_map": pub_map,
        "product_options": product_options,
        "sku_desc": sku_desc,
        "validations": validations,
    }


@st.cache_data(show_spinner=False)
def build_model_cached(master_bytes, ventas_bytes, compras_bytes=None, pubs_bytes=None, ads_bytes=None, keywords_bytes=None):
    master_up = StoredUploadedFile(Path(FILE_SPECS["master"]["filename"]), master_bytes)
    ventas_up = StoredUploadedFile(Path(FILE_SPECS["ventas"]["filename"]), ventas_bytes)
    compras_up = StoredUploadedFile(Path(FILE_SPECS["compras"]["filename"]), compras_bytes) if compras_bytes else None
    pubs_up = StoredUploadedFile(Path(FILE_SPECS["pubs"]["filename"]), pubs_bytes) if pubs_bytes else None
    ads_up = StoredUploadedFile(Path(FILE_SPECS["ads"]["filename"]), ads_bytes) if ads_bytes else None
    keywords_up = StoredUploadedFile(Path(FILE_SPECS["keywords"]["filename"]), keywords_bytes) if keywords_bytes else None
    return build_model(master_up, ventas_up, compras_up, pubs_up, ads_up, keywords_up)


def build_shared_model(resolved_files: dict):
    return build_model_cached(
        resolved_files["master"].getvalue() if resolved_files.get("master") else None,
        resolved_files["ventas"].getvalue() if resolved_files.get("ventas") else None,
        resolved_files["compras"].getvalue() if resolved_files.get("compras") else None,
        resolved_files["pubs"].getvalue() if resolved_files.get("pubs") else None,
        resolved_files["ads"].getvalue() if resolved_files.get("ads") else None,
        resolved_files["keywords"].getvalue() if resolved_files.get("keywords") else None,
    )


def persist_current_master_workbook(model: dict, note: str = "maestra actualizada desde app"):
    ensure_storage_dirs()
    wb = model["wb"]
    output_bytes = build_download_bytes(model["master"], model["rel"], wb["file_bytes"], wb["maestra_name"], wb["rel_name"])
    active_path = active_file_path("master")
    archived_path = archive_existing_active_file("master")
    active_path.write_bytes(output_bytes)
    stored = StoredUploadedFile(path=active_path, data=output_bytes, original_name=active_path.name)
    log_source_file_event(
        file_key="master",
        active_filename=active_path.name,
        archived_filename=archived_path.name if archived_path else "",
        original_filename=note,
        file_sig=file_signature(stored),
        file_size=len(output_bytes),
    )
    build_model_cached.clear()
    load_master_workbook.clear()
    bump_shared_version(note)
    return stored


# =========================================================
# UI bootstrap
# =========================================================
ensure_storage_dirs()
ensure_source_files_table()

st.title("Centro de Control Comercial Aurora")

resolved_files = {}
resolved_sources = {}

with st.sidebar:
    st.subheader("Archivos activos")
    uploaders = {}
    for file_key, spec in FILE_SPECS.items():
        uploaders[file_key] = st.file_uploader(spec["label"], type=["xlsx"], key=f"upload_{file_key}")

    pending_updates = [FILE_SPECS[k]["label"] for k, v in uploaders.items() if v is not None]
    if pending_updates:
        st.info("Pendientes por validar y reemplazar: " + ", ".join(pending_updates))

    col_reload, col_apply = st.columns(2)
    with col_reload:
        if st.button("Recargar activos", use_container_width=True):
            st.rerun()
    with col_apply:
        if st.button("Validar y reemplazar", use_container_width=True):
            ok, errors, updated_labels = apply_uploaded_updates(uploaders)
            if ok:
                st.success("Archivos actualizados: " + ", ".join(updated_labels) if updated_labels else "No había archivos nuevos para actualizar.")
                st.rerun()
            else:
                for err in errors:
                    st.error(err)

    for file_key in FILE_SPECS:
        resolved_files[file_key], resolved_sources[file_key] = resolve_input_file(file_key, uploaders[file_key])

    status_df = storage_status_df()
    st.caption("Los archivos nuevos no reemplazan al activo hasta que pases la validación. Se mantiene un único respaldo del archivo anterior.")
    st.dataframe(status_df, use_container_width=True, hide_index=True, height=250)

    st.markdown("---")
    default_period = st.selectbox("Periodo de análisis", [7, 15, 30, 90], index=2)
    st.caption("Ventas, patrones y margen histórico se priorizan con este periodo.")

    st.markdown("---")
    st.caption("Modo compartido sin recarga automática agresiva. Para ver cambios hechos desde otra ventana, usa recargar activos.")

master_up = resolved_files["master"]
ventas_up = resolved_files["ventas"]
compras_up = resolved_files["compras"]
pubs_up = resolved_files["pubs"]
ads_up = resolved_files["ads"]
keywords_up = resolved_files["keywords"]

current_shared_status = get_shared_status()
current_shared_version = int(current_shared_status.get("version", 0) or 0)
previous_shared_version = st.session_state.get("seen_shared_version")
st.session_state["seen_shared_version"] = current_shared_version

required_missing = [
    FILE_SPECS[file_key]["label"]
    for file_key, spec in FILE_SPECS.items()
    if spec["required"] and resolved_files[file_key] is None
]

if required_missing:
    st.info("Para comenzar deja activos o sube al menos: maestra, ventas y publicaciones ML.")
    st.stop()

shared_status = get_shared_status()
combined_sig = "|".join([
    file_signature(x) if x is not None else ""
    for x in [master_up, ventas_up, compras_up, pubs_up, ads_up, keywords_up]
])
current_state_sig = f"v{shared_status['version']}|{combined_sig}"
model = build_shared_model(resolved_files)
action_table = model["action_table"].copy()


# Auto snapshot deduplicado por estado consolidado
if master_up and ventas_up and pubs_up and not action_table.empty:
    sigs = {
        "master_sig": file_signature(master_up) if master_up else "",
        "ventas_sig": file_signature(ventas_up) if ventas_up else "",
        "compras_sig": file_signature(compras_up) if compras_up else "",
        "pubs_sig": file_signature(pubs_up) if pubs_up else "",
        "ads_sig": file_signature(ads_up) if ads_up else "",
        "keywords_sig": file_signature(keywords_up) if keywords_up else "",
    }
    payload_df = action_table[[
        "sku", "descripcion", "costo_maestra", "ultimo_costo_compra", "brecha_costo_pct",
        "precio_bruto", "monto_sim", "precio_ml_actual", "ingreso_estimado_ml", "brecha_precio_pct",
        "brecha_monto_sim_pct", "margen_ml_actual", "margen_hist_30d", "margen_hist_90d", "margen_hist_total",
        "delta_margen_30d_pp", "ingresos_ml_30d", "ingresos_tienda_30d", "ads_inversion", "ads_ingresos", "ads_acos",
    ]].rename(columns={"ingresos_ml_30d": "ventas_ml_30d", "ingresos_tienda_30d": "ventas_tienda_30d"})
    current_payload_sig = payload_signature(payload_df, extra=json.dumps({"sigs": sigs, "state": current_state_sig}, sort_keys=True))
    if get_app_meta("last_snapshot_sig", "") != current_payload_sig:
        try:
            run_id = save_snapshot_to_db(payload_df, sigs)
            set_app_meta("last_snapshot_sig", current_payload_sig)
            set_app_meta("last_run_id", str(run_id))
        except Exception as e:
            st.warning(f"No pude guardar snapshot automático: {e}")

model["action_table"] = action_table
model["validations"] = build_validation_layers(model["master"], model["ventas"], model["compras"], model["pubs"], model["product_ads"], model["promos"], action_table)

tabs = st.tabs([
    "Centro de Control Comercial",
    "Ficha de Producto",
    "Ads",
])

# =========================================================
# Tab 1 - Control center
# =========================================================
with tabs[0]:
    critical_cost = int((action_table["estado_brecha_costo"] == "CRÍTICO").sum())
    alert_cost = int((action_table["estado_brecha_costo"] == "ALERTA").sum())
    critical_margin = int((action_table["estado_margen"] == "CRÍTICO").sum())
    ads_risk = int((action_table.get("estado_ads", pd.Series(dtype=str)).isin(["CRÍTICO", "ALERTA", "NO USAR ADS"])).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Brechas costo críticas", critical_cost)
    c2.metric("Brechas costo alerta", alert_cost)
    c3.metric("Margen histórico deteriorado", critical_margin)
    c4.metric("Ads en riesgo", ads_risk)

    st.subheader("Bandeja de acción")
    f1, f2, f3, f4 = st.columns([1.2, 1, 1, 1.2])
    estado_filter = f1.multiselect("Estado general", ["CRÍTICO", "ALERTA", "OPORTUNIDAD", "ESTABLE"], default=["CRÍTICO", "ALERTA", "OPORTUNIDAD", "ESTABLE"])
    canal_filter = f2.selectbox("Canal", ["Todos", "ML", "TIENDA"], index=0)
    ads_filter = f3.selectbox("Ads", ["Todos", "Solo con ads", "Solo sin ads"], index=0)
    text_filter = f4.text_input("Buscar SKU / descripción / MLC")

    work = action_table.copy()
    if estado_filter:
        work = work[work["estado_general"].isin(estado_filter)]
    if canal_filter == "ML":
        work = work[work["ingresos_ml_30d"].fillna(0) > 0]
    elif canal_filter == "TIENDA":
        work = work[work["ingresos_tienda_30d"].fillna(0) > 0]
    if ads_filter == "Solo con ads":
        work = work[work["ads_flag"]]
    elif ads_filter == "Solo sin ads":
        work = work[~work["ads_flag"]]
    if text_filter:
        q = text_filter.strip().lower()
        work = work[
            work["sku"].astype(str).str.contains(q, na=False) |
            work["descripcion"].astype(str).str.lower().str.contains(q, na=False) |
            work["mlc_principal"].astype(str).str.lower().str.contains(q, na=False)
        ]

    display_required_cols = [
        "sku", "descripcion", "estado_general", "brecha_costo_clp", "brecha_costo_pct", "delta_margen_30d_pp",
        "ads_flag", "margen_ml_actual", "margen_hist_30d", "ingresos_ml_30d", "accion_sugerida"
    ]
    for col in display_required_cols:
        if col not in work.columns:
            if col == "ads_flag":
                work[col] = False
            else:
                work[col] = np.nan
    display = work[display_required_cols].copy()
    display.columns = ["SKU", "Descripción", "Estado", "Brecha costo ($)", "Δ costo %", "Δ margen pp", "Ads", "Margen ML actual", "Margen hist. 30d", "Ventas ML 30d", "Acción sugerida"]
    display["Brecha costo ($)"] = display["Brecha costo ($)"].map(fmt_money)
    display["Δ costo %"] = display["Δ costo %"].map(fmt_pct)
    display["Δ margen pp"] = display["Δ margen pp"].map(lambda x: "—" if pd.isna(x) else f"{x:.1f} pp")
    display["Margen ML actual"] = display["Margen ML actual"].map(fmt_pct)
    display["Margen hist. 30d"] = display["Margen hist. 30d"].map(fmt_pct)
    display["Ventas ML 30d"] = display["Ventas ML 30d"].map(fmt_money)
    display["Ads"] = display["Ads"].map(lambda x: "Sí" if bool(x) else "No")
    st.dataframe(display, use_container_width=True, hide_index=True, height=420)

    sku_labels = [f"{sku} — {model['sku_desc'].get(sku, '')}" for sku in work["sku"].tolist()]
    if sku_labels:
        selected_label = st.selectbox("Abrir producto", sku_labels, key="selected_sku_from_control")
        st.session_state.selected_sku = selected_label.split(" — ")[0]
    else:
        st.info("No hay productos con esos filtros.")

    st.subheader("Capas de validación")
    validations = model.get("validations", {"summary": pd.DataFrame(), "details": {}})
    summary_df = validations.get("summary", pd.DataFrame())
    if summary_df.empty:
        st.info("No encontré hallazgos de validación para la carga actual.")
    else:
        st.dataframe(summary_df, use_container_width=True, hide_index=True, height=240)
        with st.expander("Ver detalle por capa"):
            for name, detail_df in validations.get("details", {}).items():
                if detail_df is not None and not detail_df.empty:
                    st.markdown(f"**{name}**")
                    st.dataframe(detail_df, use_container_width=True, hide_index=True, height=min(260, 60 + 35 * len(detail_df.head(10))))

    st.subheader("Brecha maestra vs última compra")
    b1, b2, b3 = st.columns([1.1, 1.1, 1.4])
    cost_state_filter = b1.multiselect("Estado costo", ["CRÍTICO", "ALERTA", "BAJÓ COSTO", "OK", "SIN DATOS"], default=["CRÍTICO", "ALERTA", "BAJÓ COSTO", "OK"], key="cost_gap_state_filter")
    cost_sort = b2.selectbox("Orden costo", ["Mayor brecha costo", "Mayor última compra", "Mayor costo maestra"], key="cost_gap_sort")
    cost_limit = int(b3.number_input("Filas costo", min_value=20, max_value=10000, value=200, step=20, key="cost_gap_limit"))

    brechas = action_table.copy()
    if cost_state_filter:
        brechas = brechas[brechas["estado_brecha_costo"].isin(cost_state_filter)]
    sort_col = {
        "Mayor brecha costo": "brecha_costo_pct",
        "Mayor última compra": "ultimo_costo_compra",
        "Mayor costo maestra": "costo_maestra",
    }[cost_sort]
    brechas = brechas.sort_values(sort_col, ascending=False, na_position="last")
    brechas_show = brechas[[
        "sku", "descripcion", "costo_maestra", "ultimo_costo_compra", "brecha_costo_clp", "brecha_costo_pct", "estado_brecha_costo", "accion_sugerida"
    ]].copy()
    brechas_show.columns = ["SKU", "Descripción", "Costo maestra", "Última compra", "Brecha costo ($)", "Brecha costo %", "Estado", "Acción"]
    for c in ["Costo maestra", "Última compra", "Brecha costo ($)"]:
        brechas_show[c] = brechas_show[c].map(fmt_money)
    brechas_show["Brecha costo %"] = brechas_show["Brecha costo %"].map(fmt_pct)
    st.dataframe(brechas_show.head(cost_limit), use_container_width=True, hide_index=True, height=320)

    st.subheader("Bandeja de acción Ads")
    st.caption("Prioriza campañas que están quemando margen, las que requieren optimización y las que tienen espacio para escalar.")
    a1, a2, a3, a4 = st.columns([1.1, 1.0, 1.0, 1.2])
    ads_state_filter = a1.multiselect("Estado Ads", ["CRÍTICO", "ALERTA", "OPORTUNIDAD", "OK", "SIN ADS"], default=["CRÍTICO", "ALERTA", "OPORTUNIDAD", "OK"], key="ads_state_filter")
    ads_sort = a2.selectbox("Orden Ads", ["Mayor gap ACOS", "Peor margen con Ads", "Mayor inversión Ads", "Mejor oportunidad Ads"], key="ads_sort")
    ads_only_active = a3.selectbox("Cobertura Ads", ["Solo con ads activos", "Todos"], key="ads_coverage_filter")
    ads_limit = int(a4.number_input("Filas Ads", min_value=20, max_value=10000, value=200, step=20, key="ads_limit"))

    ads_work = action_table.copy()
    if ads_state_filter:
        ads_work = ads_work[ads_work["estado_ads"].isin(ads_state_filter)]
    if ads_only_active == "Solo con ads activos":
        ads_work = ads_work[(ads_work["ads_inversion"].fillna(0) > 0) | (ads_work["ads_ingresos"].fillna(0) > 0) | (ads_work["ads_clics"].fillna(0) > 0)]

    ads_sort_map = {
        "Mayor gap ACOS": ["gap_acos_pct", "ads_inversion"],
        "Peor margen con Ads": ["margen_ml_con_ads", "ads_inversion"],
        "Mayor inversión Ads": ["ads_inversion", "ads_ingresos"],
        "Mejor oportunidad Ads": ["ads_score", "margen_ml_con_ads"],
    }
    sort_cols = ads_sort_map[ads_sort]
    ascending = [False, False]
    if ads_sort == "Peor margen con Ads":
        ascending = [True, False]
    ads_work = ads_work.sort_values(sort_cols, ascending=ascending, na_position="last")

    ads_required_cols = [
        "sku", "descripcion", "estado_ads", "ads_inversion", "ads_ingresos", "ads_acos",
        "acos_max_permitido_pct", "gap_acos_pct", "margen_ml_reportado", "margen_ml_con_ads",
        "brecha_ads_objetivo_pp", "accion_ads"
    ]
    for col in ads_required_cols:
        if col not in ads_work.columns:
            ads_work[col] = np.nan
    ads_show = ads_work[ads_required_cols].copy()
    ads_show.columns = [
        "SKU", "Descripción", "Estado Ads", "Inversión Ads", "Ingresos Ads", "ACOS real",
        "ACOS máx.", "Gap ACOS", "Margen base Ads", "Margen con Ads",
        "Brecha vs objetivo", "Acción Ads"
    ]
    for c in ["Inversión Ads", "Ingresos Ads"]:
        ads_show[c] = ads_show[c].map(fmt_money)
    for c in ["ACOS real", "ACOS máx.", "Gap ACOS", "Margen base Ads", "Margen con Ads", "Brecha vs objetivo"]:
        ads_show[c] = ads_show[c].map(fmt_pct)
    st.dataframe(ads_show.head(ads_limit), use_container_width=True, hide_index=True, height=320)

    st.subheader("Brecha comercial real: monto en simulación maestra vs ingreso estimado del reporte ML")
    st.caption("La brecha comercial principal se mide contra la fuente de verdad operativa: MONTO EN SIMULACIÓN de la maestra versus INGRESO ESTIMADO del reporte de publicaciones.")
    c1, c2, c3 = st.columns([1.1, 1.1, 1.4])
    commercial_sort = c1.selectbox("Orden comercial", ["Mayor brecha comercial", "Mayor ingreso estimado ML", "Mayor monto simulación"], key="commercial_sort")
    only_with_pub = c2.selectbox("Cobertura ML", ["Solo con publicación", "Todos"], key="commercial_pub_filter")
    commercial_limit = int(c3.number_input("Filas comercial", min_value=20, max_value=10000, value=200, step=20, key="commercial_limit"))

    commercial = action_table.copy()
    if only_with_pub == "Solo con publicación":
        commercial = commercial[commercial["ingreso_estimado_ml"].notna()]
    commercial = commercial.sort_values({
        "Mayor brecha comercial": "brecha_monto_sim_pct",
        "Mayor ingreso estimado ML": "ingreso_estimado_ml",
        "Mayor monto simulación": "monto_sim",
    }[commercial_sort], ascending=False, na_position="last")
    commercial_required_cols = [
        "sku", "descripcion", "status_publicacion",
        "monto_sim", "ingreso_estimado_ml", "brecha_monto_sim_pct",
        "precio_ml_base", "precio_ml_oferta", "costo_fijo_ml", "margen_ml_reportado"
    ]
    for col in commercial_required_cols:
        if col not in commercial.columns:
            commercial[col] = np.nan
    commercial_show = commercial[commercial_required_cols].copy()
    commercial_show.columns = [
        "SKU", "Descripción", "Status",
        "Monto simulación maestra", "Ingreso estimado reporte ML", "Brecha comercial %",
        "Precio base ML", "Precio oferta ML", "Costo fijo ML", "Margen ML reportado"
    ]
    for c in ["Monto simulación maestra", "Ingreso estimado reporte ML", "Precio base ML", "Precio oferta ML", "Costo fijo ML"]:
        commercial_show[c] = commercial_show[c].map(fmt_money)
    for c in ["Brecha comercial %", "Margen ML reportado"]:
        commercial_show[c] = commercial_show[c].map(fmt_pct)
    st.dataframe(commercial_show.head(commercial_limit), use_container_width=True, hide_index=True, height=360)

# =========================================================
# Tab 2 - Product sheet
# =========================================================
with tabs[1]:
    if "selected_sku" not in st.session_state:
        st.session_state.selected_sku = model["product_options"][0] if model["product_options"] else None

    options = [f"{sku} — {model['sku_desc'].get(sku, '')}" for sku in model["product_options"]]
    selected_label = st.selectbox("Producto", options, index=max(0, options.index(f"{st.session_state.selected_sku} — {model['sku_desc'].get(st.session_state.selected_sku, '')}")) if st.session_state.selected_sku and f"{st.session_state.selected_sku} — {model['sku_desc'].get(st.session_state.selected_sku, '')}" in options else 0)
    sku = selected_label.split(" — ")[0] if selected_label else None
    st.session_state.selected_sku = sku

    row = action_table[action_table["sku"] == sku]
    if row.empty:
        st.warning("No encontré el SKU seleccionado.")
    else:
        row = row.iloc[0]
        header_l, header_r = st.columns([3, 1.2])
        with header_l:
            st.subheader(f"{row['sku']} — {row['descripcion']}")
            st.write(f"MLC asociados: {format_mlc_list(row.get('mlcs'))}")
        with header_r:
            st.metric("Estado general", row["estado_general"])
            st.metric("Acción sugerida", row["accion_sugerida"])

        st.markdown("### Resumen rápido")
        r1, r2, r3, r4, r5, r6 = st.columns(6)
        r1.metric("Ventas ML 30d", fmt_money(row.get("ingresos_ml_30d")), fmt_int(row.get("unidades_ml_30d")) + " un")
        r2.metric("Ventas tienda 30d", fmt_money(row.get("ingresos_tienda_30d")), fmt_int(row.get("unidades_tienda_30d")) + " un")
        r3.metric("Margen ML actual", fmt_pct(row.get("margen_ml_actual")))
        r4.metric("Margen hist. ML 30d", fmt_pct(row.get("margen_hist_30d")))
        r5.metric("Δ margen", "—" if pd.isna(row.get("delta_margen_30d_pp")) else f"{row.get('delta_margen_30d_pp'):.1f} pp")
        r6.metric("Brecha costo ($)", fmt_money(row.get("brecha_costo_clp")), fmt_pct(row.get("brecha_costo_pct")))

        st.markdown("### Precios y rentabilidad")
        a, b = st.columns(2)
        with a:
            st.markdown("#### Mercado Libre")
            st.write(f"Precio ML actual: {fmt_money(row.get('precio_ml_actual'))}")
            st.write(f"Precio base ML: {fmt_money(row.get('precio_ml_base'))}")
            st.write(f"Precio oferta ML: {fmt_money(row.get('precio_ml_oferta'))}")
            st.write(f"Monto en simulación: {fmt_money(row.get('monto_sim'))}")
            st.write(f"Ingreso estimado ML: {fmt_money(row.get('ingreso_estimado_ml'))}")
            st.write(f"Margen ML actual: {fmt_pct(row.get('margen_ml_actual'))}")
            st.write(f"Margen histórico ML 30d: {fmt_pct(row.get('margen_hist_30d'))}")
            st.write(f"Margen histórico ML 90d: {fmt_pct(row.get('margen_hist_90d'))}")
            st.write(f"Margen histórico ML total: {fmt_pct(row.get('margen_hist_total'))}")
            st.write(f"Brecha precio ML: {fmt_pct(row.get('brecha_precio_pct'))}")
            st.write(f"Brecha comercial (monto simulación maestra vs ingreso estimado ML): {fmt_pct(row.get('brecha_monto_sim_pct'))}")
        with b:
            st.markdown("#### Tienda")
            st.write(f"Precio bruto tienda: {fmt_money(row.get('precio_bruto'))}")
            st.write(f"Margen tienda actual: {fmt_pct(row.get('margen_tienda_actual'))}")
            st.write(f"Precio neto tienda: {fmt_money(row.get('precio_bruto') / 1.19 if pd.notna(row.get('precio_bruto')) else np.nan)}")
            st.write(f"Ventas tienda 30d: {fmt_money(row.get('ingresos_tienda_30d'))}")
            st.write(f"Ventas tienda 90d: {fmt_money(model['sales_windows'].get(90, pd.DataFrame()).set_index('sku').get('ingresos_tienda_90d', pd.Series()).get(sku, np.nan) if not model['sales_windows'].get(90, pd.DataFrame()).empty else np.nan)}")

        st.markdown("### Ads")
        ads_detail = build_ads_report_detail_for_sku(sku, model.get("product_ads", pd.DataFrame()), model.get("pubs", pd.DataFrame()))
        if ads_detail.empty:
            st.info("No encontré Product Ads asociados a las publicaciones de este SKU.")
        else:
            inversion_total = ads_detail["inversion_ads"].fillna(0).sum()
            ingresos_total = ads_detail["ingresos_ads"].fillna(0).sum()
            ventas_total = ads_detail["ventas_ads"].fillna(0).sum()
            acos_total = (inversion_total / ingresos_total * 100) if ingresos_total > 0 else np.nan
            roas_total = (ingresos_total / inversion_total) if inversion_total > 0 else np.nan

            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Ads en reporte", "Sí")
            m2.metric("Inversión", fmt_money(inversion_total))
            m3.metric("Ingresos ads", fmt_money(ingresos_total))
            m4.metric("ACOS", fmt_pct(acos_total))
            m5.metric("ROAS", f"{roas_total:.2f}" if pd.notna(roas_total) else "—")
            st.caption(f"Ventas por publicidad: {fmt_int(ventas_total)}")

            ads_show = ads_detail[["campana", "mlc", "estado", "inversion_ads", "ingresos_ads", "acos", "roas", "ventas_ads"]].copy()
            ads_show.columns = ["Campaña", "MLC", "Estado", "Inversión", "Ingresos", "ACOS", "ROAS", "Ventas Ads"]
            ads_show["MLC"] = ads_show["MLC"].astype(str).str.replace(r"\.0$", "", regex=True)
            ads_show["Inversión"] = ads_show["Inversión"].map(fmt_money)
            ads_show["Ingresos"] = ads_show["Ingresos"].map(fmt_money)
            ads_show["ACOS"] = ads_show["ACOS"].map(fmt_pct)
            ads_show["ROAS"] = ads_show["ROAS"].map(lambda x: f"{safe_float(x, np.nan):.2f}" if pd.notna(x) else "—")
            ads_show["Ventas Ads"] = ads_show["Ventas Ads"].map(fmt_int)
            st.dataframe(ads_show, use_container_width=True, hide_index=True, height=220)

        st.markdown("### Compras")
        ps = model["purchase_summary"]
        purchase_row = ps[ps["sku"] == sku]
        if purchase_row.empty:
            st.info("No encontré compras para este SKU.")
        else:
            pr = purchase_row.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Última compra", fmt_date(pr["ultima_fecha_compra"]))
            c2.metric("Último costo compra", fmt_money(pr["ultimo_costo_compra"]))
            c3.metric("Proveedor", pr["ultimo_proveedor"])
            c4.metric("Brecha maestra vs última compra", fmt_money(row.get("brecha_costo_clp")), fmt_pct(row.get("brecha_costo_pct")))
            hist = model["purchase_map"].get(sku, pd.DataFrame()).copy()
            if not hist.empty:
                hist_show = hist[["fecha", "proveedor", "cantidad", "precio_unitario", "documento", "folio"]].sort_values("fecha", ascending=False)
                hist_show.columns = ["Fecha", "Proveedor", "Cantidad", "Precio Unitario", "Documento", "Folio"]
                hist_show["Fecha"] = hist_show["Fecha"].map(fmt_date)
                hist_show["Precio Unitario"] = hist_show["Precio Unitario"].map(fmt_money)
                st.dataframe(hist_show, use_container_width=True, hide_index=True, height=280)

        st.markdown("### Comportamiento de venta")
        b1, b2 = st.columns(2)
        with b1:
            sw = model["sales_windows"].get(default_period, pd.DataFrame())
            srow = sw[sw["sku"] == sku]
            if srow.empty:
                st.info("No encontré ventas para este SKU en el periodo.")
            else:
                srow = srow.iloc[0]
                total_ing = safe_float(srow.get(f"ingresos_ml_{default_period}d"), 0) + safe_float(srow.get(f"ingresos_tienda_{default_period}d"), 0)
                part_ml = safe_float(srow.get(f"ingresos_ml_{default_period}d"), 0) / total_ing * 100 if total_ing > 0 else np.nan
                part_t = safe_float(srow.get(f"ingresos_tienda_{default_period}d"), 0) / total_ing * 100 if total_ing > 0 else np.nan
                st.write(f"Participación ML {default_period}d: {fmt_pct(part_ml)}")
                st.write(f"Participación tienda {default_period}d: {fmt_pct(part_t)}")
                st.write(f"Empresas {default_period}d: {fmt_pct(srow.get(f'participacion_empresa_{default_period}d'))}")
                st.write(f"Personas {default_period}d: {fmt_pct(srow.get(f'participacion_persona_{default_period}d'))}")
        with b2:
            if not srow.empty:
                st.write(f"Compra típica empresas: {fmt_int(srow.get(f'mediana_unidades_empresa_{default_period}d'))} unidades")
                st.write(f"P90 empresas: {fmt_int(srow.get(f'p90_unidades_empresa_{default_period}d'))} unidades")
                st.write(f"Compra típica personas: {fmt_int(srow.get(f'mediana_unidades_persona_{default_period}d'))} unidades")
                st.write(f"P90 personas: {fmt_int(srow.get(f'p90_unidades_persona_{default_period}d'))} unidades")

        st.markdown("### Datos de Publicación ML")
        pr = choose_primary_publication(model["pub_map"].get(sku, pd.DataFrame()))
        if pr is None:
            st.info("No encontré publicación principal para este SKU.")
        else:
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("Dimensiones", pr["dimensiones"])
            peso_real = "—"
            if pd.notna(pr.get("peso_grs", np.nan)):
                peso_g = safe_float(pr.get("peso_grs"), np.nan)
                peso_real = f"{peso_g/1000:.2f} kg" if peso_g >= 1000 else f"{peso_g:.0f} g"
            d2.metric("Peso", peso_real)
            d3.metric("Peso volumétrico", f"{safe_float(pr['peso_volumetrico_kg'], np.nan):.2f} kg" if pd.notna(pr["peso_volumetrico_kg"]) else "—")
            d4.metric("Días publicado", fmt_int(pr["dias_publicado"]))
            st.caption(f"Status: {pr['status']} | Entrega: {pr['entrega']}")

        st.markdown("### Historial de ventas")
        sales_sku = model["ventas"][model["ventas"]["sku"] == sku].copy()
        if sales_sku.empty:
            st.info("No encontré ventas para este SKU.")
        else:
            sales_sku["fecha"] = pd.to_datetime(sales_sku["fecha"], errors="coerce")
            sales_sku = sales_sku.sort_values("fecha", ascending=False)

            sales_ml = sales_sku[sales_sku["canal"] == "ML"].copy()
            sales_tienda = sales_sku[sales_sku["canal"] == "TIENDA"].copy()

            hm1, hm2, hm3, hm4 = st.columns(4)
            hm1.metric("Ventas ML totales", fmt_money(sales_ml["total_linea"].sum()))
            hm2.metric("Unidades ML", fmt_int(sales_ml["cantidad"].sum()))
            hm3.metric("Ventas tienda totales", fmt_money(sales_tienda["total_linea"].sum()))
            hm4.metric("Unidades tienda", fmt_int(sales_tienda["cantidad"].sum()))

            vv1, vv2 = st.columns(2)
            with vv1:
                st.markdown("#### Ventas Mercado Libre")
                if sales_ml.empty:
                    st.info("No encontré ventas ML para este SKU.")
                else:
                    ml_show = sales_ml[["fecha", "tipo_cliente", "cantidad", "precio_unitario", "total_linea", "cliente", "rut"]].copy()
                    ml_show.columns = ["Fecha", "Tipo cliente", "Cantidad", "Precio unitario", "Total línea", "Cliente", "RUT"]
                    ml_show["Fecha"] = ml_show["Fecha"].map(fmt_date)
                    ml_show["Precio unitario"] = ml_show["Precio unitario"].map(fmt_money)
                    ml_show["Total línea"] = ml_show["Total línea"].map(fmt_money)
                    st.dataframe(ml_show, use_container_width=True, hide_index=True, height=320)

            with vv2:
                st.markdown("#### Ventas Tienda")
                if sales_tienda.empty:
                    st.info("No encontré ventas tienda para este SKU.")
                else:
                    tienda_show = sales_tienda[["fecha", "tipo_cliente", "cantidad", "precio_unitario", "total_linea", "cliente", "rut"]].copy()
                    tienda_show.columns = ["Fecha", "Tipo cliente", "Cantidad", "Precio unitario", "Total línea", "Cliente", "RUT"]
                    tienda_show["Fecha"] = tienda_show["Fecha"].map(fmt_date)
                    tienda_show["Precio unitario"] = tienda_show["Precio unitario"].map(fmt_money)
                    tienda_show["Total línea"] = tienda_show["Total línea"].map(fmt_money)
                    st.dataframe(tienda_show, use_container_width=True, hide_index=True, height=320)

# =========================================================
# Tab 3 - Mass repricing
# =========================================================
if False:
    st.subheader("Repricing técnico basado en reportes")
    st.caption("La fuente de verdad para precio, cargos y comisión es el reporte de publicaciones ML. La maestra queda como consolidado de trabajo.")
    x1, x2, x3, x4 = st.columns(4)
    proveedor_alza_pct = x1.number_input("Simular alza proveedor %", min_value=-30.0, max_value=200.0, value=0.0, step=1.0)
    comision_extra_pct = x2.number_input("Cambio fee ML (pp)", min_value=-20.0, max_value=20.0, value=0.0, step=0.5)
    margen_obj_ml = x3.number_input("Margen objetivo ML %", min_value=0.0, max_value=80.0, value=15.0, step=0.5)
    incluir_ads = x4.selectbox("Considerar ads en cálculo", ["Sí", "No"], index=0)

    sim = action_table.copy()
    sim = sim[sim["precio_ml_actual"].notna()].copy()
    for _col in ["total_cargo_pct_ml", "ads_share_ml_pct", "costo_fijo_ml", "costo_maestra", "precio_ml_actual"]:
        if _col not in sim.columns:
            sim[_col] = np.nan
    sim["costo_simulado"] = sim["costo_maestra"] * (1 + proveedor_alza_pct / 100.0)
    sim["fee_total_sim_pct"] = sim["total_cargo_pct_ml"].fillna(0) + comision_extra_pct
    sim["ads_pct_sim"] = sim["ads_share_ml_pct"].fillna(0) if incluir_ads == "Sí" else 0.0
    sim["precio_sugerido_ml"] = sim.apply(lambda r: calc_price_for_target_ml_margin(r["costo_simulado"], r["fee_total_sim_pct"], r.get("costo_fijo_ml", 0.0), margen_obj_ml, r.get("ads_pct_sim", 0.0)), axis=1)
    sim["margen_proyectado_actual"] = sim.apply(lambda r: calc_margin_from_ml_price(r["costo_simulado"], r["precio_ml_actual"], r["fee_total_sim_pct"], r.get("costo_fijo_ml", 0.0), r.get("ads_pct_sim", 0.0)), axis=1)
    sim["margen_proyectado_sugerido"] = sim.apply(lambda r: calc_margin_from_ml_price(r["costo_simulado"], r["precio_sugerido_ml"], r["fee_total_sim_pct"], r.get("costo_fijo_ml", 0.0), r.get("ads_pct_sim", 0.0)), axis=1)
    sim["delta_precio_sugerido_pct"] = np.where(
        sim["precio_ml_actual"].notna() & (sim["precio_ml_actual"] != 0) & sim["precio_sugerido_ml"].notna(),
        ((sim["precio_sugerido_ml"] - sim["precio_ml_actual"]) / sim["precio_ml_actual"]) * 100,
        np.nan
    )
    sim["gap_margen_obj_pp"] = sim["margen_proyectado_actual"] - margen_obj_ml
    sim["decision_repricing"] = np.select(
        [
            sim["margen_proyectado_actual"].isna(),
            sim["gap_margen_obj_pp"] <= -5,
            sim["gap_margen_obj_pp"].between(-5, -1, inclusive="left"),
            sim["gap_margen_obj_pp"] >= 3,
        ],
        [
            "Sin base suficiente",
            "Subir precio urgente",
            "Subir precio / revisar costo",
            "Hay holgura",
        ],
        default="Mantener / monitorear",
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("SKUs con base ML", fmt_int(len(sim)))
    k2.metric("Subir urgente", fmt_int((sim["decision_repricing"] == "Subir precio urgente").sum()))
    k3.metric("Gap margen promedio", "—" if sim["gap_margen_obj_pp"].dropna().empty else f"{sim['gap_margen_obj_pp'].mean():.1f} pp")
    k4.metric("Δ precio sugerido promedio", "—" if sim["delta_precio_sugerido_pct"].dropna().empty else f"{sim['delta_precio_sugerido_pct'].mean():.1f}%")

    sim_show = sim[[
        "sku", "descripcion", "costo_maestra", "costo_simulado", "precio_ml_actual", "precio_sugerido_ml",
        "fee_total_sim_pct", "ads_pct_sim", "costo_fijo_ml", "margen_proyectado_actual", "margen_proyectado_sugerido",
        "delta_precio_sugerido_pct", "decision_repricing", "accion_sugerida"
    ]].copy()
    sim_show.columns = ["SKU", "Descripción", "Costo actual", "Costo simulado", "Precio ML actual", "Precio sugerido ML", "Fee ML sim %", "Ads sim %", "Costo fijo ML", "Margen proyectado actual", "Margen proyectado sugerido", "Δ precio sugerido %", "Decisión repricing", "Acción base"]
    for c in ["Costo actual", "Costo simulado", "Precio ML actual", "Precio sugerido ML", "Costo fijo ML"]:
        sim_show[c] = sim_show[c].map(fmt_money)
    for c in ["Fee ML sim %", "Ads sim %", "Margen proyectado actual", "Margen proyectado sugerido", "Δ precio sugerido %"]:
        sim_show[c] = sim_show[c].map(fmt_pct)
    st.dataframe(sim_show.sort_values(["Decisión repricing", "Δ precio sugerido %"], ascending=[True, False]), use_container_width=True, hide_index=True, height=540)


# =========================================================
# Tab 3 - Ads
# =========================================================
with tabs[2]:
    st.subheader("Módulo Ads")
    st.caption("Rentabilidad Ads calculada con ÚLTIMO COSTO + MONTO EN SIMULACIÓN neto de IVA. El precio real publicado se muestra como referencia operativa.")

    ads_table = action_table.copy()
    if ads_table.empty:
        st.info("No hay base suficiente para construir el módulo Ads.")
    else:
        active_ads = ads_table[(ads_table["ads_inversion"].fillna(0) > 0) | (ads_table["ads_ingresos"].fillna(0) > 0) | (ads_table["ads_clics"].fillna(0) > 0)].copy()
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        total_inv = active_ads["ads_inversion"].fillna(0).sum()
        total_ing = active_ads["ads_ingresos"].fillna(0).sum()
        total_clicks = active_ads["ads_clics"].fillna(0).sum()
        total_impr = active_ads["ads_impresiones"].fillna(0).sum()
        global_acos = (total_inv / total_ing * 100) if total_ing > 0 else np.nan
        global_roas = (total_ing / total_inv) if total_inv > 0 else np.nan
        k1.metric("Inversión Ads", fmt_money(total_inv))
        k2.metric("Ingresos Ads", fmt_money(total_ing))
        k3.metric("ACOS global", fmt_pct(global_acos))
        k4.metric("ROAS global", "—" if pd.isna(global_roas) else f"{global_roas:.2f}")
        k5.metric("SKUs críticos", fmt_int(int((ads_table["estado_ads"] == "CRÍTICO").sum())))
        k6.metric("SKUs oportunidad", fmt_int(int((ads_table["estado_ads"] == "OPORTUNIDAD").sum())))

        x1, x2, x3, x4 = st.columns([1.2, 1.2, 1.0, 1.2])
        state_options = ["CRÍTICO", "ALERTA", "OPORTUNIDAD", "OK", "SIN ADS", "NO USAR ADS"]
        ads_state_filter = x1.multiselect("Estado Ads", state_options, default=state_options, key="ads_module_state")
        ads_action_filter = x2.multiselect("Acción Ads", sorted([x for x in ads_table["accion_ads"].dropna().astype(str).unique().tolist() if x]), default=[], key="ads_module_action")
        ads_only_live = x3.selectbox("Cobertura", ["Todos", "Solo con datos Ads", "Solo sin Ads"], key="ads_module_coverage")
        ads_search = x4.text_input("Buscar SKU / descripción / MLC", key="ads_module_search")

        y1, y2, y3, y4 = st.columns(4)
        min_inv = float(y1.number_input("Inversión mínima", min_value=0.0, value=0.0, step=1000.0, key="ads_module_min_inv"))
        min_clicks = float(y2.number_input("Clicks mínimos", min_value=0.0, value=0.0, step=1.0, key="ads_module_min_clicks"))
        acos_view = y3.selectbox("Vista ACOS", ["Todos", "Sobre 5.09%", "Sobre ACOS máximo", "Dentro de límite"], key="ads_module_acos_view")
        sort_mode = y4.selectbox("Orden", ["Mayor criticidad", "Mayor inversión", "Peor margen con Ads", "Mayor gap ACOS", "Mejor oportunidad"], key="ads_module_sort")

        ads_work = ads_table.copy()
        if ads_state_filter:
            ads_work = ads_work[ads_work["estado_ads"].isin(ads_state_filter)]
        if ads_action_filter:
            ads_work = ads_work[ads_work["accion_ads"].isin(ads_action_filter)]
        if ads_only_live == "Solo con datos Ads":
            ads_work = ads_work[(ads_work["ads_inversion"].fillna(0) > 0) | (ads_work["ads_ingresos"].fillna(0) > 0) | (ads_work["ads_clics"].fillna(0) > 0)]
        elif ads_only_live == "Solo sin Ads":
            ads_work = ads_work[(ads_work["ads_inversion"].fillna(0) <= 0) & (ads_work["ads_ingresos"].fillna(0) <= 0) & (ads_work["ads_clics"].fillna(0) <= 0)]
        ads_work = ads_work[ads_work["ads_inversion"].fillna(0) >= min_inv]
        ads_work = ads_work[ads_work["ads_clics"].fillna(0) >= min_clicks]
        if acos_view == "Sobre 5.09%":
            ads_work = ads_work[ads_work["ads_acos"].fillna(-np.inf) > ADS_GLOBAL_ACOS_ALERT_PCT]
        elif acos_view == "Sobre ACOS máximo":
            ads_work = ads_work[ads_work["gap_acos_pct"].fillna(-np.inf) > 0]
        elif acos_view == "Dentro de límite":
            ads_work = ads_work[ads_work["gap_acos_pct"].fillna(np.inf) <= 0]
        if ads_search:
            q = ads_search.strip().lower()
            ads_work = ads_work[
                ads_work["sku"].astype(str).str.lower().str.contains(q, na=False) |
                ads_work["descripcion"].astype(str).str.lower().str.contains(q, na=False) |
                ads_work["mlc_principal"].astype(str).str.lower().str.contains(q, na=False)
            ]

        severity_rank = {"CRÍTICO": 5, "NO USAR ADS": 4, "ALERTA": 3, "OPORTUNIDAD": 2, "OK": 1, "SIN ADS": 0}
        ads_work["_ads_rank"] = ads_work["estado_ads"].map(severity_rank).fillna(0)
        sort_config = {
            "Mayor criticidad": (["_ads_rank", "ads_inversion", "gap_acos_pct"], [False, False, False]),
            "Mayor inversión": (["ads_inversion", "ads_ingresos"], [False, False]),
            "Peor margen con Ads": (["margen_ml_con_ads", "ads_inversion"], [True, False]),
            "Mayor gap ACOS": (["gap_acos_pct", "ads_inversion"], [False, False]),
            "Mejor oportunidad": (["ads_score", "ads_roas"], [False, False]),
        }
        scols, sasc = sort_config[sort_mode]
        ads_work = ads_work.sort_values(scols, ascending=sasc, na_position="last")

        ads_tabs = st.tabs(["Resumen", "Alertas", "Oportunidades", "Detalle por SKU"])

        with ads_tabs[0]:
            st.markdown("**Resumen ejecutivo Ads**")
            top_summary = ads_work[[
                "sku", "descripcion", "estado_ads", "motivo_ads", "accion_ads",
                "ads_inversion", "ads_ingresos", "ads_acos", "ads_roas",
                "margen_ads_base_pct", "margen_ml_con_ads", "acos_max_permitido_pct", "gap_acos_pct"
            ]].copy()
            top_summary.columns = [
                "SKU", "Descripción", "Estado Ads", "Motivo", "Acción Ads",
                "Inversión Ads", "Ingresos Ads", "ACOS real", "ROAS",
                "Margen base Ads", "Margen con Ads", "ACOS máximo", "Gap ACOS"
            ]
            for c in ["Inversión Ads", "Ingresos Ads"]:
                top_summary[c] = top_summary[c].map(fmt_money)
            for c in ["ACOS real", "Margen base Ads", "Margen con Ads", "ACOS máximo", "Gap ACOS"]:
                top_summary[c] = top_summary[c].map(fmt_pct)
            top_summary["ROAS"] = top_summary["ROAS"].map(lambda x: "—" if pd.isna(x) else f"{x:.2f}")
            st.dataframe(top_summary.head(200), use_container_width=True, hide_index=True, height=420)

        with ads_tabs[1]:
            st.markdown("**Bandeja de alertas Ads**")
            alerts = ads_work[ads_work["estado_ads"].isin(["CRÍTICO", "ALERTA", "NO USAR ADS"])].copy()
            if alerts.empty:
                st.success("No hay SKUs en alerta con los filtros aplicados.")
            else:
                alert_view = alerts[[
                    "sku", "descripcion", "mlc_principal", "estado_ads", "motivo_ads", "accion_ads",
                    "ads_inversion", "ads_ingresos", "ads_clics", "ads_impresiones",
                    "ads_acos", "ads_roas", "margen_ads_base_pct", "margen_ml_con_ads",
                    "acos_max_permitido_pct", "gap_acos_pct", "precio_ml_actual", "monto_sim", "monto_sim_neto"
                ]].copy()
                alert_view.columns = [
                    "SKU", "Descripción", "MLC", "Estado Ads", "Motivo", "Acción Ads",
                    "Inversión Ads", "Ingresos Ads", "Clicks", "Impresiones",
                    "ACOS real", "ROAS", "Margen base Ads", "Margen con Ads",
                    "ACOS máximo", "Gap ACOS", "Precio real ML", "Monto simulación", "Monto sim neto"
                ]
                for c in ["Inversión Ads", "Ingresos Ads", "Precio real ML", "Monto simulación", "Monto sim neto"]:
                    alert_view[c] = alert_view[c].map(fmt_money)
                for c in ["ACOS real", "Margen base Ads", "Margen con Ads", "ACOS máximo", "Gap ACOS"]:
                    alert_view[c] = alert_view[c].map(fmt_pct)
                alert_view["ROAS"] = alert_view["ROAS"].map(lambda x: "—" if pd.isna(x) else f"{x:.2f}")
                alert_view["Clicks"] = alert_view["Clicks"].map(fmt_int)
                alert_view["Impresiones"] = alert_view["Impresiones"].map(fmt_int)
                st.dataframe(alert_view.head(300), use_container_width=True, hide_index=True, height=460)

        with ads_tabs[2]:
            st.markdown("**Oportunidades de escala / prueba**")
            opp = ads_work[ads_work["estado_ads"].isin(["OPORTUNIDAD", "SIN ADS"])].copy()
            opp = opp.sort_values(["estado_ads", "ads_score", "margen_ads_base_pct"], ascending=[True, False, False], na_position="last")
            if opp.empty:
                st.info("No encontré oportunidades con los filtros aplicados.")
            else:
                opp_view = opp[[
                    "sku", "descripcion", "estado_ads", "motivo_ads", "accion_ads",
                    "margen_ads_base_pct", "margen_ml_con_ads", "ads_acos", "ads_roas",
                    "ads_inversion", "ads_ingresos", "precio_ml_actual", "monto_sim"
                ]].copy()
                opp_view.columns = [
                    "SKU", "Descripción", "Estado Ads", "Motivo", "Acción Ads",
                    "Margen base Ads", "Margen con Ads", "ACOS real", "ROAS",
                    "Inversión Ads", "Ingresos Ads", "Precio real ML", "Monto simulación"
                ]
                for c in ["Inversión Ads", "Ingresos Ads", "Precio real ML", "Monto simulación"]:
                    opp_view[c] = opp_view[c].map(fmt_money)
                for c in ["Margen base Ads", "Margen con Ads", "ACOS real"]:
                    opp_view[c] = opp_view[c].map(fmt_pct)
                opp_view["ROAS"] = opp_view["ROAS"].map(lambda x: "—" if pd.isna(x) else f"{x:.2f}")
                st.dataframe(opp_view.head(300), use_container_width=True, hide_index=True, height=420)

        with ads_tabs[3]:
            sku_labels_ads = [f"{sku} — {model['sku_desc'].get(sku, '')}" for sku in ads_work["sku"].dropna().astype(str).tolist()]
            if not sku_labels_ads:
                st.info("No hay SKUs disponibles con los filtros actuales.")
            else:
                default_sku_ads = st.session_state.get("selected_sku", ads_work.iloc[0]["sku"])
                default_label_ads = f"{default_sku_ads} — {model['sku_desc'].get(default_sku_ads, '')}"
                index_ads = sku_labels_ads.index(default_label_ads) if default_label_ads in sku_labels_ads else 0
                selected_label_ads = st.selectbox("SKU Ads", sku_labels_ads, index=index_ads, key="ads_detail_sku")
                sku_ads = selected_label_ads.split(" — ")[0]
                st.session_state.selected_sku = sku_ads

                detail = ads_table[ads_table["sku"] == sku_ads].copy()
                if detail.empty:
                    st.info("No encontré detalle para ese SKU.")
                else:
                    drow = detail.iloc[0]
                    d1, d2, d3, d4, d5, d6 = st.columns(6)
                    d1.metric("Estado Ads", str(drow.get("estado_ads", "—")))
                    d2.metric("Acción Ads", str(drow.get("accion_ads", "—")))
                    d3.metric("Margen base Ads", fmt_pct(drow.get("margen_ads_base_pct")))
                    d4.metric("Margen con Ads", fmt_pct(drow.get("margen_ml_con_ads")))
                    d5.metric("ACOS real / máx", f"{fmt_pct(drow.get('ads_acos'))} / {fmt_pct(drow.get('acos_max_permitido_pct'))}")
                    d6.metric("ROAS", "—" if pd.isna(safe_float(drow.get("ads_roas"), np.nan)) else f"{safe_float(drow.get('ads_roas')):.2f}")

                    st.write(f"**Motivo:** {drow.get('motivo_ads', '—')}")
                    st.write(f"**MLC principal:** {drow.get('mlc_principal', '—')}")
                    st.write(f"**Precio real ML:** {fmt_money(drow.get('precio_ml_actual'))}")
                    st.write(f"**Monto simulación:** {fmt_money(drow.get('monto_sim'))}")
                    st.write(f"**Monto simulación neto:** {fmt_money(drow.get('monto_sim_neto'))}")
                    st.write(f"**Margen real reportado ML:** {fmt_pct(drow.get('margen_ml_reportado'))}")

                    report_detail = build_ads_report_detail_for_sku(sku_ads, model.get("product_ads"), model.get("pubs"))
                    if report_detail.empty:
                        st.info("No encontré campañas/anuncios Ads para este SKU en el reporte cargado.")
                    else:
                        show_detail = report_detail.copy()
                        show_detail.columns = [
                            "Campaña", "MLC", "Título", "Estado", "Inversión Ads", "Ingresos Ads", "ACOS", "ROAS", "Ventas Ads", "Impresiones", "Clicks"
                        ]
                        for c in ["Inversión Ads", "Ingresos Ads"]:
                            show_detail[c] = show_detail[c].map(fmt_money)
                        for c in ["ACOS"]:
                            show_detail[c] = show_detail[c].map(fmt_pct)
                        show_detail["ROAS"] = show_detail["ROAS"].map(lambda x: "—" if pd.isna(x) else f"{x:.2f}")
                        show_detail["Ventas Ads"] = show_detail["Ventas Ads"].map(fmt_int)
                        show_detail["Impresiones"] = show_detail["Impresiones"].map(fmt_int)
                        show_detail["Clicks"] = show_detail["Clicks"].map(fmt_int)
                        st.dataframe(show_detail, use_container_width=True, hide_index=True, height=320)

# =========================================================
# Tab 4 - Promotions
# =========================================================
if False:
    st.subheader("Operador de promociones")
    promos_all = ensure_promos_schema(model.get("promos", pd.DataFrame()))
    if promos_all.empty:
        st.info("No encontré promos en la maestra.")
    else:
        left, right = st.columns([1, 2])
        with left:
            status_options = [
                "Vencidas",
                "Vencen hoy",
                "Vencen mañana",
                "Vencen pasado mañana",
                "Vencen en 7 días",
                "Vencen en 15 días",
                "Vencen en 1 mes",
            ]
            status_filter = st.multiselect(
                "Estado",
                status_options,
                default=st.session_state.get("promo_status_filter_v3", ["Vencidas", "Vencen hoy"]),
                key="promo_status_filter_v3",
            )
            text_filter = st.text_input("Buscar por SKU / descripción / MLC", key="promo_search_v3")
            promos = promos_all.copy()
            if status_filter:
                promos = promos[promos["status"].isin(status_filter)]
            else:
                promos = promos.iloc[0:0]
            if text_filter:
                q = text_filter.lower().strip()
                promos = promos[
                    promos["sku"].astype(str).str.lower().str.contains(q, na=False) |
                    promos["descripcion"].astype(str).str.lower().str.contains(q, na=False) |
                    promos["mlc"].astype(str).str.lower().str.contains(q, na=False)
                ]
            st.caption(f"Mostrando {len(promos)} promo(s) filtradas")
            mass_date = st.date_input("Cambio masivo de fecha", value=None, format="DD/MM/YYYY", key="promo_mass_date_v3")
            if st.button("Aplicar fecha masiva a filtradas", key="promo_mass_apply_v3"):
                if mass_date and not promos.empty:
                    for _, p in promos.iterrows():
                        update_single_promo(model, int(p["master_index"]), int(p["slot"]), p["precio_b2c"], mass_date, p["comentario"])
                    persist_current_master_workbook(model, "promociones actualizadas masivamente")
                    st.success("Fecha actualizada y compartida en vivo.")
                    st.rerun()

        with right:
            if promos.empty:
                st.info("No hay promos para esos estados/filtros.")
            else:
                cols = st.columns(4)
                for i, (_, p) in enumerate(promos.sort_values(["status_order", "sku", "slot"]).iterrows()):
                    with cols[i % 4]:
                        with st.container(border=True):
                            st.markdown(f"**{p['sku']}**")
                            st.caption(str(p["descripcion"])[:55])
                            st.write(f"`{p['mlc'] or '—'}`")
                            st.write(fmt_date(p["fecha_venci"]))
                            st.write(p["status"])
                            if st.button("Abrir", key=f"open_promo_{p['master_index']}_{p['slot']}"):
                                st.session_state.edit_target_v3 = (int(p["master_index"]), int(p["slot"]))
                                st.rerun()

        if "edit_target_v3" in st.session_state:
            master_index, slot = st.session_state.edit_target_v3
            current = model["promos"][
                (model["promos"]["master_index"] == master_index) &
                (model["promos"]["slot"] == slot)
            ]
            if not current.empty:
                cp = current.iloc[0]
                @st.dialog("Editar promoción")
                def edit_promo_dialog():
                    st.write(f"**SKU:** {cp['sku']}")
                    st.write(f"**Descripción:** {cp['descripcion']}")
                    st.write(f"**MLC:** {cp['mlc'] or '—'}")
                    current_date = cp["fecha_venci"].date() if pd.notna(cp["fecha_venci"]) else None
                    new_date = st.date_input("Fecha venci", value=current_date, format="DD/MM/YYYY", key="promo_edit_date_v3")
                    with st.expander("Campos secundarios"):
                        new_price = st.number_input("Precio B2C", min_value=0.0, value=float(safe_float(cp["precio_b2c"], 0.0)), step=100.0, key="promo_edit_price_v3")
                        new_comment = st.text_input("Comentario", value=str(cp["comentario"]) if pd.notna(cp["comentario"]) else "", key="promo_edit_comment_v3")
                    if st.button("Guardar cambios", key="promo_edit_save_v3"):
                        update_single_promo(model, master_index, slot, new_price, new_date, new_comment)
                        persist_current_master_workbook(model, f"promo {cp['sku']} slot {slot} actualizada")
                        del st.session_state["edit_target_v3"]
                        st.success("Promoción actualizada y compartida en vivo.")
                        st.rerun()
                    if st.button("Cerrar", key="promo_edit_close_v3"):
                        del st.session_state["edit_target_v3"]
                        st.rerun()
                edit_promo_dialog()

if False:
    st.subheader("Historial / snapshots")
    st.write("Los snapshots se guardan automáticamente cuando cambia la carga o cambia el estado consolidado del sistema.")
    runs = list_runs()
    if runs.empty:
        st.info("Aún no hay snapshots guardados.")
    else:
        st.dataframe(runs, use_container_width=True, hide_index=True, height=240)
        st.caption("La primera corrida se interpreta como brecha inicial entre maestra y realidad actual; las siguientes permiten trazabilidad y comparación.")

    st.markdown("### Historial de archivos fuente")
    source_events = list_source_file_events()
    if source_events.empty:
        st.info("Aún no hay reemplazos de archivos registrados.")
    else:
        show = source_events.copy()
        rename_map = {
            "created_at": "Fecha",
            "file_key": "Tipo",
            "active_filename": "Activo",
            "archived_filename": "Archivado",
            "original_filename": "Nombre original",
            "file_sig": "Firma",
            "file_size": "Tamaño",
        }
        show = show.rename(columns=rename_map)
        show["Tipo"] = show["Tipo"].map(lambda x: FILE_SPECS.get(x, {}).get("label", x))
        st.dataframe(show[["Fecha", "Tipo", "Activo", "Archivado", "Nombre original", "Tamaño"]], use_container_width=True, hide_index=True, height=260)

if False:
    st.subheader("Descargar maestra actualizada")
    wb = model["wb"]
    download_bytes = build_download_bytes(model["master"], model["rel"], wb["file_bytes"], wb["maestra_name"], wb["rel_name"])
    st.download_button(
        "Descargar workbook actualizado",
        data=download_bytes,
        file_name=f"maestra_actualizada_{date.today().isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.caption("Este archivo conserva las hojas originales y reemplaza la maestra / relámpago con el estado actual en memoria.")

