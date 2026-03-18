import json
import math
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

# =========================
# CONFIG
# =========================
URL_LOGIN = "https://www.kameone.cl"
URL_REPORTE_ARTICULOS = "https://www.kameone.cl/Reporte/InventarioBodega"
URL_EMISION_PACKS = "https://www.kameone.cl/Reporte/EmisionPackStockDisponible?idArticuloPack=0&idBodega=7823&idFamilia=0"

USER = "gsifontes@faurora.cl"
PASSWORD = "Dimasoft0858"

DOWNLOAD_FOLDER = r"C:\Users\VNP-4\Downloads"
BODEGA_OBJETIVO = "BODEGA UNIVERSAL"
BRANCH_NAME = "main"

TIMEOUT = 25
MIN_ROWS_EXPORTADAS = 4000
MIN_PACK_ROWS = 500
SLEEP_SECONDS = 300
LOCK_FILE_NAME = ".robot_kame_stock.lock"

REPO_DIR = Path(__file__).resolve().parent
JSON_SALIDA = REPO_DIR / "stock_kame.json"
LOCK_FILE = REPO_DIR / LOCK_FILE_NAME


# =========================
# UTIL
# =========================
def debug(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def wait(driver, seconds=TIMEOUT):
    return WebDriverWait(driver, seconds)


class SingleInstanceLock:
    def __init__(self, path: Path):
        self.path = path
        self.fd = None

    def acquire(self):
        try:
            self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self.fd, str(os.getpid()).encode("utf-8"))
            os.fsync(self.fd)
        except FileExistsError:
            try:
                pid = self.path.read_text(encoding="utf-8").strip()
            except Exception:
                pid = "desconocido"
            raise RuntimeError(
                f"Ya existe otro proceso del bot corriendo o quedó un lock colgado: {self.path} (PID: {pid})."
            )

    def release(self):
        try:
            if self.fd is not None:
                os.close(self.fd)
        except Exception:
            pass
        try:
            if self.path.exists():
                self.path.unlink()
        except Exception:
            pass


# =========================
# DRIVER
# =========================
def iniciar_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1600,1000")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    prefs = {
        "download.default_directory": DOWNLOAD_FOLDER,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


# =========================
# ARCHIVOS / DESCARGAS
# =========================
def listar_descargas():
    carpeta = Path(DOWNLOAD_FOLDER)
    if not carpeta.exists():
        raise FileNotFoundError(f"No existe la carpeta de descargas: {DOWNLOAD_FOLDER}")
    return {p.name for p in carpeta.iterdir() if p.is_file()}


def esperar_archivo_nuevo(antes, timeout=90, prefijos_aceptados=None):
    carpeta = Path(DOWNLOAD_FOLDER)
    fin = time.time() + timeout
    prefijos_aceptados = [p.lower() for p in (prefijos_aceptados or [])]

    while time.time() < fin:
        actuales = {p.name for p in carpeta.iterdir() if p.is_file()}
        nuevos = actuales - antes

        xlsx_nuevos = []
        for n in nuevos:
            low = n.lower()
            if not low.endswith(".xlsx"):
                continue
            if low.endswith(".crdownload"):
                continue
            if prefijos_aceptados and not any(low.startswith(p) for p in prefijos_aceptados):
                continue
            xlsx_nuevos.append(n)

        if xlsx_nuevos:
            rutas = [carpeta / n for n in xlsx_nuevos]
            rutas.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return str(rutas[0])

        time.sleep(2)

    return None


def borrar_archivo(ruta):
    try:
        if ruta and os.path.exists(ruta):
            os.remove(ruta)
            debug(f"Archivo eliminado: {ruta}")
    except Exception as e:
        debug(f"No se pudo borrar archivo {ruta}: {e}")


def borrar_descargas_stock():
    """
    Limpia Excels temporales del bot para que no se acumulen en Descargas.
    """
    carpeta = Path(DOWNLOAD_FOLDER)
    if not carpeta.exists():
        return

    patrones = [
        "inventariobodega_*.xlsx",
        "informestockpacks*.xlsx",
        "informestockpack*.xlsx",
    ]

    for patron in patrones:
        for archivo in carpeta.glob(patron):
            borrar_archivo(str(archivo))


# =========================
# HELPERS UI
# =========================
def click_safe(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    time.sleep(0.4)
    driver.execute_script("arguments[0].click();", element)


def escribir_safe(element, texto):
    element.clear()
    time.sleep(0.2)
    element.send_keys(texto)


# =========================
# LOGIN / REPORTE
# =========================
def login(driver):
    debug("Abriendo login...")
    driver.get(URL_LOGIN)

    user_input = wait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, "//input[@type='text' or @type='email']"))
    )
    pass_input = wait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))
    )

    debug("Ingresando credenciales...")
    escribir_safe(user_input, USER)
    escribir_safe(pass_input, PASSWORD)

    try:
        btn = driver.find_element(By.XPATH, "//button[contains(., 'Acceder')]")
        click_safe(driver, btn)
    except NoSuchElementException:
        pass_input.send_keys(Keys.ENTER)

    wait(driver, 25).until_not(
        EC.presence_of_element_located((By.XPATH, "//button[contains(., 'Acceder')]"))
    )
    debug("Login OK.")


def abrir_reporte_articulos(driver):
    debug("Abriendo reporte de artículos...")
    driver.get(URL_REPORTE_ARTICULOS)

    wait(driver, 25).until(
        EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Inventario por Bodega')]")),
            EC.presence_of_element_located((By.XPATH, "//button[contains(., 'GENERAR EXCEL')]")),
        )
    )
    debug("Reporte artículos abierto.")


def seleccionar_bodega(driver, nombre_bodega=BODEGA_OBJETIVO):
    debug(f"Seleccionando bodega: {nombre_bodega}")

    selects = driver.find_elements(By.TAG_NAME, "select")
    for sel in selects:
        try:
            opciones = [o.text.strip() for o in Select(sel).options]
            if nombre_bodega in opciones:
                Select(sel).select_by_visible_text(nombre_bodega)
                debug("Bodega seleccionada por <select>.")
                return
        except Exception:
            pass

    raise RuntimeError(f"No se pudo seleccionar la bodega '{nombre_bodega}'.")


def activar_solo_con_saldo(driver):
    debug("Verificando switch 'Desplegar sólo artículos con saldo'...")

    try:
        on_el = driver.find_element(
            By.XPATH,
            "//*[contains(., 'Desplegar sólo artículos con saldo')]/following::*[contains(., 'ON')][1]"
        )
        if on_el:
            debug("Switch ya está en ON.")
            return
    except Exception:
        pass

    try:
        label = driver.find_element(By.XPATH, "//*[contains(., 'Desplegar sólo artículos con saldo')]")
        switch = label.find_element(
            By.XPATH,
            "./following::*[contains(@class,'switch') or contains(@class,'toggle') or self::label][1]"
        )
        click_safe(driver, switch)
        time.sleep(1)
        debug("Switch activado.")
    except Exception:
        debug("No pude tocar el switch. Sigo con el estado actual.")


def generar_excel_articulos(driver):
    debug("Presionando GENERAR EXCEL artículos...")
    antes = listar_descargas()

    btn = wait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'GENERAR EXCEL')]"))
    )
    click_safe(driver, btn)
    return antes


def abrir_campana(driver):
    campana = wait(driver, 20).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//i[contains(@class,'bell')]/ancestor::*[self::a or self::li or self::button][1]")
        )
    )
    click_safe(driver, campana)
    time.sleep(1.5)


def esperar_notificacion_lista_y_descargar(driver, timeout=120):
    debug("Esperando notificación lista...")
    fin = time.time() + timeout

    while time.time() < fin:
        abrir_campana(driver)

        links = driver.find_elements(
            By.XPATH,
            "//a[contains(translate(., 'ÁÍ', 'AI'), 'aqui') or contains(translate(., 'ÁÍ', 'AI'), 'AQUI')]"
        )

        if links:
            for link in links:
                try:
                    texto_bloque = link.find_element(By.XPATH, "./ancestor::li[1]").text.lower()
                except Exception:
                    texto_bloque = link.text.lower()

                if "inventario" in texto_bloque or "bodega" in texto_bloque or "excel" in texto_bloque:
                    debug("Notificación encontrada. Descargando archivo artículos...")
                    click_safe(driver, link)
                    return True

            debug("Hay link 'aqui', pero no pude validar contexto correcto.")
            return False

        debug("Todavía no aparece la descarga. Reintentando...")
        time.sleep(4)

    return False


def abrir_reporte_packs(driver):
    debug("Abriendo reporte packs directo...")
    driver.get(URL_EMISION_PACKS)

    wait(driver, 30).until(
        EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//*[contains(., 'INFORME STOCK DE PACKS')]")),
            EC.element_to_be_clickable((By.XPATH, "//*[self::button or self::a][contains(., 'Exportar a Excel')]")),
        )
    )
    debug("Reporte packs abierto.")


def descargar_excel_packs(driver):
    debug("Presionando Exportar a Excel de packs...")
    antes = listar_descargas()

    btn = wait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//*[self::button or self::a][contains(., 'Exportar a Excel')]"))
    )
    click_safe(driver, btn)

    archivo = esperar_archivo_nuevo(
        antes,
        timeout=90,
        prefijos_aceptados=["informestockpacks", "informestockpack"]
    )
    if not archivo:
        raise RuntimeError("No se detectó el Excel de packs en Descargas.")
    debug(f"Archivo packs final: {archivo}")
    return archivo


# =========================
# EXCEL -> PAYLOAD
# =========================
def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def buscar_columna_exacta(df: pd.DataFrame, nombre_exacto: str) -> str:
    for c in df.columns:
        if str(c).strip().lower() == nombre_exacto.strip().lower():
            return c
    raise KeyError(f"No se encontró la columna exacta '{nombre_exacto}'. Columnas reales: {list(df.columns)}")


def limpiar_sku(valor) -> str:
    if pd.isna(valor):
        return ""
    s = str(valor).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s.strip()


def limpiar_numero(valor):
    if pd.isna(valor):
        return 0

    if isinstance(valor, (int, float)):
        if isinstance(valor, float) and math.isnan(valor):
            return 0
        return int(valor) if float(valor).is_integer() else float(valor)

    s = str(valor).strip()
    if not s:
        return 0

    s = s.replace(" ", "").replace("\xa0", "")
    s = re.sub(r"[^0-9,.\-]", "", s)

    if not s:
        return 0

    tiene_punto = "." in s
    tiene_coma = "," in s

    if tiene_punto and tiene_coma:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif tiene_coma:
        partes = s.split(",")
        if len(partes) == 2 and len(partes[1]) in (1, 2):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif tiene_punto:
        partes = s.split(".")
        if len(partes) == 2 and len(partes[1]) in (1, 2):
            pass
        else:
            if all(parte.isdigit() for parte in partes if parte != ""):
                if all(len(parte) == 3 for parte in partes[1:]):
                    s = "".join(partes)

    try:
        num = float(s)
        return int(num) if num.is_integer() else num
    except Exception:
        return 0


def excel_a_payload_stock_articulos(ruta_excel: str, bodega: str) -> dict:
    nombre_archivo = Path(ruta_excel).name.lower()

    if not nombre_archivo.startswith("inventariobodega_"):
        raise ValueError(f"Archivo descargado incorrecto: {nombre_archivo}")

    debug(f"Leyendo Excel artículos: {ruta_excel}")
    df = pd.read_excel(ruta_excel)
    df = normalizar_columnas(df)

    col_sku = buscar_columna_exacta(df, "SKU")
    col_saldo = buscar_columna_exacta(df, "Saldo")
    col_bodega = buscar_columna_exacta(df, "Bodega")

    debug(f"Columna SKU artículos: {col_sku}")
    debug(f"Columna Saldo artículos: {col_saldo}")
    debug(f"Columna Bodega artículos: {col_bodega}")

    df = df[df[col_bodega].astype(str).str.strip().str.upper() == bodega.strip().upper()].copy()

    stock_map = {}
    filas_totales = len(df)
    filas_validas = 0

    for _, row in df.iterrows():
        sku = limpiar_sku(row.get(col_sku))
        if not sku:
            continue

        saldo = limpiar_numero(row.get(col_saldo))
        stock_map[sku] = saldo
        filas_validas += 1

    return {
        "source_file": Path(ruta_excel).name,
        "bodega": bodega,
        "rows_read": int(filas_totales),
        "rows_exported": int(filas_validas),
        "stock": stock_map,
    }


def excel_a_payload_stock_packs(ruta_excel: str, bodega: str) -> dict:
    nombre_archivo = Path(ruta_excel).name
    debug(f"Leyendo Excel packs: {ruta_excel}")

    # El Excel de packs tiene 3 filas superiores y el header real en la fila 4
    df = pd.read_excel(ruta_excel, header=3)
    df = normalizar_columnas(df)

    if len(df.columns) < 4:
        raise ValueError(f"Excel packs con estructura inesperada. Columnas: {list(df.columns)}")

    col_nombre = df.columns[0]
    col_sku = df.columns[1]
    col_bodega = df.columns[2]
    col_saldo = df.columns[3]

    debug(f"Columnas packs: nombre={col_nombre}, sku={col_sku}, bodega={col_bodega}, saldo={col_saldo}")

    stock_map = {}
    filas_validas = 0

    for _, row in df.iterrows():
        nombre = str(row.get(col_nombre, "")).strip()
        sku = limpiar_sku(row.get(col_sku))
        bodega_row = str(row.get(col_bodega, "")).strip().upper()

        if not sku or not bodega_row:
            continue
        if bodega_row != bodega.strip().upper():
            continue
        if nombre.upper().startswith("TOTAL SALDO"):
            continue
        if nombre.upper() == "NOMBRE PACK":
            continue

        saldo = limpiar_numero(row.get(col_saldo))
        stock_map[sku] = saldo
        filas_validas += 1

    return {
        "source_file_packs": nombre_archivo,
        "bodega": bodega,
        "rows_exported_packs": int(filas_validas),
        "stock_packs": stock_map,
    }


def combinar_stocks(payload_articulos: dict, payload_packs: dict) -> dict:
    stock_final = dict(payload_articulos["stock"])
    stock_final.update(payload_packs["stock_packs"])

    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_file": payload_articulos["source_file"],
        "source_file_packs": payload_packs["source_file_packs"],
        "bodega": payload_articulos["bodega"],
        "rows_read": int(payload_articulos["rows_read"]),
        "rows_exported": int(payload_articulos["rows_exported"]),
        "rows_exported_packs": int(payload_packs["rows_exported_packs"]),
        "stock": stock_final,
    }


# =========================
# VALIDACIONES
# =========================
def validar_payload_stock(payload: dict):
    if not isinstance(payload, dict):
        raise ValueError("Payload inválido: no es dict.")

    requeridos = ["updated_at", "source_file", "source_file_packs", "bodega", "rows_read", "rows_exported", "rows_exported_packs", "stock"]
    for k in requeridos:
        if k not in payload:
            raise ValueError(f"Payload inválido: falta clave '{k}'.")

    source_file = str(payload.get("source_file", "")).lower()
    if not source_file.startswith("inventariobodega_"):
        raise ValueError(f"Archivo fuente artículos inválido: {source_file}")

    if payload["bodega"] != BODEGA_OBJETIVO:
        raise ValueError(f"Bodega inesperada: {payload['bodega']}")

    rows_exported = payload.get("rows_exported", 0)
    if not isinstance(rows_exported, int):
        raise ValueError("rows_exported inválido.")
    if rows_exported < MIN_ROWS_EXPORTADAS:
        raise ValueError(
            f"Protección activada artículos: rows_exported={rows_exported} menor al mínimo permitido {MIN_ROWS_EXPORTADAS}."
        )

    rows_exported_packs = payload.get("rows_exported_packs", 0)
    if not isinstance(rows_exported_packs, int):
        raise ValueError("rows_exported_packs inválido.")
    if rows_exported_packs < MIN_PACK_ROWS:
        raise ValueError(
            f"Protección activada packs: rows_exported_packs={rows_exported_packs} menor al mínimo permitido {MIN_PACK_ROWS}."
        )

    stock = payload.get("stock")
    if not isinstance(stock, dict):
        raise ValueError("La clave 'stock' no es dict.")
    if len(stock) < MIN_ROWS_EXPORTADAS:
        raise ValueError(
            f"Protección activada: stock combinado tiene {len(stock)} SKUs, menor al mínimo permitido {MIN_ROWS_EXPORTADAS}."
        )


def cargar_json_existente(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def payload_equivalente_sin_fecha(a, b) -> bool:
    if not a or not b:
        return False

    a_cmp = dict(a)
    b_cmp = dict(b)
    a_cmp.pop("updated_at", None)
    b_cmp.pop("updated_at", None)
    return a_cmp == b_cmp


def guardar_json(payload: dict, path: Path):
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    debug(f"JSON guardado en: {path}")


# =========================
# GIT
# =========================
def run_git(args, cwd: Path, allow_fail: bool = False) -> str:
    cmd = ["git"] + list(args)
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()

    if result.returncode != 0 and not allow_fail:
        raise RuntimeError(
            f"Git falló: {' '.join(cmd)}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )

    return stdout if stdout else stderr


def validar_repo_git(repo_dir: Path):
    if not repo_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta repo: {repo_dir}")
    if not (repo_dir / ".git").exists():
        raise FileNotFoundError(f"La carpeta actual no parece repo git: {repo_dir}")


def git_sync_hard(repo_dir: Path, branch_name: str):
    debug("Git sync: fetch origin...")
    run_git(["fetch", "origin"], repo_dir)

    debug(f"Git sync: checkout {branch_name}...")
    run_git(["checkout", branch_name], repo_dir)

    debug(f"Git sync: reset --hard origin/{branch_name} ...")
    run_git(["reset", "--hard", f"origin/{branch_name}"], repo_dir)

    try:
        lock = repo_dir / LOCK_FILE_NAME
        if lock.exists():
            lock.unlink()
    except Exception:
        pass


def git_commit_y_push_resiliente(repo_dir: Path, archivo_relativo: str, payload_nuevo: dict, branch_name: str):
    debug("Validando repo git...")
    validar_repo_git(repo_dir)

    commit_msg = f"update stock kame {payload_nuevo['bodega']} {payload_nuevo['updated_at']}"

    debug("git add...")
    run_git(["add", archivo_relativo], repo_dir)

    diff_cached = run_git(["diff", "--cached", "--name-only"], repo_dir)
    staged_files = [x.strip() for x in diff_cached.splitlines() if x.strip()]

    if archivo_relativo not in staged_files:
        debug("No hay cambios staged para stock_kame.json. No se hace commit.")
        return False

    debug("git commit...")
    run_git(["commit", "-m", commit_msg], repo_dir)

    debug(f"git push origin {branch_name} ...")
    try:
        run_git(["push", "origin", branch_name], repo_dir)
        return True
    except Exception as e:
        msg = str(e).lower()
        if ("non-fast-forward" not in msg) and ("fetch first" not in msg) and ("failed to push some refs" not in msg):
            raise

        debug("Push rechazado por remoto. Re-sincronizando y reintentando una vez...")

        git_sync_hard(repo_dir, branch_name)

        payload_remoto = cargar_json_existente(JSON_SALIDA)
        if payload_equivalente_sin_fecha(payload_remoto, payload_nuevo):
            debug("Después de sincronizar, el JSON remoto ya equivale al nuevo. No hace falta push.")
            return False

        guardar_json(payload_nuevo, JSON_SALIDA)

        debug("git add (reintento)...")
        run_git(["add", archivo_relativo], repo_dir)

        diff_cached_2 = run_git(["diff", "--cached", "--name-only"], repo_dir)
        staged_files_2 = [x.strip() for x in diff_cached_2.splitlines() if x.strip()]
        if archivo_relativo not in staged_files_2:
            debug("Tras reintento no hubo cambios staged. No se hace push.")
            return False

        debug("git commit (reintento)...")
        run_git(["commit", "-m", commit_msg + " retry"], repo_dir)

        debug("git push final...")
        run_git(["push", "origin", branch_name], repo_dir)
        return True


# =========================
# CICLO PRINCIPAL
# =========================
def ejecutar_ciclo():
    driver = None
    try:
        debug(f"Repo local actual: {REPO_DIR}")
        debug(f"JSON salida: {JSON_SALIDA}")

        validar_repo_git(REPO_DIR)
        git_sync_hard(REPO_DIR, BRANCH_NAME)

        driver = iniciar_driver()
        login(driver)

        debug("=== BLOQUE 1: ARTÍCULOS UNITARIOS ===")
        abrir_reporte_articulos(driver)
        seleccionar_bodega(driver, BODEGA_OBJETIVO)
        activar_solo_con_saldo(driver)

        archivos_antes = generar_excel_articulos(driver)

        ok = esperar_notificacion_lista_y_descargar(driver, timeout=120)
        if not ok:
            raise RuntimeError("No apareció la notificación correcta lista para descargar artículos.")

        archivo_articulos = esperar_archivo_nuevo(
            archivos_antes,
            timeout=90,
            prefijos_aceptados=["inventariobodega_"]
        )
        if not archivo_articulos:
            raise RuntimeError("No se detectó un inventariobodega_*.xlsx nuevo en Descargas.")

        debug("OK - Descarga artículos completada")
        debug(f"Archivo artículos: {archivo_articulos}")
        payload_articulos = excel_a_payload_stock_articulos(
            ruta_excel=archivo_articulos,
            bodega=BODEGA_OBJETIVO
        )
        borrar_archivo(archivo_articulos)

        debug("=== BLOQUE 2: STOCK PACKS ===")
        abrir_reporte_packs(driver)
        archivo_packs = descargar_excel_packs(driver)
        payload_packs = excel_a_payload_stock_packs(
            ruta_excel=archivo_packs,
            bodega=BODEGA_OBJETIVO
        )
        borrar_archivo(archivo_packs)
        debug(f"Packs detectados: {payload_packs['rows_exported_packs']}")

        payload_nuevo = combinar_stocks(payload_articulos, payload_packs)
        validar_payload_stock(payload_nuevo)

        payload_anterior = cargar_json_existente(JSON_SALIDA)

        if payload_equivalente_sin_fecha(payload_anterior, payload_nuevo):
            debug("SIN CAMBIOS - El stock combinado es equivalente al actual.")
            debug("No se sobrescribe ni se hace push.")
            return

        guardar_json(payload_nuevo, JSON_SALIDA)
        borrar_descargas_stock()

        debug("OK - JSON combinado generado")
        debug(f"JSON final: {JSON_SALIDA}")
        debug(f"Bodega: {payload_nuevo['bodega']}")
        debug(f"Rows artículos: {payload_nuevo['rows_exported']}")
        debug(f"Rows packs: {payload_nuevo['rows_exported_packs']}")
        debug(f"SKUs totales: {len(payload_nuevo['stock'])}")

        pushed = git_commit_y_push_resiliente(
            repo_dir=REPO_DIR,
            archivo_relativo="stock_kame.json",
            payload_nuevo=payload_nuevo,
            branch_name=BRANCH_NAME
        )

        if pushed:
            debug("OK - Git actualizado. Commit y push realizados correctamente.")
        else:
            debug("SIN CAMBIOS GIT - No había diferencias reales para subir o remoto ya tenía el mismo contenido.")

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    lock = SingleInstanceLock(LOCK_FILE)
    try:
        lock.acquire()
        debug("Lock adquirido. Bot iniciado.")
        while True:
            try:
                print("\n" + "=" * 70, flush=True)
                print("INICIO CICLO:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), flush=True)
                ejecutar_ciclo()
                print("FIN CICLO OK", flush=True)
            except KeyboardInterrupt:
                print("\nProceso detenido manualmente por el usuario.", flush=True)
                break
            except Exception as e:
                print(f"\nFIN CICLO CON ERROR: {e}", flush=True)

            print(f"\nEsperando {SLEEP_SECONDS // 60} minutos para el próximo ciclo...", flush=True)
            time.sleep(SLEEP_SECONDS)
    finally:
        lock.release()
