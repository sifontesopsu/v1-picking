import json
import math
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
URL_REPORTE = "https://www.kameone.cl/Reporte/InventarioBodega"

USER = "gsifontes@faurora.cl"
PASSWORD = "Dimasoft0858"

DOWNLOAD_FOLDER = r"C:\Users\VNP-4\Downloads"
BODEGA_OBJETIVO = "BODEGA UNIVERSAL"
BRANCH_NAME = "main"   # cambia a "master" si tu repo usa master

TIMEOUT = 25
MIN_ROWS_EXPORTADAS = 4000

# Repo = misma carpeta donde está este script
REPO_DIR = Path(__file__).resolve().parent
JSON_SALIDA = REPO_DIR / "stock_kame.json"


# =========================
# UTIL
# =========================
def debug(msg):
    print(f"[DEBUG] {msg}")


def wait(driver, seconds=TIMEOUT):
    return WebDriverWait(driver, seconds)


# =========================
# DRIVER
# =========================
def iniciar_driver():
    chrome_options = Options()

    # Visible para depurar. Cuando ya quede estable, descomenta:
    # chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1600,1000")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

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


def esperar_archivo_nuevo(antes, timeout=90):
    carpeta = Path(DOWNLOAD_FOLDER)
    fin = time.time() + timeout

    while time.time() < fin:
        actuales = {p.name for p in carpeta.iterdir() if p.is_file()}
        nuevos = actuales - antes

        # SOLO aceptar inventariobodega_*.xlsx
        xlsx_nuevos = [
            n for n in nuevos
            if n.lower().endswith(".xlsx")
            and not n.lower().endswith(".crdownload")
            and n.lower().startswith("inventariobodega_")
        ]

        if xlsx_nuevos:
            rutas = [carpeta / n for n in xlsx_nuevos]
            rutas.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return str(rutas[0])

        time.sleep(2)

    return None


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


def abrir_reporte_directo(driver):
    debug("Abriendo reporte directo...")
    driver.get(URL_REPORTE)

    wait(driver, 25).until(
        EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Inventario por Bodega')]")),
            EC.presence_of_element_located((By.XPATH, "//button[contains(., 'GENERAR EXCEL')]")),
        )
    )
    debug("Reporte abierto.")


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


def generar_excel(driver):
    debug("Presionando GENERAR EXCEL...")
    antes = listar_descargas()

    btn = wait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'GENERAR EXCEL')]"))
    )
    click_safe(driver, btn)
    return antes


# =========================
# NOTIFICACIONES
# =========================
def abrir_campana(driver):
    debug("Abriendo campana...")
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
                    debug("Notificación encontrada. Descargando archivo...")
                    click_safe(driver, link)
                    return True

            debug("Hay link 'aqui', pero no pude validar contexto correcto.")
            return False

        debug("Todavía no aparece la descarga. Reintentando...")
        time.sleep(4)

    return False


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

    s = s.replace(" ", "")
    s = s.replace(".", "")
    s = s.replace(",", ".")

    try:
        num = float(s)
        return int(num) if num.is_integer() else num
    except Exception:
        return 0


def excel_a_payload_stock(ruta_excel: str, bodega: str) -> dict:
    nombre_archivo = Path(ruta_excel).name.lower()

    if not nombre_archivo.startswith("inventariobodega_"):
        raise ValueError(f"Archivo descargado incorrecto: {nombre_archivo}")

    debug(f"Leyendo Excel: {ruta_excel}")
    df = pd.read_excel(ruta_excel)
    df = normalizar_columnas(df)

    # SOLO columnas exactas
    col_sku = buscar_columna_exacta(df, "SKU")
    col_saldo = buscar_columna_exacta(df, "Saldo")
    col_bodega = buscar_columna_exacta(df, "Bodega")

    if str(col_saldo).strip().lower() != "saldo":
        raise ValueError(f"El archivo no contiene la columna exacta 'Saldo'. Detectado: {col_saldo}")

    debug(f"Columna SKU detectada: {col_sku}")
    debug(f"Columna Saldo detectada: {col_saldo}")
    debug(f"Columna Bodega detectada: {col_bodega}")

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

    payload = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_file": Path(ruta_excel).name,
        "bodega": bodega,
        "rows_read": int(filas_totales),
        "rows_exported": int(filas_validas),
        "stock": stock_map,
    }

    return payload


# =========================
# VALIDACIONES
# =========================
def validar_payload_stock(payload: dict):
    if not isinstance(payload, dict):
        raise ValueError("Payload inválido: no es dict.")

    requeridos = ["updated_at", "source_file", "bodega", "rows_read", "rows_exported", "stock"]
    for k in requeridos:
        if k not in payload:
            raise ValueError(f"Payload inválido: falta clave '{k}'.")

    source_file = str(payload.get("source_file", "")).lower()
    if not source_file.startswith("inventariobodega_"):
        raise ValueError(f"Archivo fuente inválido: {source_file}")

    if payload["bodega"] != BODEGA_OBJETIVO:
        raise ValueError(f"Bodega inesperada: {payload['bodega']}")

    rows_exported = payload.get("rows_exported", 0)
    if not isinstance(rows_exported, int):
        raise ValueError("rows_exported inválido.")
    if rows_exported < MIN_ROWS_EXPORTADAS:
        raise ValueError(
            f"Protección activada: rows_exported={rows_exported} menor al mínimo permitido {MIN_ROWS_EXPORTADAS}."
        )

    stock = payload.get("stock")
    if not isinstance(stock, dict):
        raise ValueError("La clave 'stock' no es dict.")
    if len(stock) < MIN_ROWS_EXPORTADAS:
        raise ValueError(
            f"Protección activada: stock tiene {len(stock)} SKUs, menor al mínimo permitido {MIN_ROWS_EXPORTADAS}."
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
def run_git(args: list[str], cwd: Path) -> str:
    cmd = ["git"] + args
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Git falló: {' '.join(cmd)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout.strip()


def validar_repo_git(repo_dir: Path):
    if not repo_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta repo: {repo_dir}")
    if not (repo_dir / ".git").exists():
        raise FileNotFoundError(f"La carpeta actual no parece repo git: {repo_dir}")


def git_commit_y_push_si_hay_cambios(repo_dir: Path, archivo_relativo: str, mensaje_commit: str, branch_name: str):
    debug("Validando repo git...")
    validar_repo_git(repo_dir)

    debug("git add...")
    run_git(["add", archivo_relativo], repo_dir)

    diff_cached = run_git(["diff", "--cached", "--name-only"], repo_dir)
    staged_files = [x.strip() for x in diff_cached.splitlines() if x.strip()]

    if archivo_relativo not in staged_files:
        debug("No hay cambios staged para stock_kame.json. No se hace commit.")
        return False

    debug("git commit...")
    run_git(["commit", "-m", mensaje_commit], repo_dir)

    debug(f"git push origin {branch_name} ...")
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

        driver = iniciar_driver()
        login(driver)
        abrir_reporte_directo(driver)
        seleccionar_bodega(driver, BODEGA_OBJETIVO)
        activar_solo_con_saldo(driver)

        archivos_antes = generar_excel(driver)

        ok = esperar_notificacion_lista_y_descargar(driver, timeout=120)
        if not ok:
            raise RuntimeError("No apareció la notificación correcta lista para descargar.")

        archivo_descargado = esperar_archivo_nuevo(archivos_antes, timeout=90)
        if not archivo_descargado:
            raise RuntimeError("No se detectó un inventariobodega_*.xlsx nuevo en Descargas.")

        print("\nOK - Descarga completada")
        print("Archivo final:", archivo_descargado)

        payload_nuevo = excel_a_payload_stock(
            ruta_excel=archivo_descargado,
            bodega=BODEGA_OBJETIVO
        )

        validar_payload_stock(payload_nuevo)

        payload_anterior = cargar_json_existente(JSON_SALIDA)

        if payload_equivalente_sin_fecha(payload_anterior, payload_nuevo):
            print("\nSIN CAMBIOS - El stock nuevo es equivalente al actual.")
            print("No se sobrescribe ni se hace push.")
            return

        guardar_json(payload_nuevo, JSON_SALIDA)

        print("\nOK - JSON generado")
        print("JSON final:", JSON_SALIDA)
        print("Bodega:", payload_nuevo["bodega"])
        print("Rows read:", payload_nuevo["rows_read"])
        print("Rows exported:", payload_nuevo["rows_exported"])

        commit_msg = (
            f"update stock kame {payload_nuevo['bodega']} "
            f"{payload_nuevo['updated_at']}"
        )

        pushed = git_commit_y_push_si_hay_cambios(
            repo_dir=REPO_DIR,
            archivo_relativo="stock_kame.json",
            mensaje_commit=commit_msg,
            branch_name=BRANCH_NAME
        )

        if pushed:
            print("\nOK - Git actualizado")
            print("Commit y push realizados correctamente.")
        else:
            print("\nSIN CAMBIOS GIT - No había diferencias reales para subir.")

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    while True:
        try:
            print("\n" + "=" * 70)
            print("INICIO CICLO:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            ejecutar_ciclo()
            print("FIN CICLO OK")
        except KeyboardInterrupt:
            print("\nProceso detenido manualmente por el usuario.")
            break
        except Exception as e:
            print(f"\nFIN CICLO CON ERROR: {e}")

        print("\nEsperando 5 minutos para el próximo ciclo...")
        time.sleep(300)