"""
Scraper de resultados y splits del Maraton de la Ciudad de Mexico 2026 (Telcel)
Evento: SPTMCAR1770827883
Fuente: https://resultados.marcate.events

Dos etapas:
  1) Recorre las 22 categorias via OverAllFilter para obtener la lista completa
     de bibs (numeros de corredor) del maraton.
  2) Por cada bib, consulta loadResultado para obtener el detalle completo,
     incluyendo los tiempos parciales (splits) de cada tapete:
     5K, 10K, 15K, 21K, 25K, 30K, 35K, 40K.

Disenado para correr en Google Colab. Guarda progreso incremental para poder
reanudar si se corta la ejecucion (timeout de Colab, error de red, etc.).

USO EN COLAB:
  1) (Opcional pero recomendado) Monta tu Google Drive y cambia BIBS_FILE /
     OUTPUT_FILE mas abajo para que apunten ahi, asi el progreso sobrevive
     aunque el entorno de Colab se reinicie:

         from google.colab import drive
         drive.mount('/content/drive')

  2) Sube este archivo a Colab (o pega su contenido en una celda) y corre:

         !python scraper_maraton_cdmx_2026_splits.py

     o si lo pegaste en una celda, simplemente ejecuta la celda.

  3) Si se corta a medias, vuelve a correrlo: retoma automaticamente donde
     se quedo (no vuelve a pedir los bibs ya guardados en OUTPUT_FILE).
"""

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(it, **kwargs):
        return it

# ----------------------------------------------------------------------------
# Configuracion
# ----------------------------------------------------------------------------

BASE_URL = "https://resultados.marcate.events"
CARRERA_ID = "SPTMCAR1770827883"
RESULT_PAGE = f"{BASE_URL}/resultado/{CARRERA_ID}"
OVERALL_FILTER_URL = f"{BASE_URL}/eventos/OverAllFilter"
LOAD_RESULTADO_URL = f"{BASE_URL}/eventos/loadResultado"

# Cambia estas rutas a una carpeta de Google Drive si quieres que el progreso
# sobreviva a un reinicio del entorno de Colab, ej:
# BIBS_FILE = "/content/drive/MyDrive/maraton_cdmx/bibs_maraton_cdmx_2026.csv"
BIBS_FILE = "bibs_maraton_cdmx_2026.csv"
OUTPUT_FILE = "resultados_splits_maraton_cdmx_2026.csv"

MAX_WORKERS = 8            # peticiones concurrentes en la etapa 2
DELAY_ENTRE_CATEGORIAS = 0.5   # segundos entre cada una de las 22 llamadas de la etapa 1
GUARDAR_CADA = 100          # cada cuantas respuestas de la etapa 2 se guarda progreso a disco

CATEGORIAS = [
    "ABSOLUTOS VARONIL",
    "LIBRE VARONIL (18 A 34 AñOS)",
    "MASTER VARONIL (35 A 39 AñOS)",
    "VETERANO VARONIL (40 A 44 AñOS)",
    "VETERANO VARONIL II (45 A 49 AñOS)",
    "VETERANO VARONIL III (50 A 54 AñOS)",
    "VETERANO VARONIL IV (55 A 59 AñOS)",
    "VETERANO VARONIL V (60 A 64 AñOS)",
    "VETERANO VARONIL VI (65 AñOS Y MAS)",
    "SILLA DE RUEDAS VARONIL",
    "CIEGO TOTAL Y DEBIL VISUAL VARONIL",
    "ABSOLUTOS FEMENIL",
    "LIBRE FEMENIL (18 A 34 AñOS)",
    "MASTER FEMENIL (35 A 39 AñOS)",
    "VETERANO FEMENIL (40 A 44 AñOS)",
    "VETERANO FEMENIL II (45 A 49 AñOS)",
    "VETERANO FEMENIL III (50 A 54 AñOS)",
    "VETERANO FEMENIL IV (55 A 59 AñOS)",
    "VETERANO FEMENIL V (60 A 64 AñOS)",
    "VETERANO FEMENIL VI (65 AñOS Y MAS)",
    "SILLA DE RUEDAS FEMENIL",
    "CIEGO TOTAL Y DEBIL VISUAL FEMENIL",
]

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": BASE_URL,
    "referer": RESULT_PAGE,
    "x-requested-with": "XMLHttpRequest",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    ),
}

SPLITS_ESPERADOS = ["5K", "10K", "15K", "21K", "25K", "30K", "35K", "40K"]


# ----------------------------------------------------------------------------
# Sesion HTTP compartida
# ----------------------------------------------------------------------------

def crear_sesion():
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    s.mount("https://", adapter)
    # Priming: visitar la pagina normal para que el servidor nos de las cookies de sesion
    s.get(RESULT_PAGE, timeout=30)
    return s


# ----------------------------------------------------------------------------
# Etapa 1: lista de bibs por categoria
# ----------------------------------------------------------------------------

def obtener_bibs(session):
    bibs = {}
    for cat in CATEGORIAS:
        data = {"distancia": "MARATON", "carreraId": CARRERA_ID, "categoriaId": cat}
        try:
            resp = session.post(OVERALL_FILTER_URL, data=data, timeout=30)
            resp.raise_for_status()
            corredores = resp.json()
        except Exception as e:
            print(f"  ERROR en categoria '{cat}': {e}")
            continue

        nuevos = 0
        for c in corredores:
            bib = c.get("id")
            if bib and bib not in bibs:
                bibs[bib] = {"categoria_overall": cat, "nombre_overall": c.get("nombre")}
                nuevos += 1

        print(f"[{cat}] {len(corredores)} corredores ({nuevos} nuevos) - acumulado: {len(bibs)} bibs unicos")
        time.sleep(DELAY_ENTRE_CATEGORIAS)

    return bibs


# ----------------------------------------------------------------------------
# Etapa 2: detalle + splits por bib
# ----------------------------------------------------------------------------

def bib_a_fila(bib, payload):
    info = payload.get("info", {})
    fila = {
        "bib": bib,
        "nombre": info.get("nombre"),
        "categoria": info.get("categoria"),
        "rama": info.get("rama"),
        "posicion_overall": info.get("posicion"),
        "posicion_cuenta": info.get("posicion_cuenta"),
        "posicionRama": info.get("posicionRama"),
        "posicionRama_de": info.get("posicionRama_cuantos"),
        "posicionCategoria": info.get("posicionCategoria"),
        "posicionCategoria_de": info.get("posicionCategoria_cuantos"),
        "guntime": info.get("guntime"),
        "tiempoChip": info.get("tiempoChip"),
        "segundosGuntime": info.get("segundosGuntime"),
        "pace": info.get("pace"),
        "paso": info.get("paso"),
        "equipo": info.get("equipo"),
    }
    for split_nombre in SPLITS_ESPERADOS:
        fila[f"split_{split_nombre}"] = None
    for split in info.get("Intermedios", []) or []:
        nombre_split = split.get("nombre")
        fila[f"split_{nombre_split}"] = split.get("tiempo")
    return fila


def consultar_bib(session, bib, intentos=3):
    data = {"nombre": "", "numero": bib, "carreraId": CARRERA_ID}
    for intento in range(1, intentos + 1):
        try:
            resp = session.post(LOAD_RESULTADO_URL, data=data, timeout=30)
            resp.raise_for_status()
            j = resp.json()
            if j.get("encontrado") == "si":
                return bib_a_fila(bib, j)
            return None  # bib valido pero sin resultado (no corrio, DNF, etc.)
        except Exception as e:
            if intento == intentos:
                print(f"  ERROR bib {bib} tras {intentos} intentos: {e}")
                return None
            time.sleep(1.5 * intento)


def obtener_splits(session, bibs, resume=True):
    filas = []
    ya_procesados = set()

    if resume and os.path.exists(OUTPUT_FILE):
        df_prev = pd.read_csv(OUTPUT_FILE, dtype=str)
        filas = df_prev.to_dict("records")
        ya_procesados = set(df_prev["bib"].astype(str))
        print(f"Reanudando: {len(ya_procesados)} bibs ya estaban procesados.")

    pendientes = [b for b in bibs if str(b) not in ya_procesados]
    print(f"Pendientes por consultar: {len(pendientes)} de {len(bibs)} totales.")

    lock = threading.Lock()
    procesados_desde_ultimo_guardado = 0

    def guardar():
        pd.DataFrame(filas).to_csv(OUTPUT_FILE, index=False)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(consultar_bib, session, bib): bib for bib in pendientes}
        for fut in tqdm(as_completed(futuros), total=len(futuros)):
            fila = fut.result()
            with lock:
                if fila:
                    filas.append(fila)
                procesados_desde_ultimo_guardado += 1
                if procesados_desde_ultimo_guardado >= GUARDAR_CADA:
                    guardar()
                    procesados_desde_ultimo_guardado = 0

    guardar()
    print(f"Listo. Total de filas guardadas: {len(filas)}")
    return pd.DataFrame(filas)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    session = crear_sesion()

    print("=== Etapa 1: obteniendo lista de bibs por categoria ===")
    if os.path.exists(BIBS_FILE):
        df_bibs = pd.read_csv(BIBS_FILE, dtype=str)
        bibs_info = {
            row["bib"]: {
                "categoria_overall": row["categoria_overall"],
                "nombre_overall": row["nombre_overall"],
            }
            for _, row in df_bibs.iterrows()
        }
        print(f"Se cargaron {len(bibs_info)} bibs desde {BIBS_FILE} (ya existia).")
    else:
        bibs_info = obtener_bibs(session)
        pd.DataFrame(
            [{"bib": b, **info} for b, info in bibs_info.items()]
        ).to_csv(BIBS_FILE, index=False)
        print(f"Guardado {BIBS_FILE} con {len(bibs_info)} bibs.")

    print("\n=== Etapa 2: obteniendo detalle + splits por bib ===")
    df_final = obtener_splits(session, list(bibs_info.keys()))
    print(df_final.head())
