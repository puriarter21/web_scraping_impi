##-------------------------------------------------------------------------------------------------------------------
##-------------------------------------------------------------------------------------------------------------------
##-------------------------------------------------------------------------------------------------------------------

# Dos bloqueos seguidos (paron completo)

#!/usr/bin/env python3
import time
import random
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================== CONFIGURACIÓN ==============================
CSV_FILE      = Path("7_df_combinado.csv")
OUTPUT_FOLDER = Path("7.pdfs_resto")
FAILED_FILE   = Path("7.failed_downloads_resto.txt")

BATCH_SIZE        = (30, 60)
SLEEP_PER_PDF     = (0.5, 1.0)
SLEEP_AFTER_BATCH = (10, 40)

MAX_RETRIES         = 3
BACKOFF_FACTOR      = 2
TIMEOUT             = 30
MAX_BLOQUEOS_CONSEC = 2   


# ======================== SISTEMA DE PROGRESO ========================
def cargar_descargados() -> set:
    return {f.stem for f in OUTPUT_FOLDER.glob("*.pdf")}


def cargar_fallidos() -> set:
    if FAILED_FILE.exists():
        return {line.strip() for line in FAILED_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}
    return set()


def registrar_fallido(file_stem: str):
    with FAILED_FILE.open("a", encoding="utf-8") as f:
        f.write(file_stem + "\n")


# ======================== CONSTRUCCIÓN DE PENDING ========================
def construir_pending(df: pd.DataFrame, descargados: set, fallidos: set) -> list:
    conteo  = {}
    pending = []
    for _, row in df.iterrows():
        id_  = str(row["id"])
        url  = str(row["pdf_links_marcia"])
        n    = conteo.get(id_, 0)
        conteo[id_] = n + 1
        file_stem = id_ if n == 0 else f"{id_}_{n}"
        if file_stem not in descargados and file_stem not in fallidos:
            pending.append((file_stem, url))
    return pending


# ======================== DESCARGA ========================
def descargar_pdf(file_stem: str, url: str, session: requests.Session) -> bool:
    output_path = OUTPUT_FOLDER / f"{file_stem}.pdf"

    for intento in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=TIMEOUT, verify=False, stream=True)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower() and "octet-stream" not in content_type.lower():
                raise ValueError(f"Content-Type inesperado: {content_type}")

            with output_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            if output_path.stat().st_size == 0:
                output_path.unlink()
                raise ValueError("Archivo descargado está vacío")

            return True

        except Exception as e:
            if output_path.exists():
                output_path.unlink()

            if intento == MAX_RETRIES:
                print(f"    ✗ Falló definitivamente: {file_stem} → {e}")
                return False
            else:
                wait = BACKOFF_FACTOR ** (intento - 1)
                print(f"    ↺ Intento {intento} fallido para {file_stem}, reintentando en {wait}s...")
                time.sleep(wait)

    return False


def esperar_21_min(file_stem: str):
    pausa = 21 * 60
    print(f"\n  ⚠ Bloqueo detectado en {file_stem}.")
    print(f"  Esperando 21 minutos antes de reintentar...")
    for remaining in range(pausa, 0, -60):
        print(f"    {remaining // 60} min restantes...", flush=True)
        time.sleep(60)
    print(f"  Reintentando {file_stem}...")


# ======================== EJECUCIÓN ========================
def ejecutar_descarga():
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(CSV_FILE)

    if "id" not in df.columns or "pdf_links_marcia" not in df.columns:
        raise ValueError("El CSV debe tener columnas 'id' y 'pdf_links_marcia'")

    df = df.dropna(subset=["pdf_links_marcia"])

    descargados = cargar_descargados()
    fallidos    = cargar_fallidos()
    pending     = construir_pending(df, descargados, fallidos)

    conteo_ids = df["id"].astype(str).value_counts()
    multiples  = (conteo_ids > 1).sum()

    print(f"Total filas en CSV          : {len(df):,}")
    print(f"IDs únicos                  : {conteo_ids.shape[0]:,}")
    print(f"IDs con múltiples PDFs      : {multiples:,}")
    print(f"Ya descargados              : {len(descargados):,}")
    print(f"Fallidos definitivos        : {len(fallidos):,}")
    print(f"Pendientes por descargar    : {len(pending):,}\n")

    if not pending:
        print("Todos los PDFs ya fueron descargados.")
        return

    start_time       = time.time()
    nuevos           = 0
    nuevos_fallidos  = 0
    batch_num        = 0
    i                = 0
    bloqueos_consec  = 0   
    parar            = False

    with requests.Session() as session:
        while i < len(pending) and not parar:
            this_batch_size = random.randint(*BATCH_SIZE)
            batch           = pending[i:i + this_batch_size]
            batch_num      += 1

            print(f"\n{'─'*55}")
            print(f"  Batch {batch_num}  ({len(batch)} PDFs)  |  global: {i}/{len(pending)}")
            print(f"{'─'*55}")

            for j, (file_stem, url) in enumerate(batch, 1):
                ok = descargar_pdf(file_stem, url, session)

                if not ok:
                    # Primer intento tras bloqueo
                    bloqueos_consec += 1
                    esperar_21_min(file_stem)
                    ok = descargar_pdf(file_stem, url, session)

                    if not ok:
                        # Sigue fallando — contar como segundo bloqueo consecutivo
                        bloqueos_consec += 1
                        if bloqueos_consec >= MAX_BLOQUEOS_CONSEC * 2:
                            registrar_fallido(file_stem)
                            print(f"\n  {MAX_BLOQUEOS_CONSEC} bloqueos consecutivos sin recuperación.")
                            print(f"  Deteniendo el proceso. Reinicia cuando el servidor esté disponible.")
                            parar = True
                            break
                else:
                    bloqueos_consec = 0   # descarga exitosa → resetear contador

                if ok:
                    nuevos += 1
                    status  = "✓"
                else:
                    nuevos_fallidos += 1
                    registrar_fallido(file_stem)
                    status = "✗"

                elapsed    = time.time() - start_time
                velocidad  = nuevos / elapsed * 60 if elapsed > 0 else 0
                global_pos = i + j
                print(
                    f"  [{j:>3}/{len(batch)}] global {global_pos:>5}/{len(pending)}"
                    f"  {status}  {file_stem}"
                    f"  |  {velocidad:.1f} PDFs/min  ok={nuevos} ✗={nuevos_fallidos}"
                )

                time.sleep(random.uniform(*SLEEP_PER_PDF))

            if parar:
                break

            elapsed      = time.time() - start_time
            velocidad    = nuevos / elapsed * 60 if elapsed > 0 else 0
            restantes    = len(pending) - nuevos - nuevos_fallidos
            estimado_min = (restantes / (nuevos / elapsed)) / 60 if nuevos > 0 else 0

            print(f"\n  → Batch {batch_num} terminado")
            print(f"    Descargados esta sesión : {nuevos:,}  |  Fallidos: {nuevos_fallidos:,}")
            print(f"    Velocidad               : {velocidad:.1f} PDFs/min")
            print(f"    Estimado restante       : {estimado_min:.0f} min  ({estimado_min/60:.1f} hrs)")

            i += len(batch)

            if i < len(pending):
                this_sleep = random.randint(*SLEEP_AFTER_BATCH)
                print(f"\n  Pausa de {this_sleep}s antes del siguiente batch...")
                time.sleep(this_sleep)

    print(f"\n{'='*55}")
    if parar:
        print(f" Proceso detenido por bloqueos consecutivos.")
    else:
        print(f"  ✓ Descarga completada.")
    print(f"    Descargados esta sesión : {nuevos:,}")
    print(f"    Fallidos definitivos    : {nuevos_fallidos:,}")
    print(f"    PDFs totales en disco   : {len(cargar_descargados()):,}")
    print(f"{'='*55}")


if __name__ == "__main__":
    try:
        ejecutar_descarga()
    except KeyboardInterrupt:
        print("\n\nInterrumpido. Al reiniciar continuará desde donde se quedó.")