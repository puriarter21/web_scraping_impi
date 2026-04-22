import json
import time
import random
import requests
import argparse
from pathlib import Path
from tqdm import tqdm
from bs4 import XMLParsedAsHTMLWarning
import warnings
import urllib3

# ============================== CONFIGURACIÓN ==============================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

BASE_URL      = "https://marcia.impi.gob.mx"
ORIGINAL_JSON = Path("2.MARCIA_result_pages.json")

# ── compartidos entre todos los nodos ───────────────────────────
RESULTADOS_JSONL = Path("4.marcia_details.jsonl")
FALLIDOS_JSONL   = Path("4.fallidos.jsonl")

# ==================== PARÁMETROS ====================
BATCH_SIZE        = 99
SLEEP_PER_RECORD  = random.uniform(0.01, 0.05)
SLEEP_AFTER_BATCH = 0.2
MAX_RETRIES       = 2
BACKOFF_FACTOR    = 2


# ======================== SESIÓN ========================
def crear_sesion():
    session = requests.Session()
    session.get(f"{BASE_URL}/marcas/search/quick")
    xsrf_token = session.cookies.get("XSRF-TOKEN")
    headers = {
        "X-XSRF-TOKEN": xsrf_token,
        "Referer":       f"{BASE_URL}/marcas/search/quick"
    }
    return session, headers


# ======================== FUNCIÓN PRINCIPAL ========================
def obtener_detalle(id_marca: str, session, headers) -> dict:
    url = f"{BASE_URL}/marcas/search/internal/view/{id_marca}?sort=&pageSize=100"
    r   = session.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    datos = r.json()

    pdf_links = [
        rec.get("image", "")
        for rec in datos.get("historyData", {}).get("historyRecords", [])
        if rec.get("image")
    ]

    return {
        "id":          id_marca,
        "details":     datos.get("details", {}),
        "historyData": datos.get("historyData", {}),
        "pdf_links":   pdf_links
    }


# ======================== SISTEMA DE PROGRESO ========================
def cargar_set(filepath: Path) -> set:
    if filepath.exists():
        return {line.strip() for line in filepath.read_text(encoding="utf-8").splitlines() if line.strip()}
    return set()


def guardar_set(data: set, filepath: Path):
    filepath.write_text("\n".join(sorted(data)) + "\n", encoding="utf-8")


def append_jsonl(data: dict, filepath: Path):
    with filepath.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def cargar_fallidos_set() -> set:
    if not FALLIDOS_JSONL.exists():
        return set()
    fallidos = set()
    with FALLIDOS_JSONL.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("id"):
                    fallidos.add(str(obj["id"]))
            except json.JSONDecodeError:
                continue
    return fallidos


def sincronizar_processed(processed: set, processed_file: Path) -> set:
    if not RESULTADOS_JSONL.exists():
        return processed
    ids_en_jsonl = set()
    with RESULTADOS_JSONL.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("id") and "error" not in obj:
                    ids_en_jsonl.add(obj["id"])
            except json.JSONDecodeError:
                continue
    nuevos = ids_en_jsonl - processed
    if nuevos:
        print(f"  → Sincronizando {len(nuevos):,} IDs del jsonl")
        processed.update(nuevos)
        guardar_set(processed, processed_file)
    return processed


# ======================== EJECUCIÓN ========================
def ejecutar_scraping(start: int, end: int, nodo: int):

    # cada nodo tiene su propio archivo de progreso
    processed_file = Path(f"4.processed_ids_nodo{nodo}.txt")

    print(f"Nodo {nodo} | Rango índices: {start:,} → {end:,}\n")

    with ORIGINAL_JSON.open(encoding="utf-8") as f:
        datos = json.load(f)

    todos_ids = [item["id"] for item in datos if item.get("id")]
    ids_nodo  = todos_ids[start:end]   # ← solo el rango de este nodo

    processed            = cargar_set(processed_file)
    processed            = sincronizar_processed(processed, processed_file)
    fallidos             = cargar_fallidos_set()
    procesados_al_inicio = len(processed)
    bytes_totales        = 0

    pending = [
        id_marca for id_marca in ids_nodo
        if id_marca not in processed and id_marca not in fallidos
    ]

    print(f"IDs en este nodo      : {len(ids_nodo):,}")
    print(f"Ya procesados         : {len(processed):,}")
    print(f"Fallidos definitivos  : {len(fallidos):,}")
    print(f"Pendientes            : {len(pending):,}\n")

    if not pending:
        print(f"Nodo {nodo}: todos los IDs ya fueron procesados.")
        return

    session, headers = crear_sesion()
    start_time_total  = time.time()

    for i in range(0, len(pending), BATCH_SIZE):
        batch_start = time.time()
        batch       = pending[i:i + BATCH_SIZE]
        print(f"\n[Nodo {nodo}] Batch {i//BATCH_SIZE + 1}/{-(-len(pending)//BATCH_SIZE)} ({len(batch)} registros)")

        for id_marca in tqdm(batch, desc=f"Nodo {nodo}", leave=False):

            for intento in range(1, MAX_RETRIES + 1):
                try:
                    detalle = obtener_detalle(id_marca, session, headers)

                    if not detalle.get("details"):
                        raise Exception("Respuesta vacía")

                    append_jsonl(detalle, RESULTADOS_JSONL)
                    processed.add(id_marca)
                    bytes_totales += len(json.dumps(detalle).encode("utf-8"))

                    time.sleep(SLEEP_PER_RECORD + random.uniform(0.2, 0.8))
                    break

                except Exception as e:
                    if intento == MAX_RETRIES:
                        print(f"  ✗ [Nodo {nodo}] Falló: {id_marca} → {e}")
                        append_jsonl({"id": id_marca, "error": str(e)}, FALLIDOS_JSONL)
                        fallidos.add(id_marca)
                        if "connection" in str(e).lower() or "timeout" in str(e).lower():
                            print(f"  → [Nodo {nodo}] Renovando sesión...")
                            session, headers = crear_sesion()
                    else:
                        time.sleep(BACKOFF_FACTOR ** (intento - 1))

        # ── estadísticas ─────────────────────────────────────────
        batch_time        = time.time() - batch_start
        total_time        = time.time() - start_time_total
        nuevos            = len(processed) - procesados_al_inicio
        velocidad         = nuevos / total_time if total_time > 0 else 0
        pendientes_reales = len(pending) - nuevos
        estimado_min      = (pendientes_reales / velocidad / 60) if velocidad > 0 else 0

        if nuevos > 0:
            mb_restantes = (pendientes_reales * (bytes_totales / nuevos)) / (1024 ** 2)
        else:
            mb_restantes = 0

        print(f"[Nodo {nodo}] Batch: {batch_time/60:.1f} min | Velocidad: {velocidad:.1f} reg/min")
        print(f"[Nodo {nodo}] Nuevos: {nuevos:,} | Estimado: {estimado_min:.0f} min ({estimado_min/60:.1f} hrs)")
        print(f"[Nodo {nodo}] Tráfico: {bytes_totales/1024**2:.1f} MB | Restante: {mb_restantes:.0f} MB")

        guardar_set(processed, processed_file)

        print(f"[Nodo {nodo}] Pausa de {SLEEP_AFTER_BATCH:.0f}s...")
        time.sleep(SLEEP_AFTER_BATCH)

    print(f"\n Nodo {nodo} completado.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper MARCIA details — modo nodo")
    parser.add_argument("--start", type=int, required=True, help="Índice inicial (inclusive)")
    parser.add_argument("--end",   type=int, required=True, help="Índice final (exclusive)")
    parser.add_argument("--nodo",  type=int, required=True, help="Número de nodo (1, 2, 3...)")
    args = parser.parse_args()

    try:
        ejecutar_scraping(args.start, args.end, args.nodo)
    except KeyboardInterrupt:
        print(f"\n\n[Nodo {args.nodo}] Interrumpido. Progreso guardado.")