from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import sys
import zipfile
from typing import Optional, Dict, Any

from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

from src.utils_youtube import fetch_youtube_meta, extract_video_id

# Variables de entorno requeridas
PROJECT = os.getenv("GCP_PROJECT")
BUCKET = os.getenv("GCS_BUCKET", "svr_object_storage")
DATASET = os.getenv("BQ_DATASET", "hud_project")
TABLE = os.getenv("BQ_TABLE_ARCHIVES", "archives_index")
UPLOADER_VERSION = os.getenv("UPLOADER_VERSION", "uploader-v1")

# Prefijos en GCS
ARCHIVE_PREFIX = "archive/"
LOGS_PREFIX = "logs/archive_ingest/"

# Extensiones de imagen permitidas para el conteo
ALLOWED_EXT = {".jpg", ".jpeg", ".png"}

# Valores permitidos para --origin
ORIGIN_CHOICES = ["youtube", "public", "simulated", "real"]
CATEGORY_PREFIX = {
    "public": "public/",
    "simulated": "simulated/",
    "real": "real/",
    "youtube": "youtube/",
}

# Helpers
def get_bq() -> bigquery.Client:
    if not PROJECT:
        raise RuntimeError("Missing env var GCP_PROJECT.")
    return bigquery.Client(project=PROJECT)

def get_gcs() -> storage.Client:
    if not PROJECT:
        raise RuntimeError("Missing env var GCP_PROJECT.")
    return storage.Client(project=PROJECT)

# Utilidades
def sha256_file(path: str) -> str:
    '''
    Calcula el hash SHA-256 de un archivo.
    '''
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def now_iso_utc() -> str:
    '''
    Devuelve la fecha y hora actual en formato ISO 8601 (UTC).
    '''
    return datetime.datetime.now(datetime.UTC).isoformat()

def count_images_in_zip(zip_path: str) -> int:
    """
    Conteo de imágenes dentro del ZIP (soporte a subcarpetas).
    """
    n = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename.lower()
            if any(name.endswith(ext) for ext in ALLOWED_EXT):
                n += 1
    return n

def youtube_from_url(url: str) -> Dict[str, Optional[str]]:
    """
    Devuelve metadatos de YouTube a partir de la URL.
    Campos: video_id, title, channel, publish_date (YYYY-MM-DD), license
    """
    meta = fetch_youtube_meta(url)
    vid = meta.get("video_id") or extract_video_id(url)
    if not vid:
        raise ValueError("Could not determine youtube.video_id from the given URL.")
    return meta

# CLI principal
def main() -> None:
    # Definición de CLI
    parser = argparse.ArgumentParser(
        description="Upload a ZIP to gs://<bucket>/archive/ with object metadata, log JSON, and BigQuery record."
    )
    parser.add_argument("--zip", required=True, help="Local path to the ZIP file")
    parser.add_argument("--origin", required=True, choices=ORIGIN_CHOICES,
                        help="ZIP source type")
    parser.add_argument("--dataset", default="", help="Dataset logical name (if applicable)")
    parser.add_argument("--url", default="", help="Source URL (required if origin=youtube)")
    args = parser.parse_args()

    # Validaciones de entrada
    if args.origin == "youtube" and not args.url:
        print("[ERROR] origin=youtube requires --url of the video.", file=sys.stderr)
        sys.exit(2)
    if not os.path.isfile(args.zip):
        print(f"[ERROR] ZIP not found: {args.zip}", file=sys.stderr)
        sys.exit(2)

    # Derivados básicos del ZIP
    ts_ingest = now_iso_utc()  # Timestamp de ingestión
    zip_name = os.path.basename(args.zip)  # Nombre del archivo ZIP sin ruta
    sha256_zip = sha256_file(args.zip)  # Hash SHA-256 del archivo ZIP para deduplicar y auditar contenido
    size_bytes = os.path.getsize(args.zip)  # Tamaño del archivo ZIP en bytes
    num_images = count_images_in_zip(args.zip)  # Conteo de imágenes válidas dentro del ZIP

    dataset = (args.dataset or "").lower()  # Normalización del nombre del dataset en minúsculas
    origin = args.origin
    dest_prefix = ARCHIVE_PREFIX + CATEGORY_PREFIX[origin] # Prefijo destino en GCS

    # Clientes GCP
    bq = get_bq()
    gcs = get_gcs()
    table_ref_full = f"{PROJECT}.{DATASET}.{TABLE}"  # Referencia completa a la tabla de BigQuery

    # Dedupe por sha256_zip
    ## Consulta previa de si existe un registro con ese sha256_zip
    prev_rows = list(
        bq.query(
            f"""
            SELECT is_deleted
            FROM `{table_ref_full}`
            WHERE sha256_zip = @sha
            ORDER BY ts_ingest DESC
            LIMIT 1
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("sha", "STRING", sha256_zip)]
            ),
        )
    )
    prev_is_deleted = prev_rows[0]["is_deleted"] if prev_rows else None

    # Si existe un registro previo y no está eliminado, se omite la carga
    if prev_rows and (prev_is_deleted is False or prev_is_deleted is None):
        print(f"[SKIP] ZIP already registered (sha256={sha256_zip}).")
        sys.exit(0)

    # Metadatos YouTube
    ## Obtener los metadatos y aplicar la regla "1 video -> 1 ZIP"
    youtube: Optional[Dict[str, Any]] = None
    if origin == "youtube":
        youtube = youtube_from_url(args.url) # {video_id,title,channel,publish_date,license}
        clash = list(
            bq.query(
                f"""
                SELECT zip_name
                FROM `{table_ref_full}`
                WHERE COALESCE(is_deleted, FALSE) = FALSE
                AND youtube.video_id = @vid
                LIMIT 1
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("vid", "STRING", youtube["video_id"])]
                ),
            )
        )
        if clash:
            print(f"[ERROR] There is already an active ZIP for video_id={youtube['video_id']}.", file=sys.stderr)
            sys.exit(3)

    # Subida a GCS con metadata del objeto
    bucket = gcs.bucket(BUCKET)
    blob = bucket.blob(dest_prefix + zip_name) # Ruta final: archive/<origin>/<zip_name>

    # Metadata del objeto que se adjunta al objeto de GCS
    metadata = {
        "origin": origin,
        "source_url": args.url or "",
        "dataset": dataset,
        "sha256_zip": sha256_zip,
        "zip_size_bytes": str(size_bytes),
        "num_images": str(num_images),
        "ts_ingest": ts_ingest,
        "ingested_by": UPLOADER_VERSION,
    }
    if youtube:
        # Campos de YouTube expandidos como metadata plana (útiles para buscar/filtrar en GCS)
        metadata.update({
            "youtube_video_id": youtube.get("video_id") or "",
            "youtube_title": youtube.get("title") or "",
            "youtube_channel": youtube.get("channel") or "",
            "youtube_publish_date": youtube.get("publish_date") or "",
            "license": youtube.get("license") or "",
        })

    # Subida del archivo y escritura de metadata
    blob.metadata = metadata
    blob.upload_from_filename(args.zip)
    gcs_uri = f"gs://{BUCKET}/{dest_prefix}{zip_name}"

    #  Intentamos leer la 'generation' (versión) del objeto recién subido
    try:
        blob.reload()
        generation = int(blob.generation)
    except (NotFound, ValueError, TypeError):
        generation = None  # No es crítico para continuar

    # Log JSON lateral en GCS (auditoría/trazabilidad)
    log_doc = {
        "event": "ingest",
        "zip_name": zip_name,
        "gcs_uri": gcs_uri,
        "origin": origin,
        "source_url": args.url or None,
        "dataset": dataset,
        "sha256_zip": sha256_zip,
        "zip_size_bytes": size_bytes,
        "num_images": num_images,
        "ts_ingest": ts_ingest,
        "uploader_version": UPLOADER_VERSION,
        "youtube": youtube,
    }
    # Nombre del log: logs/archive_ingest/<origin>/<zip_name_sin_ext>.json
    logs_prefix_cat = LOGS_PREFIX + CATEGORY_PREFIX[origin]
    bucket.blob(logs_prefix_cat + os.path.splitext(zip_name)[0] + ".json").upload_from_string(
        json.dumps(log_doc, ensure_ascii=False, indent=2), content_type="application/json"
    )

    # Escritura en BigQuery: reactivar o insertar
    if prev_rows and prev_is_deleted is True:
        # Soft-delete: Ya existe un registro previo del mismo sha pero está marcado como eliminado, la reactivamos
        has_yt = bool(youtube)

        params = [
            bigquery.ScalarQueryParameter("gen", "INT64", generation),
            bigquery.ScalarQueryParameter("uri", "STRING", gcs_uri),
            bigquery.ScalarQueryParameter("zip", "STRING", zip_name),
            bigquery.ScalarQueryParameter("origin", "STRING", origin),
            bigquery.ScalarQueryParameter("src", "STRING", args.url or None),
            bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
            bigquery.ScalarQueryParameter("nimg", "INT64", num_images),
            bigquery.ScalarQueryParameter("ts", "TIMESTAMP", ts_ingest),
            bigquery.ScalarQueryParameter("has_yt", "BOOL", has_yt),
            bigquery.ScalarQueryParameter("sha", "STRING", sha256_zip),
        ]

        # Si hay YouTube, preparamos @yt como STRUCT y añadimos el parámetro
        if has_yt:
            yt_publish_date_date = None
            if youtube.get("publish_date"):
                yt_publish_date_date = datetime.date.fromisoformat(youtube["publish_date"])
            yt_param = bigquery.StructQueryParameter(
                "yt",
                [
                    bigquery.ScalarQueryParameter("video_id", "STRING", youtube.get("video_id")),
                    bigquery.ScalarQueryParameter("title", "STRING", youtube.get("title")),
                    bigquery.ScalarQueryParameter("channel", "STRING", youtube.get("channel")),
                    bigquery.ScalarQueryParameter("publish_date", "DATE", yt_publish_date_date),
                    bigquery.ScalarQueryParameter("license", "STRING", youtube.get("license")),
                ],
            )
            params.append(yt_param)  # solo añadimos @yt si has_yt=True
            bq.query(
                f"""
                UPDATE `{table_ref_full}`
                SET is_deleted = FALSE,
                    ts_deleted = NULL,
                    delete_reason = NULL,
                    deleted_by = NULL,
                    exists_in_gcs = TRUE,
                    gcs_generation_last = @gen,
                    gcs_uri = @uri,
                    zip_name = @zip,
                    origin = @origin,
                    source_url = @src,
                    dataset = @dataset,
                    num_images = @nimg,
                    ts_ingest = @ts,
                    -- clave: si no hay youtube, forzamos NULL; si hay, asignamos el STRUCT @yt
                    youtube = IF(@has_yt, @yt, NULL)
                WHERE sha256_zip = @sha
                AND COALESCE(is_deleted, FALSE) = TRUE
                """,
                job_config=bigquery.QueryJobConfig(query_parameters=params),
            ).result()
    else:
        # No existe registro previo, insertamos una nueva fila
        row = {
            "gcs_uri": gcs_uri,
            "zip_name": zip_name,
            "sha256_zip": sha256_zip,
            "zip_size_bytes": size_bytes,
            "origin": origin,
            "source_url": args.url or None,
            "dataset": dataset,
            "num_images": num_images,
            "ts_ingest": ts_ingest,
            "youtube": youtube,
            "exists_in_gcs": True,
            "is_deleted": False,
            "ts_deleted": None,
            "delete_reason": None,
            "deleted_by": None,
            "gcs_generation_last": generation,
        }
        job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        load_job = bq.load_table_from_json([row], table_ref_full, job_config=job_cfg)
        load_job.result()  # Espera a que termine el job

        if load_job.errors:
            print(f"[ERROR] BigQuery load job: {load_job.errors}", file=sys.stderr)
            sys.exit(4)

    print(f"[OK] Uploaded and recorded: {gcs_uri}")


if __name__ == "__main__":
    main()
