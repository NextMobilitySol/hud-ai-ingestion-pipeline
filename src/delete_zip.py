from __future__ import annotations

import argparse
import json
import os
import sys
import datetime
from typing import Optional, Tuple

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

# Variables de entorno requeridas
PROJECT = os.getenv("GCP_PROJECT")
BUCKET = os.getenv("GCS_BUCKET", "svr_object_storage")
DATASET = os.getenv("BQ_DATASET", "hud_project")
TABLE = os.getenv("BQ_TABLE_ARCHIVES", "archives_index")

# Prefijos en GCS
ARCHIVE_PREFIX = "archive/"
DELETE_LOG_PREFIX = "logs/archive_delete/"

# Valores permitidos para --origin
ORIGIN_CHOICES = ["youtube", "public", "simulated", "real"]
CATEGORY_PREFIX = {
    "public": "public/",
    "simulated": "simulated/",
    "real": "real/",
    "youtube": "youtube/",
}

# Helpers
def now_iso_utc() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()

def get_bq() -> bigquery.Client:
    if not PROJECT:
        raise RuntimeError("Missing env var GCP_PROJECT.")
    return bigquery.Client(project=PROJECT)

def get_gcs() -> storage.Client:
    if not PROJECT:
        raise RuntimeError("Missing env var GCP_PROJECT.")
    return storage.Client(project=PROJECT)

def resolve_archive_path(zip_name: str, origin: Optional[str]) -> Tuple[str, Optional[str]]:
    """
    Devuelve (prefix, gcs_uri) donde buscar/borrar.
    - Si origin se da: usa ese prefix.
    - Si no: busca en todas las categorías y exige unicidad.
    """
    gcs = get_gcs()
    bucket = gcs.bucket(BUCKET)

    if origin:
        dest_prefix = ARCHIVE_PREFIX + CATEGORY_PREFIX[origin]
        gcs_uri = f"gs://{BUCKET}/{dest_prefix}{zip_name}"
        return dest_prefix, gcs_uri

    # No origin: probamos todas y vemos dónde existe
    candidates = []
    for cat, sub in CATEGORY_PREFIX.items():
        pfx = ARCHIVE_PREFIX + sub
        blob = bucket.blob(pfx + zip_name)
        if blob.exists():
            candidates.append((pfx, f"gs://{BUCKET}/{pfx}{zip_name}"))

    if len(candidates) == 1:
        return candidates[0]
    elif len(candidates) == 0:
        # No existe en ninguna; por defecto devolvemos 'archive/' sin subcarpeta para compatibilidad
        # (seguirá sin existir, pero se registrará el intento en logs/BQ)
        pfx = ARCHIVE_PREFIX
        return pfx, f"gs://{BUCKET}/{pfx}{zip_name}"
    else:
        cats = ", ".join([p.split("/", 2)[1] for p, _ in candidates])  # nombres de categoría
        print(f"[ERROR] ZIP '{zip_name}' exists in multiple categories: {cats}. "
            f"Please specify --origin.", file=sys.stderr)
        sys.exit(2)

# Utilidades

def gcs_delete_zip(zip_name: str, origin: Optional[str]) -> Tuple[bool, Optional[int], str, str]:
    """
    Intenta borrar gs://BUCKET/archive/<origin>/<zip_name> (o resuelve origin).
    Return: (existed_before, generation_deleted, gcs_uri, dest_prefix)
    """
    gcs = get_gcs()
    bucket = gcs.bucket(BUCKET)

    dest_prefix, gcs_uri = resolve_archive_path(zip_name, origin)
    blob = bucket.blob(dest_prefix + zip_name)
    try:
        if not blob.exists():
            # El archivo no existía en GCS
            return (False, None, gcs_uri, dest_prefix)
        # Si existe, obtenemos la "generation" y lo borramos
        blob.reload()
        gen = int(blob.generation)
        blob.delete()
        return (True, gen, gcs_uri, dest_prefix)
    except NotFound:
        # Si justo entre medias no se encuentra, lo tratamos como no existente
        return (False, None, gcs_uri, dest_prefix)

def write_delete_log(zip_name: str, gcs_uri: str, reason: str, who: str,
                    generation: Optional[int], existed_before: bool,
                    dest_prefix: str) -> None:
    '''
    Escribir un log JSON en GCS con el detalle de la eliminación.
    '''
    gcs = get_gcs()
    bucket = gcs.bucket(BUCKET)
    log_doc = {
        "event": "delete",
        "zip_name": zip_name,
        "gcs_uri": gcs_uri,
        "ts": now_iso_utc(),
        "reason": reason,
        "deleted_by": who,
        "gcs_generation_last": generation,
        "existed_in_gcs_before": existed_before,
    }
    # Guardamos el log en logs/archive_delete/<origin>/<zip_name_sin_ext>.json
    cat = ""
    if dest_prefix.startswith(ARCHIVE_PREFIX) and len(dest_prefix) > len(ARCHIVE_PREFIX):
        cat = dest_prefix[len(ARCHIVE_PREFIX):]  # p.ej. 'public/'

    log_prefix = DELETE_LOG_PREFIX + (cat or "")
    log_blob = bucket.blob(log_prefix + os.path.splitext(zip_name)[0] + ".json")
    log_blob.upload_from_string(
        json.dumps(log_doc, ensure_ascii=False, indent=2),
        content_type="application/json",
    )


def bq_soft_delete(zip_name: str, gcs_uri: str, reason: str, who: str,
                   generation: Optional[int]) -> int:
    """
    Marca como borrada (soft-delete) la fila ACTIVA en BigQuery
    que corresponda a zip_name o gcs_uri.
    Devuelve el número de filas afectadas.
    """
    bq = get_bq()
    table_ref_full = f"{PROJECT}.{DATASET}.{TABLE}"

    job = bq.query(
        f"""
        UPDATE `{table_ref_full}`
        SET is_deleted = TRUE,
            ts_deleted = CURRENT_TIMESTAMP(),
            delete_reason = @reason,
            deleted_by = @who,
            exists_in_gcs = FALSE,
            gcs_generation_last = @gen
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND (zip_name = @zip OR gcs_uri = @uri)
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("reason", "STRING", reason),
                bigquery.ScalarQueryParameter("who", "STRING", who),
                bigquery.ScalarQueryParameter("gen", "INT64", generation),
                bigquery.ScalarQueryParameter("zip", "STRING", zip_name),
                bigquery.ScalarQueryParameter("uri", "STRING", gcs_uri),
            ]
        ),
    )
    res = job.result()
    # num_dml_affected_rows puede ser None en versiones antiguas, manejamos ambos casos
    return int(getattr(res, "num_dml_affected_rows", 0) or 0)

# CLI principal
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Safe delete: remove ZIP from GCS and mark soft-delete in BigQuery."
    )
    parser.add_argument("--zip", required=True, help="ZIP file name (e.g., my_dataset_2025-08-31.zip)")
    parser.add_argument("--reason", default="cleanup", help="Reason for deletion (default: cleanup)")
    parser.add_argument("--origin", choices=ORIGIN_CHOICES, help="Category where the ZIP lives (public|simulated|real|youtube). If omitted, it will be auto-resolved.")
    parser.add_argument("--who", default="uploader-cli", help="Actor performing the deletion")
    args = parser.parse_args()

    zip_name = os.path.basename(args.zip)

    # Borrado en GCS, si existe
    existed_before, generation, gcs_uri, dest_prefix = gcs_delete_zip(zip_name, args.origin)

    # Soft-delete en BigQuery (si hay fila activa)
    ## Si el objeto no existía, dejamos constancia igualmente en BQ para mantener consistencia
    effective_reason = args.reason if existed_before else f"{args.reason}|object-not-found"
    affected = bq_soft_delete(zip_name, gcs_uri, effective_reason, args.who, generation)

    # Log de borrado
    write_delete_log(zip_name, gcs_uri, effective_reason, args.who, generation, existed_before, dest_prefix)

    if existed_before:
        print(f"[OK] Deleted from GCS: {gcs_uri} (generation={generation})")
    else:
        print(f"[NOTE] Object not found in GCS: {gcs_uri}")

    if affected > 0:
        print(f"[OK] BigQuery soft-delete updated ({affected} row/s).")
    else:
        print("[WARN] No active BigQuery row found to soft-delete for this ZIP.")

if __name__ == "__main__":
    main()
