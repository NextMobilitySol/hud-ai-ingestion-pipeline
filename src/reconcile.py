from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional

from google.cloud import bigquery, storage

# Variables de entorno requeridas
PROJECT = os.getenv("GCP_PROJECT")
BUCKET = os.getenv("GCS_BUCKET", "svr_object_storage")
DATASET = os.getenv("BQ_DATASET", "hud_project")
TABLE = os.getenv("BQ_TABLE_ARCHIVES", "archives_index")

# Prefijos en GCS
ARCHIVE_PREFIX = "archive/"
RECON_LOG_PREFIX = "logs/archive_reconcile/"

CATEGORIES = {"public", "simulated", "real", "youtube"}  # informativo


# Helpers
def now_iso_utc() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def get_bq() -> bigquery.Client:
    if not PROJECT:
        raise RuntimeError("Missing env var GCP_PROJECT.")
    return bigquery.Client(project=PROJECT)


def get_gcs() -> storage.Client:
    if not PROJECT:
        raise RuntimeError("Missing env var GCP_PROJECT.")
    return storage.Client(project=PROJECT)


# Utilidades
def list_gcs_archive_objects() -> Dict[str, List[Tuple[str, Optional[int], str]]]:
    """
    Lista todos los objetos dentro de gs://BUCKET/archive/**.
    Devuelve un índice por basename:
        { basename -> [(path_con_archivo, generation, category), ...] }
    donde:
        - path_con_archivo: p.ej. 'archive/public/LISA_images.zip'
        - category: 'public'|'simulated'|'real'|'youtube'|'' (vacío si sin subcarpeta)
    """
    client = get_gcs()
    gens: Dict[str, List[Tuple[str, Optional[int], str]]] = {}
    for blob in client.list_blobs(BUCKET, prefix=ARCHIVE_PREFIX):
        # Ignorar "directorios" vacíos
        name = blob.name
        if name.endswith("/"):
            continue  # "directorios"
        rel = name[
            len(ARCHIVE_PREFIX) :
        ]  # p.ej. 'public/LISA_images.zip' o 'LISA_images.zip'
        if "/" in rel:
            cat, base = rel.split("/", 1)
            category = cat
        else:
            base = rel
            category = ""
        try:
            gen = int(blob.generation) if blob.generation is not None else None
        except (TypeError, ValueError):
            gen = None
        gens.setdefault(base, []).append((name, gen, category))
    return gens


def fetch_active_bq_rows() -> List[dict]:
    """
    Obtiene todas las filas activas (is_deleted=FALSE) en la tabla de BigQuery.
    """
    bq = get_bq()
    table_ref_full = f"{PROJECT}.{DATASET}.{TABLE}"
    q = bq.query(
        f"""
        SELECT zip_name, gcs_uri, exists_in_gcs, is_deleted
        FROM `{table_ref_full}`
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        """
    )
    return list(q)


def fetch_deleted_bq_rows() -> List[dict]:
    """
    Filas soft-deleted (is_deleted=TRUE).
    """
    bq = get_bq()
    table_ref_full = f"{PROJECT}.{DATASET}.{TABLE}"
    q = bq.query(
        f"""
        SELECT zip_name, gcs_uri, exists_in_gcs, is_deleted
        FROM `{table_ref_full}`
        WHERE COALESCE(is_deleted, FALSE) = TRUE
        """
    )
    return list(q)


def resolve_gcs_match(
    zip_name: str,
    bq_gcs_uri: Optional[str],
    gcs_index: Dict[str, List[Tuple[str, Optional[int], str]]],
) -> Tuple[Optional[str], Optional[int], str]:
    """
    Intenta resolver el objeto real en GCS para un zip_name.
    Devuelve (path_con_archivo, generation, status), donde status ∈:
        - 'exact_uri'         -> coincide el gcs_uri exacto guardado en BQ
        - 'unique_basename'   -> hay un único objeto con ese basename
        - 'ambiguous'         -> hay múltiples candidatos (p.ej. public/ y real/)
        - 'not_found'         -> no existe en GCS
    """
    entries = gcs_index.get(zip_name, [])

    # 1) Si BQ tiene gcs_uri exacto, comprobamos si existe
    if bq_gcs_uri:
        prefix = f"gs://{BUCKET}/"
        if bq_gcs_uri.startswith(prefix):
            path = bq_gcs_uri[len(prefix) :]  # p.ej. 'archive/public/LISA_images.zip'
            for p, gen, _cat in entries:
                if p == path:
                    return p, gen, "exact_uri"

    # 2) Si hay un único basename en GCS, usamos ese
    if len(entries) == 1:
        p, gen, _cat = entries[0]
        return p, gen, "unique_basename"

    # 3) Ambigüedad o no encontrado
    if len(entries) == 0:
        return None, None, "not_found"
    return None, None, "ambiguous"


def bq_soft_delete_zip(zip_name: str, reason: str, who: str, dry_run: bool) -> None:
    """
    Marca como borrado (soft-delete) un zip_name activo en BigQuery.
    Si dry_run=True, solo imprime la acción en consola.
    """
    if dry_run:
        print(
            f"[DRY-RUN] Soft-delete BQ: zip_name={zip_name}, reason={reason}, who={who}"
        )
        return
    bq = get_bq()
    table_ref_full = f"{PROJECT}.{DATASET}.{TABLE}"
    bq.query(
        f"""
        UPDATE `{table_ref_full}`
        SET is_deleted = TRUE,
            ts_deleted = CURRENT_TIMESTAMP(),
            delete_reason = @reason,
            deleted_by = @who,
            exists_in_gcs = FALSE
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        AND zip_name = @zip
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("reason", "STRING", reason),
                bigquery.ScalarQueryParameter("who", "STRING", who),
                bigquery.ScalarQueryParameter("zip", "STRING", zip_name),
            ]
        ),
    ).result()


def bq_mark_exists(
    zip_name: str, generation: Optional[int], new_gcs_uri: Optional[str], dry_run: bool
) -> None:
    """
    Marca exists_in_gcs=TRUE en BQ para un zip_name activo
    si estaba en FALSE o NULL y el objeto existe en GCS.
    """
    if dry_run:
        print(
            f"[DRY-RUN] Mark exists_in_gcs=TRUE: zip_name={zip_name}, gen={generation}, uri={new_gcs_uri or '<keep>'}"
        )
        return
    bq = get_bq()
    table_ref_full = f"{PROJECT}.{DATASET}.{TABLE}"
    bq.query(
        f"""
        UPDATE `{table_ref_full}`
        SET exists_in_gcs = TRUE,
            gcs_generation_last = @gen,
            gcs_uri = IF(@uri IS NULL, gcs_uri, @uri)
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND zip_name = @zip
          AND (
                (exists_in_gcs IS NULL OR exists_in_gcs = FALSE)
                OR (@uri IS NOT NULL AND gcs_uri != @uri)
              )
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("gen", "INT64", generation),
                bigquery.ScalarQueryParameter("zip", "STRING", zip_name),
                bigquery.ScalarQueryParameter("uri", "STRING", new_gcs_uri),
            ]
        ),
    ).result()


def bq_reactivate_deleted(
    zip_name: str,
    generation: Optional[int],
    new_gcs_uri: Optional[str],
    who: str,
    dry_run: bool,
) -> None:
    """
    Reactiva una fila soft-deleted cuando el ZIP existe en GCS.
    """
    if dry_run:
        print(
            f"[DRY-RUN] Reactivate BQ row: {zip_name}, gen={generation}, uri={new_gcs_uri or '<keep>'}, by={who}"
        )
        return
    bq = get_bq()
    table_ref_full = f"{PROJECT}.{DATASET}.{TABLE}"
    bq.query(
        f"""
        UPDATE `{table_ref_full}`
        SET is_deleted = FALSE,
            ts_deleted = NULL,
            delete_reason = NULL,
            deleted_by = NULL,
            exists_in_gcs = TRUE,
            gcs_generation_last = @gen,
            gcs_uri = IF(@uri IS NULL, gcs_uri, @uri)
        WHERE COALESCE(is_deleted, FALSE) = TRUE
        AND zip_name = @zip
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("gen", "INT64", generation),
                bigquery.ScalarQueryParameter("zip", "STRING", zip_name),
                bigquery.ScalarQueryParameter("uri", "STRING", new_gcs_uri),
            ]
        ),
    ).result()


def write_reconcile_log(
    payload: dict, upload: bool, filename: Optional[str] = None
) -> None:
    """
    Escribe un log JSON con el resultado de la reconciliación.
    Siempre imprime en consola; si upload=True, también lo sube a GCS.
    """
    ts = now_iso_utc().replace(":", "").replace("-", "")
    name = filename or f"reconcile_{ts}.json"
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not upload:
        return
    client = get_gcs()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(RECON_LOG_PREFIX + name)
    blob.upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    print(f"[OK] Reconcile log uploaded: gs://{BUCKET}/{RECON_LOG_PREFIX}{name}")


# CLI principal
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Reconcile BigQuery rows with GCS archive/ objects (supports archive/<category>/...)."
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Show actions without applying changes"
    )
    ap.add_argument(
        "--reason",
        default="reconcile-missing",
        help="Soft-delete reason to set when ZIP is missing in GCS",
    )
    ap.add_argument("--who", default="reconcile-cli", help="Actor label for updates")
    ap.add_argument(
        "--upload-log",
        action="store_true",
        help="Upload a JSON report to logs/archive_reconcile/",
    )
    ap.add_argument(
        "--include-deleted",
        action="store_true",
        help="Also analyze soft-deleted rows (report-only unless combined with --reactivate-deleted).",
    )
    ap.add_argument(
        "--reactivate-deleted",
        action="store_true",
        help="If a row is soft-deleted but the ZIP exists in GCS, reactivate it. Implies --include-deleted.",
    )
    args = ap.parse_args()

    # Si reactivamos, forzamos incluir borradas
    if args.reactivate_deleted:
        args.include_deleted = True

    # Inventario GCS: índice por basename -> lista de candidatos (ruta completa, gen, categoría)
    gcs_index = list_gcs_archive_objects()  # Dict[str, List[(path, gen, cat)]]
    gcs_basenames: Set[str] = set(gcs_index.keys())
    duplicates_basenames: Set[str] = {
        bn for bn, entries in gcs_index.items() if len(entries) > 1
    }

    # Filas activas en BQ
    rows_active = fetch_active_bq_rows()

    # Listas de trabajo
    missing_in_gcs: List[str] = []
    ambiguous_in_gcs: List[str] = []
    fixed_exists_flag: List[str] = []
    fixed_wrong_uri: List[str] = []
    deleted_but_exists: List[str] = []
    reactivated: List[str] = []
    untracked_in_bq: List[str] = sorted(
        [bn for bn in gcs_basenames if bn not in {r["zip_name"] for r in rows_active}]
    )

    # Filas activas
    for r in rows_active:
        zip_name = r["zip_name"]
        bq_uri = r.get("gcs_uri")
        exists_flag = r.get("exists_in_gcs")

        # Si hay duplicados para este basename, reportamos AMBIGUO y no tocamos BQ
        if zip_name in duplicates_basenames:
            ambiguous_in_gcs.append(zip_name)
            continue

        resolved_path, generation, status = resolve_gcs_match(
            zip_name, bq_uri, gcs_index
        )

        if status == "not_found":
            missing_in_gcs.append(zip_name)
            bq_soft_delete_zip(
                zip_name, reason=args.reason, who=args.who, dry_run=args.dry_run
            )
            continue

        if status == "ambiguous":
            ambiguous_in_gcs.append(zip_name)
            continue

        # Existe en GCS (exact_uri o unique_basename)
        real_uri = f"gs://{BUCKET}/{resolved_path}" if resolved_path else None

        # 1) exists_in_gcs == False/NULL  -> corregimos a TRUE (y guardamos gen + gcs_uri real)
        if exists_flag in (False, None):
            bq_mark_exists(
                zip_name,
                generation=generation,
                new_gcs_uri=real_uri,
                dry_run=args.dry_run,
            )
            fixed_exists_flag.append(zip_name)
        else:
            # 2) exists_in_gcs ya TRUE, pero gcs_uri distinto -> lo alineamos
            if real_uri and bq_uri and real_uri != bq_uri:
                bq_mark_exists(
                    zip_name,
                    generation=generation,
                    new_gcs_uri=real_uri,
                    dry_run=args.dry_run,
                )
                fixed_wrong_uri.append(zip_name)

    # Filas borradas (opcional)
    if args.include_deleted:
        rows_deleted = fetch_deleted_bq_rows()
        for r in rows_deleted:
            zip_name = r["zip_name"]

            # Si hay duplicados, solo reportamos ambigüedad
            if zip_name in duplicates_basenames:
                ambiguous_in_gcs.append(zip_name)
                continue

            resolved_path, generation, status = resolve_gcs_match(
                zip_name, r.get("gcs_uri"), gcs_index
            )

            if status in ("exact_uri", "unique_basename"):
                real_uri = f"gs://{BUCKET}/{resolved_path}" if resolved_path else None
                if args.reactivate_deleted:
                    bq_reactivate_deleted(
                        zip_name,
                        generation,
                        real_uri,
                        who=args.who,
                        dry_run=args.dry_run,
                    )
                    reactivated.append(zip_name)
                else:
                    deleted_but_exists.append(zip_name)
            # not_found -> nada: ya está borrada y no está en GCS
            # ambiguous -> ya lo añadimos arriba en caso de duplicados

    # Construir y mostrar informe de reconciliación
    report = {
        "event": "reconcile",
        "bucket": BUCKET,
        "ts": now_iso_utc(),
        "summary": {
            "bq_active_rows": len(rows_active),
            "gcs_basenames": len(gcs_basenames),
            "missing_in_gcs": len(missing_in_gcs),
            "ambiguous_in_gcs": len(set(ambiguous_in_gcs)),
            "fixed_exists_flag": len(fixed_exists_flag),
            "fixed_wrong_uri": len(fixed_wrong_uri),
            "untracked_in_bq": len(untracked_in_bq),
            "deleted_but_exists": len(deleted_but_exists),
            "reactivated": len(reactivated),
        },
        "details": {
            "missing_in_gcs": sorted(set(missing_in_gcs)),
            "ambiguous_in_gcs": sorted(set(ambiguous_in_gcs)),
            "fixed_exists_flag": sorted(set(fixed_exists_flag)),
            "fixed_wrong_uri": sorted(set(fixed_wrong_uri)),
            "untracked_in_bq": untracked_in_bq[:200],
            "deleted_but_exists": sorted(set(deleted_but_exists)),
            "reactivated": sorted(set(reactivated)),
        },
    }
    write_reconcile_log(report, upload=args.upload_log)


if __name__ == "__main__":
    main()
