-- Propósito: Listar filas con inconsistencias o que requieren revisión
-- Ejemplos: Borrados pero aún existen en GCS, faltan hashes, URIs vacíos, etc.

CREATE OR REPLACE VIEW `@@PROJECT_ID@@.@@BQ_DATASET@@.v_archives_review` AS
WITH base AS (
  SELECT
    ai.*,
    ARRAY_CONCAT(
      IF(is_deleted AND exists_in_gcs,              ['deleted_but_exists'],      []),
      IF(NOT is_deleted AND NOT exists_in_gcs,      ['missing_in_gcs'],          []),
      IF(gcs_uri IS NULL OR gcs_uri = '',           ['missing_gcs_uri'],         []),
      IF(sha256_zip IS NULL OR sha256_zip = '',     ['missing_sha256'],          []),
      IF(num_images IS NULL OR num_images = 0,      ['suspicious_num_images'],   [])
    ) AS review_reasons
  FROM `@@PROJECT_ID@@.@@BQ_DATASET@@.archives_index` AS ai
)
SELECT
  gcs_uri,
  zip_name,
  origin,
  dataset,
  source_url,
  sha256_zip,
  zip_size_bytes,
  num_images,
  ts_ingest,
  youtube,
  exists_in_gcs,
  is_deleted,
  ts_deleted,
  delete_reason,
  deleted_by,
  gcs_generation_last,
  review_reasons
FROM base
WHERE ARRAY_LENGTH(review_reasons) > 0
ORDER BY ts_ingest DESC;
