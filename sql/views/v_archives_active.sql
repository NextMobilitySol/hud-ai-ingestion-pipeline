-- Propósito: Seleccionar solo los ZIP activos y utilizables
-- Criterio: No borrados (is_deleted=FALSE) y existen en GCS (exists_in_gcs=TRUE)

CREATE OR REPLACE VIEW `@@PROJECT_ID@@.@@BQ_DATASET@@.v_archives_active` AS
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
  gcs_generation_last
FROM `@@PROJECT_ID@@.@@BQ_DATASET@@.archives_index`
WHERE is_deleted = FALSE
  AND exists_in_gcs = TRUE
ORDER BY ts_ingest DESC;
