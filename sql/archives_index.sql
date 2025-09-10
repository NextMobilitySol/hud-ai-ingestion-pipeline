-- BigQuery DDL for HUD archives_index

CREATE TABLE IF NOT EXISTS `hud_project.archives_index` (
  gcs_uri           STRING OPTIONS(description="GCS URI of the ZIP in archive/"),
  zip_name          STRING OPTIONS(description="ZIP file name"),
  sha256_zip        STRING OPTIONS(description="SHA-256 hash of the ZIP"),
  zip_size_bytes    INT64  OPTIONS(description="Size of the ZIP in bytes"),
  origin            STRING OPTIONS(description="Source type: youtube|public|simulated|real"),
  source_url        STRING OPTIONS(description="Original source URL (YouTube or dataset page)"),
  dataset           STRING OPTIONS(description="Logical dataset name"),
  num_images        INT64  OPTIONS(description="Number of image files inside the ZIP"),
  ts_ingest         TIMESTAMP OPTIONS(description="Ingestion timestamp (UTC)"),

  youtube STRUCT<
    video_id     STRING   OPTIONS(description="YouTube video ID"),
    title        STRING   OPTIONS(description="YouTube video title"),
    channel      STRING   OPTIONS(description="YouTube channel name"),
    publish_date DATE     OPTIONS(description="YouTube publish date (YYYY-MM-DD)"),
    license      STRING   OPTIONS(description="YouTube license")
  > OPTIONS(description="YouTube metadata when origin=youtube"),

  exists_in_gcs       BOOL      OPTIONS(description="Object currently exists in GCS"),
  is_deleted          BOOL      OPTIONS(description="Soft-delete flag"),
  ts_deleted          TIMESTAMP OPTIONS(description="Soft-delete timestamp"),
  delete_reason       STRING    OPTIONS(description="Reason for soft-delete"),
  deleted_by          STRING    OPTIONS(description="Actor who performed the delete"),
  gcs_generation_last INT64     OPTIONS(description="Last GCS generation number")
)
PARTITION BY DATE(ts_ingest)
CLUSTER BY origin, dataset
OPTIONS(
  description="Registry of ZIPs ingested into archive/ with source metadata, YouTube info, and lifecycle flags"
);
