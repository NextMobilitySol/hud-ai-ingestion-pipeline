# **HUD AI Ingestion Pipeline**

> **Propósito:** orquestar la **ingesta**, **borrado seguro** y **reconciliación** de artefactos ZIP en el **Data Lake (GCS)** y mantener el **índice lógico** en el **Data Warehouse (BigQuery)** **consistente, auditable e idempotente**.

## **Índice**

* [Visión general](#visión-general)
* [Arquitectura (alto nivel)](#arquitectura-alto-nivel)
* [Componentes](#componentes)
* [Esquema de datos](#esquema-de-datos)
* [Estructura del repositorio](#estructura-del-repositorio)
* [Configuración y requisitos](#configuración-y-requisitos)
* [Uso — CLI unificada](#uso--cli-unificada)
* [Makefile](#makefile)
* [Reglas de reconciliación](#reglas-de-reconciliación)
* [Observabilidad y auditoría](#observabilidad-y-auditoría)
* [Seguridad y permisos](#seguridad-y-permisos)
* [SOP — Casuística típica](#sop--casuística-típica)
* [Pruebas](#pruebas)
* [Hoja de ruta](#hoja-de-ruta)
* [Glosario breve](#glosario-breve)

## **Visión general**

Este proyecto materializa un *data loop* sencillo pero robusto:

* **Ingesta (uploader):** sube un ZIP al Lake, escribe metadatos como **object metadata** y registra una fila en el Warehouse.
* **Borrado seguro (delete):** elimina el objeto del Lake (si existe), **SSOT** en Warehouse y log de auditoría.
* **Reconciliación (reconcile):** compara **estado físico** (GCS) vs **estado lógico** (BQ), **corrige lo cierto** (flags/URI) y **reporta lo dudoso** (duplicados, huérfanos), con opción de **reactivar** tombstones si reaparece evidencia física.

**Principios:** *Single source of truth (SSOT)* por archivo, trazabilidad completa (logs), idempotencia y políticas conservadoras ante ambigüedad.

## **Arquitectura (alto nivel)**

* **Data Lake (GCS)**
  Prefijo: `archive/<categoría>/...` con categorías **`public|simulated|real|youtube`** (y tolerancia para `archive/<zip>` sin categoría).
* **Data Warehouse (BigQuery)**
  Tabla `archives_index` que indexa los artefactos, su procedencia/origen y estado de ciclo de vida.
* **Observabilidad**
  Logs JSON en GCS:

  * `logs/archive_ingest/`
  * `logs/archive_delete/`
  * `logs/archive_reconcile/`

*(introducir foto del diagrama de arquitectura general)*

## Componentes

### `uploader.py`

* Sube `--zip` a `gs://<BUCKET>/archive/<origin>/<zip_name>`.
* Escribe **object metadata** (sha256, num\_images, ts\_ingest, etc.).
* Log JSON en `logs/archive_ingest/`.
* Inserta/actualiza fila en BQ (`archives_index`) con `gcs_uri`, `origin`, `exists_in_gcs=TRUE`, etc.
* Para `--origin youtube` requiere `--url` y persiste metadatos del vídeo.

*(introducir foto del diagrama de uploader.py)*

### `delete_zip.py`

* Borra `archive/<origin>/<zip>` si `--origin` es explícito.
  Sin `--origin`, **resuelve** buscando en todas las categorías:

  * **1 coincidencia** → borra ahí.
  * **>1 coincidencia** → aborta (evita borrar la copia equivocada).
  * **0 coincidencias** → no hay objeto; continúa para trazar y sincronizar BQ.
* **Soft-delete en BQ** (tombstone): `is_deleted=TRUE`, `exists_in_gcs=FALSE`, `delete_reason`, `deleted_by`, `gcs_generation_last`.
* Log JSON en `logs/archive_delete/`.

*(introducir foto del diagrama de delete\_zip.py)*

### `reconcile.py`

* Escanea `archive/**` y **agrupe por basename** (p.ej., `LISA_images.zip`).
* Detecta **duplicados** (mismo basename en ≥2 rutas) → **ambiguous**: **solo informa**, **no** muta BQ.
* **Activos en BQ pero ausentes en GCS** → **tombstone** en BQ.
* **Archivo existe pero flags/URI incorrectos** → corrige `exists_in_gcs=TRUE`, `gcs_uri` y `gcs_generation_last`.
* Opciones:

  * `--dry-run` (no cambios), `--upload-log` (sube reporte),
  * `--include-deleted` (analiza tombstones),
  * `--reactivate-deleted` (revive tombstones si el archivo existe).
* Reporta también **huérfanos** (objeto en Lake sin fila activa en BQ).

*(introducir foto del diagrama de reconcile.py)*

---

## Esquema de datos

**Tabla:** `${PROJECT}.${BQ_DATASET}.${BQ_TABLE_ARCHIVES}` (particionada por `DATE(ts_ingest)`; cluster por `dataset, origin, zip_name`).

Campos principales:

* Identificación y estado:
  `zip_name`, `gcs_uri`, `exists_in_gcs` (BOOL), `is_deleted` (BOOL), `gcs_generation_last` (INT64)
* Origen y procedencia:
  `origin` (`public|simulated|real|youtube`), `source_url`, `dataset` (lógico)
* Ingesta y tamaños:
  `sha256_zip`, `zip_size_bytes`, `num_images`, `ts_ingest`
* Borrado lógico (tombstone):
  `ts_deleted`, `delete_reason`, `deleted_by`
* YouTube (STRUCT opcional):
  `youtube.video_id`, `youtube.title`, `youtube.channel`, `youtube.publish_date`, `youtube.license`

*(introducir foto del diagrama del modelo de datos / tabla)*

---

## Estructura del repositorio

```
.
├─ src/
│  ├─ main.py            # CLI unificada (subcomandos: upload, delete, reconcile)
│  ├─ uploader.py        # Ingesta en Lake + índice en Warehouse + logs
│  ├─ delete_zip.py      # Borrado seguro en Lake + tombstone en Warehouse + log
│  ├─ reconcile.py       # Sincronización Lake↔Warehouse + reporte
│  └─ utils_youtube.py   # Helpers para metadatos de YouTube
├─ docs/
│  └─ diagrams/          # (introducir fotos de diagramas aquí)
├─ sql/
│  ├─ ddl/archives_index.sql      # DDL recomendado
│  └─ views/{v_archives_active.sql, v_archives_review.sql}
├─ tests/                # (opcional) unit tests con mocks de GCS/BQ
├─ Makefile              # Atajos (bash) y autoload de .env
├─ .env.sample           # Variables de entorno de ejemplo
└─ README.md
```

---

## Configuración y requisitos

* **Python** (versión fijada en `pyproject.toml`).
* **Poetry** ≥ 1.8.
* **Credenciales GCP** con permisos adecuados.
* **Variables de entorno** (via `.env`):

  ```
  GCP_PROJECT=...
  GCS_BUCKET=svr_object_storage
  BQ_DATASET=hud_project
  BQ_TABLE_ARCHIVES=archives_index
  UPLOADER_VERSION=uploader-v1
  ```

> El **Makefile** carga automáticamente `.env` al ejecutar los targets.

---

## Uso — CLI unificada

Ver ayuda:

```bash
python -m src.main -h
python -m src.main upload -h
python -m src.main delete -h
python -m src.main reconcile -h
```

### Upload (ingesta)

```bash
python -m src.main upload --zip /path/LISA_images.zip --origin public --dataset lisa
# YouTube:
python -m src.main upload --zip /path/yt_abc123.zip --origin youtube --url "https://youtu.be/abc123" --dataset yt_set
```

### Delete (borrado seguro)

```bash
python -m src.main delete --zip LISA_images.zip --origin public --reason "cleanup" --who "cli"
# Sin --origin intenta resolver; si hay duplicado, aborta para evitar errores.
```

### Reconcile (sincronizar Lake↔Warehouse)

```bash
# Vista previa
python -m src.main reconcile --dry-run

# Aplicar y subir informe
python -m src.main reconcile --upload-log

# Incluir tombstones en el análisis
python -m src.main reconcile --include-deleted --dry-run

# Reactivar tombstones si el archivo existe
python -m src.main reconcile --reactivate-deleted --who "reconcile-cli" --upload-log
```

---

## Makefile

Atajos (bash; usa Git Bash/WSL en Windows). Carga `.env` automáticamente:

```bash
# Variables clave
make env-check

# Ingesta
make upload ZIP="/path/LISA_images.zip" ORIGIN=public DATASET=lisa

# Borrado seguro
make delete ZIP="LISA_images.zip" ORIGIN=public REASON="cleanup" WHO="cli"

# Reconciliar (dry / apply)
make reconcile-dry
make reconcile

# Reactivar tombstones si el archivo existe
make reactivate WHO="reconcile-cli"
```

---

## Reglas de reconciliación

1. **Duplicado (ambiguous)**
   Mismo basename en ≥2 rutas del Lake → **no muta BQ**, solo reporta `ambiguous_in_gcs`.
2. **Activo en BQ pero ausente en GCS**
   **Tombstone**: `is_deleted=TRUE`, `exists_in_gcs=FALSE`, `delete_reason`.
3. **Existe en GCS pero flags/URI desalineados**
   Corrige `exists_in_gcs=TRUE`, `gcs_uri` canónica y `gcs_generation_last`.
4. **Tombstone en BQ y el archivo existe**

   * `--include-deleted`: solo informe (`deleted_but_exists`).
   * `--reactivate-deleted`: **reactiva** fila (`is_deleted=FALSE`, limpia motivos, alinea URI y generation).
5. **Huérfano (objeto en Lake sin fila activa en BQ)**
   Solo informe (`untracked_in_bq`); no inserta automáticamente.
6. **Edge sin categoría (`archive/<zip>`)**
   Se trata como ubicación válida; puede corregir flags/URI. Si además existe en una categoría, se considera **ambiguous**.

---

## Observabilidad y auditoría

* **Ingesta:** `logs/archive_ingest/<zip>.json`
* **Borrado:** `logs/archive_delete/<zip>.json`
* **Reconcile:** `logs/archive_reconcile/<timestamp>.json` (summary + details)

Campos típicos: evento, `zip_name`, `gcs_uri`, `ts`, `reason`, `deleted_by`, `gcs_generation_last`, indicadores de existencia y listas de casos.

*(introducir foto del diagrama de observabilidad/logs)*

---

## Seguridad y permisos

Rol mínimo para la cuenta de ejecución:

* **Storage:** `roles/storage.objectAdmin`
* **BigQuery:** `roles/bigquery.dataEditor` + `roles/bigquery.jobUser`

> Recomendado: cuenta de servicio dedicada, rotación de credenciales y límites por entorno (dev/stg/prod).

---

## SOP — Casuística típica

* **Falta en Lake (borrado manual):** ejecutar `reconcile` y validar tombstone en BQ.
* **Duplicado detectado:** eliminar copia “sobrante” y volver a `reconcile`.
* **Fila borrada pero archivo existe:** `reconcile --reactivate-deleted`.
* **Edge sin categoría:** reconciliar; luego **migrar** con una tarea específica si procede.

*(introducir foto del diagrama de decisiones de reconcile)*

---

## Pruebas

* **Unit tests** con mocks de GCS/BQ (carpeta `tests/`).
* **Casos clave:** `missing`, `ambiguous`, `reactivate`, `wrong_uri`, `orphan`, `uncategorized`.
* **Vistas útiles en BQ** (sugeridas en `sql/views/`):

  * `v_archives_active` → consumibles (`is_deleted=FALSE AND exists_in_gcs=TRUE`)
  * `v_archives_review` → candidatos a revisión (flags raros)

---

## Hoja de ruta

* Script opcional de **migración** `archive/<zip>` → `archive/<category>/<zip>`.
* Métricas operativas (SLO de reconciliación): % activos alineados, % duplicados, MTTR de inconsistencias.
* Pipeline de **curación** post-ingesta (descomprimir, normalizar, indexar imágenes).

---

## Glosario breve

* **Lake (GCS):** almacenamiento físico de artefactos.
* **Warehouse (BQ):** catálogo lógico indexado para consumo.
* **Tombstone / Soft-delete:** registro marcado eliminado pero no purgado (`is_deleted=TRUE`).
* **Canonical URI:** `gcs_uri` que refleja la ruta real del objeto.
* **Ambiguous:** mismo basename en varias rutas → requiere resolución manual.
