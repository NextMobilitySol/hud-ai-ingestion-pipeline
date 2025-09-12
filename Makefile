# ============================================================
# Makefile — HUD AI Ingestion Pipeline
# Requisitos: Poetry 1.8+ y versión de Python definida en pyproject
# Uso: ejecutar `make <objetivo>` desde la raíz del proyecto
# IMPORTANTE: Este Makefile está escrito en sintaxis bash (no funciona en PowerShell).
#             En Windows se debe ejecutar desde Git Bash o WSL.
# ============================================================

# Incluyo src/ en el PYTHONPATH para poder importar módulos sin instalar el paquete
SHELL := /usr/bin/bash
export PYTHONPATH := src
PY := poetry run python


# Ayuda / referencia
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make setup         -> Install dependencies with Poetry (without packaging the project)"
	@echo "  make envfile       -> Generate .env from env/sample.env if it doesn't exist"
	@echo "  make fmt|lint|test -> Code quality: format, lint, test (if tests/ exists)"
	@echo "  make upload        -> Upload ZIP (requires ZIP, ORIGIN, DATASET; URL if ORIGIN=youtube)"
	@echo "  make delete        -> Safe delete (requires ZIP; optional ORIGIN; needs REASON, WHO)"
	@echo "  make reconcile-dry -> Reconcile (dry-run)"
	@echo "  make reconcile     -> Reconcile and upload report"
	@echo "  make reactivate    -> Reactivate soft-deleted rows when object exists (needs WHO)"
	@echo "  make clean         -> Clean generated artifacts and caches"

# Setup del entorno
.PHONY: setup
setup:
	@command -v poetry >/dev/null 2>&1 || { echo >&2 "Poetry no está instalado. Ver: https://python-poetry.org/docs/"; exit 1; }
	poetry install
	@$(MAKE) envfile

# Creo archivo .env automáticamente si no existe, usando env/sample.env como base
.PHONY: envfile
envfile:
	@if [ ! -f .env ]; then \
		if [ -f env/sample.env ]; then \
			cp env/sample.env .env; \
			echo "[OK] .env created from env/sample.env"; \
		else \
			echo "[WARN] env/sample.env does not exist; please create your variables manually"; \
		fi \
	else \
		echo "[SKIP] .env already exists"; \
	fi

# Calidad de código: Formateo automático del código con Black
.PHONY: fmt
fmt:
	@poetry run python -c "import sys,importlib.util as u; sys.exit(0 if u.find_spec('black') else 1)" || { echo '[INFO] Black no está instalado (grupo dev).'; exit 0; }
	poetry run black src tests

# Análisis estático de código con Ruff: Búsqueda de problemas de estilo, errores comunes y mejoras posibles
.PHONY: lint
lint:
	@poetry run python -c "import sys,importlib.util as u; sys.exit(0 if u.find_spec('ruff') else 1)" || { echo '[INFO] Ruff no está instalado (grupo dev).'; exit 0; }
	poetry run ruff check src tests

# Ejecución de tests
# Ejecución de pruebas con Pytest, si existe carpeta tests/
.PHONY: test
test:
	@poetry run python -c "import sys,importlib.util as u; sys.exit(0 if u.find_spec('pytest') else 1)" || { echo '[INFO] Pytest no está instalado (grupo dev).'; exit 0; }
	@if [ -d tests ]; then \
		poetry run pytest -q; \
	else \
		echo "[SKIP] No hay carpeta tests/"; \
	fi

# Ejecución local
## Unificado CLI
.PHONY: upload
upload:        ## Requires: ZIP=/path/to.zip ORIGIN=public|simulated|real|youtube DATASET=name [URL=... if youtube]
	@if [ -z "$(ZIP)" ] || [ -z "$(ORIGIN)" ] || [ -z "$(DATASET)" ]; then \
		echo "Usage: make upload ZIP=/path/file.zip ORIGIN=public DATASET=myset [URL=...]"; exit 2; \
	fi
	@if [ "$(ORIGIN)" = "youtube" ] && [ -z "$(URL)" ]; then \
		echo "For ORIGIN=youtube you must pass URL=..."; exit 2; \
	fi
	$(PY) -m main upload --zip "$(ZIP)" --origin "$(ORIGIN)" --dataset "$(DATASET)" $(if $(URL),--url "$(URL)",)

.PHONY: delete
delete:        ## Requires: ZIP=name.zip REASON=text WHO=actor [ORIGIN=...]
	@if [ -z "$(ZIP)" ] || [ -z "$(REASON)" ] || [ -z "$(WHO)" ]; then \
		echo "Usage: make delete ZIP=name.zip REASON='cleanup' WHO='cli' [ORIGIN=public]"; exit 2; \
	fi
	$(PY) -m main delete --zip "$(ZIP)" $(if $(ORIGIN),--origin "$(ORIGIN)",) --reason "$(REASON)" --who "$(WHO)"

.PHONY: reconcile-dry
reconcile-dry:
	$(PY) -m main reconcile --dry-run

.PHONY: reconcile
reconcile:
	$(PY) -m main reconcile --upload-log

.PHONY: reactivate  ## Requires: WHO=actor
reactivate:
	@if [ -z "$(WHO)" ]; then echo "Usage: make reactivate WHO='cli'"; exit 2; fi
	$(PY) -m main reconcile --reactivate-deleted --who "$(WHO)" --upload-log

# Limpieza: Elimino cachés y artefactos de compilación para dejar el repositorio limpio
.PHONY: clean
clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .pytest_cache .mypy_cache build dist *.egg-info 2>/dev/null || true
	@echo "[OK] Cleanup completed"
