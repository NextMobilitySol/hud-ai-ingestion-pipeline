# ============================================================
# Makefile — HUD AI Ingestion Pipeline
# Requisitos: Poetry 1.8+ y versión de Python definida en pyproject
# Uso: ejecutar `make <objetivo>` desde la raíz del proyecto
# IMPORTANTE: Este Makefile está escrito en sintaxis bash (no funciona en PowerShell).
#             En Windows se debe ejecutar desde Git Bash o WSL.
# ============================================================

# Incluyo src/ en el PYTHONPATH para poder importar módulos sin instalar el paquete
export PYTHONPATH := src


# Ayuda / referencia
.PHONY: help
help:
	@echo "Comandos disponibles:"
	@echo "  make setup       -> Instala dependencias con Poetry (sin empaquetar el proyecto)"
	@echo "  make envfile     -> Genera .env a partir de env/sample.env si no existe"
	@echo "  make fmt         -> Formatea código con Black (si está instalado)"
	@echo "  make lint        -> Revisa estilo de código con Ruff (si está instalado)"
	@echo "  make typecheck   -> Verifica tipos estáticos con MyPy (si está instalado)"
	@echo "  make test        -> Ejecuta pruebas con Pytest (si está instalado)"
	@echo "  make run-local   -> Placeholder para ejecución local del pipeline"
	@echo "  make clean       -> Limpia artefactos y cachés generados"


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
			echo "[OK] .env creado desde env/sample.env"; \
		else \
			echo "[WARN] env/sample.env no existe; crea tus variables manualmente"; \
		fi \
	else \
		echo "[SKIP] .env ya existe"; \
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

# Chequeo de tipado estático con MyPy: Comprobación de tipos en tiempo estático, revisión de correcto uso de anotaciones de tipo (str, int, etc.)
.PHONY: typecheck
typecheck:
	@poetry run python -c "import sys,importlib.util as u; sys.exit(0 if u.find_spec('mypy') else 1)" || { echo '[INFO] MyPy no está instalado (grupo dev).'; exit 0; }
	poetry run mypy src


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
## Placeholder: más adelante definiré cómo ejecutar el pipeline en local
.PHONY: run-local
run-local:
	@echo "TODO: definir ejecución CLI/pipeline local en su fase (feature/pipeline-cli)."
	@echo "PYTHONPATH actualmente es: $(PYTHONPATH)"

# Limpieza: Elimino cachés y artefactos de compilación para dejar el repositorio limpio
.PHONY: clean
clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .pytest_cache .mypy_cache build dist *.egg-info 2>/dev/null || true
	@echo "[OK] Limpieza completada"
