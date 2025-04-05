# Variables
PYTHON=python

# Targets
run:
	$(PYTHON) main.py

extract:
	$(PYTHON) src/etl/extract.py

transform:
	$(PYTHON) src/etl/transform.py

load:
	$(PYTHON) src/etl/load.py

clean:
	rm -f sql/tmdb_etl.db
	rm -f data/processed/*.csv
	rm -f data/processed/*.parquet

setup:
	@echo "📦 Creando entorno virtual y preparando el proyecto..."
	python3 -m venv venv
	@echo "✅ Entorno virtual creado: 'venv/'"
	@echo "🐍 Activando entorno y ejecutando instalación de dependencias..."
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt
	@echo "✅ Entorno configurado correctamente. Usa 'source venv/bin/activate' para activarlo."

help:
	@echo "Comandos disponibles:"
	@echo "  make run        - Ejecuta el pipeline completo (main.py)"
	@echo "  make extract    - Ejecuta solo la extracción"
	@echo "  make transform  - Ejecuta solo la transformación"
	@echo "  make load       - Ejecuta solo la carga desde CSV"
	@echo "  make clean      - Borra la base de datos y los archivos procesados"
