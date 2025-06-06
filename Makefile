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
	@echo "📦 Creating the vrtuall envvironment and preparing the projet..."
	python3 -m venv venv
	@echo "✅ Virtual envvironment created: 'venv/'"
	@echo "🐍 Activating virtuel environment and executing nstalation of dependencies..."
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt
	@echo "✅ Environment created correctly. Use 'source venv/bin/activate' to activate it."

help:
	@echo "Comandos disponibles:"
	@echo "  make run        - Execute the entire ETL (main.py)"
	@echo "  make extract    - Execute only the extration"
	@echo "  make transform  - Execute only the transformation"
	@echo "  make load       - Execute only the load"
	@echo "  make clean      - Delete the database and all the data"
	@echo "  make setup      - Setup the entire envvironment"
