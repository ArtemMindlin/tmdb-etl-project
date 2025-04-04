import sqlite3
import pandas as pd
from pathlib import Path


def load_csv_to_sqlite(table_name: str, csv_path: Path, con: sqlite3.Connection):
    """
    Carga un archivo CSV a una tabla en SQLite.
    """
    try:
        print(f"📥 Cargando {csv_path} en la tabla '{table_name}'...")
        df = pd.read_csv(csv_path, quotechar='"', engine="python")
        df.to_sql(table_name, con=con, if_exists="replace", index=False)
        print(f"✅ Tabla '{table_name}' cargada con {len(df)} filas.\n")
    except Exception as e:
        print(f"❌ Error al cargar {table_name}: {e}\n")


def main():
    # Diccionario con los archivos y nombres de tabla
    csv_files = {
        "popular": Path("../data/processed/tmdb_popular.csv"),
        "top_rated": Path("../data/processed/tmdb_top_rated.csv"),
        "upcoming": Path("../data/processed/tmdb_upcoming.csv"),
    }

    # Conexión a la base de datos SQLite
    db_path = Path("../sql/tmdb_etl.db")
    con = sqlite3.connect(db_path)

    for table_name, csv_path in csv_files.items():
        load_csv_to_sqlite(table_name, csv_path, con)

    con.close()
    print("🚀 ¡Carga de CSVs completada con éxito!")


if __name__ == "__main__":
    main()
