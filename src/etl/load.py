import sqlite3
from pathlib import Path
import pandas as pd


def load_data(dfs: dict):
    """
    Carga un diccionario de DataFrames a una base de datos SQLite.

    Args:
        dfs (dict): Diccionario con claves como 'popular', 'top_rated', etc., y valores pandas.DataFrame.
    """
    db_path = Path("sql/tmdb_etl.db")
    con = sqlite3.connect(db_path)

    for table_name, df in dfs.items():
        try:
            df.to_sql(table_name, con=con, if_exists="replace", index=False)
            print(f"✅ Tabla '{table_name}' cargada con {len(df)} registros.")
        except Exception as e:
            print(f"❌ Error al cargar la tabla '{table_name}': {e}")

    con.close()
    print("🚀 Carga completa en SQLite.")


def main():
    """
    Carga los archivos CSV procesados directamente a SQLite.
    Útil si ejecutas este archivo de forma independiente.
    """
    csv_files = {
        "popular": Path("data/processed/tmdb_popular.csv"),
        "top_rated": Path("data/processed/tmdb_top_rated.csv"),
        "upcoming": Path("data/processed/tmdb_upcoming.csv"),
        "genres": Path("data/processed/tmdb_genres.csv"),
    }

    db_path = Path("sql/tmdb_etl.db")
    con = sqlite3.connect(db_path)

    for table_name, csv_path in csv_files.items():
        try:
            df = pd.read_csv(csv_path, engine="python", quotechar='"')
            df.to_sql(table_name, con=con, if_exists="replace", index=False)
            print(f"✅ Tabla '{table_name}' cargada desde CSV con {len(df)} registros.")
        except Exception as e:
            print(f"❌ Error al cargar '{table_name}' desde CSV: {e}")

    con.close()
    print("🚀 Carga desde CSV completada.")


if __name__ == "__main__":
    main()
