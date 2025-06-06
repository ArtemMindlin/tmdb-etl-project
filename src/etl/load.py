import sqlite3
from pathlib import Path
import pandas as pd


def load_data():
    """
    Loads the processed CSV files directly to SQLite.
    Usefull if you run this script independently
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
            print(f"✅ Table '{table_name}' loaded from a CSV {len(df)} instances.")
        except Exception as e:
            print(f"❌ Error loading '{table_name}' from CSV: {e}")

    con.close()
    print("🚀 Loaded to SQLite.")


def main():
    """
    Loads the processed CSV files directly to SQLite.
    Usefull if you run this script independently
    """
    csv_files = {
        "popular": Path("../../data/processed/tmdb_popular.csv"),
        "top_rated": Path("../../data/processed/tmdb_top_rated.csv"),
        "upcoming": Path("../../data/processed/tmdb_upcoming.csv"),
        "genres": Path("../../data/processed/tmdb_genres.csv"),
    }

    db_path = Path("../../sql/tmdb_etl.db")
    con = sqlite3.connect(db_path)

    for table_name, csv_path in csv_files.items():
        try:
            df = pd.read_csv(csv_path, engine="python", quotechar='"')
            df.to_sql(table_name, con=con, if_exists="replace", index=False)
            print(f"✅ Table '{table_name}' loaded from a CSV {len(df)} instances.")
        except Exception as e:
            print(f"❌ Error loading '{table_name}' from CSV: {e}")

    con.close()
    print("🚀 Loaded to SQLite.")


if __name__ == "__main__":
    main()
