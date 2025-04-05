from src.etl import extract_data, transform_data, load_data


def main():
    print("🚀 Iniciando pipeline ETL")
    extract_data()
    dfs = transform_data()
    load_data(dfs)
    print("✅ ETL finalizado con éxito")


if __name__ == "__main__":
    main()
