from src.etl import extract_data, transform_data, load_data


def main():
    print("🚀 Starting ETL pipeline")
    extract_data()
    transform_data()
    load_data()
    print("✅ ETL finished succesfully")


if __name__ == "__main__":
    main()
