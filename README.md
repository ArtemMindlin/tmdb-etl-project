# 🎬 TMDb ETL Project – Python & SQL Pipeline

This project implements a complete ETL pipeline using the public [The Movie Database (TMDb)](https://www.themoviedb.org/) API to extract, transform, and load information about the top-rated movies.

---

## 📌 Objective

Create a workflow that:

1. **Extracts** data from the TMDb REST API
2. **Transforms** the raw data into a clean and structured format using Python (`pandas`)
3. **Loads** the data into a relational database (SQLite)
4. **Queries and analyzes** the data using SQL and Python

---

## ⚙️ Technologies Used

- **Python 3**
  - Libraries: `requests`, `pandas`, `json`, `sqlite3`, `time`, `os`, `dotenv`
- **SQL**
  - SQLite for local data storage
- **REST API**
  - TMDb `/movie/top_rated` endpoint
- **(Optional)** Jupyter Notebook for exploration and visualization

---

## 🧱 Project Structure

```
tmdb-etl-project/
├── data/
│   ├── raw/               # JSON files downloaded from the API
│   └── processed/         # Cleaned CSVs ready for loading
├── notebooks/
│   └── exploratory.ipynb  # Data analysis and visualizations
├── src/etl
│   ├── extract.py         # Fetches data from the TMDb API
│   ├── transform.py       # Cleans and transforms data with pandas
│   └── load.py            # Loads data into an SQLite database
├── sql/
│   └── queries.sql        # SQL queries for exploration and analysis
├── requirements.txt       # Project dependencies
├── main.py		   # Main file
└── README.md              # Project overview
```

---

## 🚀 How to Run the Project

### 1. Clone the repository:

```bash
git clone https://github.com/your-username/tmdb-etl-project.git
cd tmdb-etl-project
```

### 2. Prepare repository setup:

```bash
make setup
```

### 3.1 Run the entire ETL pipeline:

```bash
make run
```

### 3.2 Run individual scripts:

```
make extract
make transform
make load
```

### 4. Explore the data:

- Run SQL queries in `sql/queries.sql` against the loaded database.
- Or open the notebook `notebooks/exploratory.ipynb` for interactive analysis and visualization.

---

## 📊 Example Analyses

- Top-rated movies by year
- Popularity trends over time
- Vote distributions by language
- Top 10 most voted movies

---

## 🧠 Key Takeaways

- Fetching data from a public REST API
- Cleaning and transforming real-world data
- Structuring and storing data in a relational database
- Querying and analyzing data using SQL and Python

---

## 📄 License

This project is for educational purposes only. All data comes from TMDb and is subject to their [terms of use](https://www.themoviedb.org/documentation/api/terms-of-use).
