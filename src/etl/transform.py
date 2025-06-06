"""
TMDb ETL Transformation Script

This script loads raw movie data from the "data/raw/" directory,
cleans and enriches it (e.g., genre mapping, image URLs, feature engineering),
optimizes data types, and saves the final datasets in "data/processed/"
as CSV and Parquet files.

Usage:
    python transform.py
"""

import os
import json
import pandas as pd
import numpy as np


# ----------------------------------------------------------------
# Data Optimization
# ----------------------------------------------------------------
def optimize_df(df, parse_dates=None, verbose=True):
    start_mem = df.memory_usage(deep=True).sum() / 1024**2

    for col in df.columns:
        col_type = df[col].dtype

        if df[col].apply(lambda x: isinstance(x, list)).any():
            continue

        if col_type in ["int64", "int32"]:
            if (
                df[col].min() >= np.iinfo("int8").min
                and df[col].max() <= np.iinfo("int8").max
            ):
                df.loc[:, col] = df[col].astype("int8")
            elif (
                df[col].min() >= np.iinfo("int16").min
                and df[col].max() <= np.iinfo("int16").max
            ):
                df.loc[:, col] = df[col].astype("int16")
            elif (
                df[col].min() >= np.iinfo("int32").min
                and df[col].max() <= np.iinfo("int32").max
            ):
                df.loc[:, col] = df[col].astype("int32")

        elif col_type == "float64":
            df.loc[:, col] = df[col].astype("float32")

        elif col_type == "object":
            if parse_dates and col in parse_dates:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                # print("Se ha hechoooo")
                # print(df[col].dtype)
            else:
                try:
                    num_unique = df[col].nunique()
                    num_total = len(df[col])
                    if num_unique / num_total < 0.5:
                        df.loc[:, col] = df[col].astype("category")
                except TypeError:
                    pass

    end_mem = df.memory_usage(deep=True).sum() / 1024**2

    if verbose:
        print(
            f"🔧 Memory usage reduced from {start_mem:.2f} MB to {end_mem:.2f} MB "
            f"({100 * (start_mem - end_mem) / start_mem:.1f}% reduction)"
        )

    return df


# ----------------------------------------------------------------
# Data Cleaning
# ----------------------------------------------------------------

def clean_movies(df):
    df.loc[:, "poster_path"] = df["poster_path"].fillna("")
    df.loc[:, "backdrop_path"] = df["backdrop_path"].fillna("")

    df = df[df["release_date"].notna()].copy()

    base_poster_url = "https://image.tmdb.org/t/p/w342"
    base_backdrop_url = "https://image.tmdb.org/t/p/w780"

    df.loc[:, "poster_url"] = base_poster_url + df["poster_path"]
    df.loc[:, "backdrop_url"] = base_backdrop_url + df["backdrop_path"]

    return df


# ----------------------------------------------------------------
# Final Transformation
# ----------------------------------------------------------------

def finalize_movies(df, df_genres):
    df = optimize_df(df, parse_dates=["release_date"], verbose=True)
    df = clean_movies(df)

    df.loc[:, "release_year"] = df["release_date"].dt.year.astype("int16")
    df.loc[:, "release_month"] = df["release_date"].dt.month.astype("int8")

    df.loc[:, "popularity_level"] = pd.qcut(
        df["popularity"], q=4, labels=["low", "medium", "high", "very high"]
    )
    df.loc[:, "rating_level"] = pd.cut(
        df["vote_average"],
        bins=[0, 5, 7, 8.5, 10],
        labels=["poor", "average", "good", "excellent"],
    )

    genre_map = dict(zip(df_genres["id"], df_genres["name"]))
    df.loc[:, "genres"] = df["genre_ids"].apply(
        lambda ids: [genre_map.get(i, "Unknown") for i in ids]
    )
    df.loc[:, "genres_str"] = df["genres"].apply(lambda g: ", ".join(g))

    drop_cols = ["poster_path", "backdrop_path", "video", "adult", "genre_ids"]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    return df


# ----------------------------------------------------------------
# Save Function
# ----------------------------------------------------------------


def main_save_movies(df, name):
    output_dir = "../../data/processed/"
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, f"tmdb_{name}.csv")
    parquet_path = os.path.join(output_dir, f"tmdb_{name}.parquet")

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"✅ Saved: {csv_path} & {parquet_path}")


def save_movies(df, name):
    output_dir = "data/processed/"
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, f"tmdb_{name}.csv")
    parquet_path = os.path.join(output_dir, f"tmdb_{name}.parquet")

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"✅ Saved: {csv_path} & {parquet_path}")


# ----------------------------------------------------------------
# Main Transformation Flow
# ----------------------------------------------------------------


def main():
    # Load raw JSON data
    with open("../../data/raw/tmdb_popular.json") as f:
        pop_data = json.load(f)

    with open("../../data/raw/tmdb_top_rated.json") as f:
        topr_data = json.load(f)

    with open("../../data/raw/tmdb_upcoming.json") as f:
        upc_data = json.load(f)

    with open("../../data/raw/genres.json") as f:
        genres_data = json.load(f)

    # Extract movie lists from page
    pop_movies = []
    topr_movies = []
    upc_movies = []

    for page_data in pop_data.values():
        results = page_data.get("results", [])
        pop_movies.extend(results)

    for page_data in topr_data.values():
        results = page_data.get("results", [])
        topr_movies.extend(results)

    for page_data in upc_data.values():
        results = page_data.get("results", [])
        upc_movies.extend(results)

    # Convert to DataFrames
    df_pop = pd.DataFrame(pop_movies)
    df_topr = pd.DataFrame(topr_movies)
    df_upc = pd.DataFrame(upc_movies)
    df_genres = pd.DataFrame(genres_data["genres"])

    # Apply transformations
    df_pop_final = finalize_movies(df_pop, df_genres)
    df_topr_final = finalize_movies(df_topr, df_genres)
    df_upc_final = finalize_movies(df_upc, df_genres)

    # Save all outputs
    main_save_movies(df_pop_final, "popular")
    main_save_movies(df_topr_final, "top_rated")
    main_save_movies(df_upc_final, "upcoming")
    main_save_movies(df_genres, "genres")


def transform_data():
    # Load raw JSON data
    with open("data/raw/tmdb_popular.json") as f:
        pop_data = json.load(f)

    with open("data/raw/tmdb_top_rated.json") as f:
        topr_data = json.load(f)

    with open("data/raw/tmdb_upcoming.json") as f:
        upc_data = json.load(f)

    with open("data/raw/genres.json") as f:
        genres_data = json.load(f)

    # Extract movie lists from page
    pop_movies = []
    topr_movies = []
    upc_movies = []

    for page_data in pop_data.values():
        results = page_data.get("results", [])
        pop_movies.extend(results)

    for page_data in topr_data.values():
        results = page_data.get("results", [])
        topr_movies.extend(results)

    for page_data in upc_data.values():
        results = page_data.get("results", [])
        upc_movies.extend(results)

    # Convert to DataFrames
    df_pop = pd.DataFrame(pop_movies)
    df_topr = pd.DataFrame(topr_movies)
    df_upc = pd.DataFrame(upc_movies)
    df_genres = pd.DataFrame(genres_data["genres"])

    # Apply transformations
    df_pop_final = finalize_movies(df_pop, df_genres)
    df_topr_final = finalize_movies(df_topr, df_genres)
    df_upc_final = finalize_movies(df_upc, df_genres)

    # Save all outputs
    save_movies(df_pop_final, "popular")
    save_movies(df_topr_final, "top_rated")
    save_movies(df_upc_final, "upcoming")
    save_movies(df_genres, "genres")

# ----------------------------------------------------------------
# Run
# ----------------------------------------------------------------
if __name__ == "__main__":
    transform_data()
