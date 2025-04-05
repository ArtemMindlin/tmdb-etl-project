"""
TMDb ETL Extraction Script

This script extracts movie data from the TMDb API for three endpoints:
  - /movie/popular
  - /movie/top_rated
  - /movie/upcoming

For each endpoint, the script fetches data page by page (up to 500 pages) and stores each page's data in a dictionary keyed by "page_X". Finally, it saves the results into separate JSON files in the "data/raw" directory.

Usage:
    python extract.py
"""

import requests
import json
import time
import os
from dotenv import load_dotenv

# ----------------------------------------------------------------
# API Configuration
# ----------------------------------------------------------------


load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")
HEADERS = {"accept": "application/json", "Authorization": f"Bearer {API_KEY}"}


# ----------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------


def fetch_genres() -> dict:
    """
    Fetch the list of movie genres from the TMDb API.

    Returns:
        dict: A dictionary containing the genre list, or an empty dict if the request fails.
    """
    url = "https://api.themoviedb.org/3/genre/movie/list?language=en"

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            print("✅ Genres fetched successfully.")
            return response.json()
        else:
            print(f"❌ Failed to fetch genres. Status code: {response.status_code}")
            return {}
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching genres: {e}")
        return {}


def fetch_total_pages(endpoint_url: str) -> int:
    """
    Fetch the total number of pages from the given endpoint URL.

    Args:
        endpoint_url (str): The URL for page 1 of the endpoint.

    Returns:
        int: The total number of pages.
    """
    response = requests.get(endpoint_url, headers=HEADERS)
    data = response.json()
    return data["total_pages"]


def fetch_movies(
    endpoint: str, max_page_limit: int = 501, sleep_time: float = 0.3
) -> dict:
    """
    Fetch movie data from a specified TMDb endpoint, page by page.

    Args:
        endpoint (str): The endpoint path (e.g., "/movie/popular").
        max_page_limit (int): Maximum number of pages to fetch (default is 501).
        sleep_time (float): Delay between requests in seconds.

    Returns:
        dict: A dictionary with keys "page_1", "page_2", etc., containing JSON responses.
    """
    # Build URL for the first page to determine the total pages
    url_first = f"https://api.themoviedb.org/3{endpoint}?language=en-US&page=1"
    total_pages = fetch_total_pages(url_first)

    movies_dict = {}

    # Iterate over pages (starting from page 1)
    for i in range(1, min(total_pages, max_page_limit)):
        url = f"https://api.themoviedb.org/3{endpoint}?language=en-US&page={i}"
        try:
            start_time = time.time()
            response = requests.get(url, headers=HEADERS, timeout=10)
            duration = time.time() - start_time
            print(f"✅ Page {i} downloaded in {duration:.2f}s")
            if response.status_code == 200:
                page_data = response.json()
                movies_dict[f"page_{i}"] = page_data
            else:
                print(f"❌ Page {i} failed with status {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error connecting on page {i}: {e}")
        time.sleep(sleep_time)

    return movies_dict


def save_json(data: dict, filepath: str) -> None:
    """
    Save a dictionary as a formatted JSON file.

    Args:
        data (dict): Data to save.
        filepath (str): Destination file path.
    """
    # Ensure the target directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Data saved to: {filepath}")


# ----------------------------------------------------------------
# Main Extraction Process
# ----------------------------------------------------------------
def extract_data() -> dict:
    # Fetch Popular Movies
    print("Fetching Popular Movies...")
    popular_data = fetch_movies("/movie/popular", max_page_limit=501, sleep_time=0.2)

    # Fetch Top Rated Movies
    print("Fetching Top Rated Movies...")
    top_rated_data = fetch_movies(
        "/movie/top_rated", max_page_limit=501, sleep_time=0.3
    )

    # Fetch Upcoming Movies
    print("Fetching Upcoming Movies...")
    upcoming_data = fetch_movies("/movie/upcoming", max_page_limit=501, sleep_time=0.3)

    # Fetch Genres
    print("Fetching Genres...")
    genres = fetch_genres()

    # Save the data to JSON files
    save_json(popular_data, "../data/raw/tmdb_popular.json")
    save_json(top_rated_data, "../data/raw/tmdb_top_rated.json")
    save_json(upcoming_data, "../data/raw/tmdb_upcoming.json")
    save_json(genres, "../data/raw/genres.json")

    return {
        "popular": popular_data,
        "top_rated": top_rated_data,
        "upcoming": upcoming_data,
        "genres": genres,
    }


if __name__ == "__main__":
    extract_data()
