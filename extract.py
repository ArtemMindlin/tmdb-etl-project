import requests
import json
import time

api_key = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlZTdmZGEzYjlkMmY4ZDVmN2RmMGY2YzYwM2I2NjNhMCIsIm5iZiI6MTc0MzM1ODU1MC4wMzYsInN1YiI6IjY3ZTk4YTU2YWY3NTJhM2IyNGY2ZjQxNyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.uqBY8YEuA3XLuvi7HbHnQik8hUTxR6u36BNRxDD-InM"

headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}

url = f"https://api.themoviedb.org/3/movie/popular?language=en-US&page=1"
response = requests.get(url, headers=headers)
data = json.loads(response.text)
total_pages = data["total_pages"]

pop_total_dict = {}

for i in range(1, min(total_pages, 501)):
    url = f"https://api.themoviedb.org/3/movie/popular?language=en-US&page={i}"

    response = requests.get(url, headers=headers)

    dict = json.loads(response.text)
    pop_total_dict = {**pop_total_dict, **dict}

url = f"https://api.themoviedb.org/3/movie/top_rated?language=en-US&page=1"
response = requests.get(url, headers=headers)
data = json.loads(response.text)
total_pages = data["total_pages"]

topr_total_dict = {}

for i in range(1, min(total_pages, 501):
    url = f"https://api.themoviedb.org/3/movie/top_rated?language=en-US&page={i}"

    response = requests.get(url, headers=headers)

    dict = json.loads(response.text)
    topr_total_dict = {**topr_total_dict, **dict}


url = f"https://api.themoviedb.org/3/movie/upcoming?language=en-US&page=1"
response = requests.get(url, headers=headers)
data = json.loads(response.text)
total_pages = data["total_pages"]

upc_total_dict = {}

for i in range(1, min(total_pages, 501):
    url = f"https://api.themoviedb.org/3/movie/upcoming?language=en-US&page={i}"

    response = requests.get(url, headers=headers)

    dict = json.loads(response.text)
    upc_total_dict = {**upc_total_dict, **dict}
