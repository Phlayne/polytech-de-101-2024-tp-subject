import os
from datetime import datetime

import json
import requests

def get_paris_realtime_bicycle_data():
    
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "paris_realtime_bicycle_data.json")


def get_toulouse_realtime_bicycle_data():
    def get_url(offset):
        return "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records?limit=20&offset=" + str(offset)
    
    response = requests.request("GET", get_url(0))
    response_json = response.json()

    total_count = response_json["total_count"]

    full_data = response_json["results"]

    increment = 20
    for offset in range(increment, total_count, increment):
        response = requests.request("GET", get_url(offset))
        full_data.extend(response.json()["results"])
    
    raw_json = json.dumps(full_data)
    serialize_data(raw_json, "toulouse_realtime_bicycle_data.json")

def get_cities():
    url = "https://geo.api.gouv.fr/communes"
    
    response = requests.request("GET", url)

    serialize_data(response.text, "cities.json")
    

def serialize_data(raw_json: str, file_name: str):

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)

