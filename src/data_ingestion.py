import os
from datetime import datetime

import json
import requests
from tqdm import tqdm

def get_paris_realtime_bicycle_data():
    
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "paris_realtime_bicycle_data.json")


# Recuperation des données de L'API pour Toulouse
def get_toulouse_realtime_bicycle_data():
    def get_url(offset):
        return f"https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records?limit=20&offset={offset}"
    
    response = requests.request("GET", get_url(0))
    response_json = response.json()

    total_count = response_json["total_count"]

    full_data = response_json["results"]

    # Boucle permettant de recuperer 20 lignes pas 20 lignes 
    increment = 20
    for offset in tqdm(range(increment, total_count, increment), desc="Fetching Toulouse data"):
        response = requests.request("GET", get_url(offset))
        full_data.extend(response.json()["results"])
    
    raw_json = json.dumps(full_data)
    serialize_data(raw_json, "toulouse_realtime_bicycle_data.json")

# Recuperation des données de L'API pour Nantes
def get_nantes_realtime_bicycle_data():
    def get_url(offset):
        return f"https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/records?limit=20&offset={offset}"
    
    response = requests.request("GET", get_url(0))
    response_json = response.json()

    total_count = response_json["total_count"]

    full_data = response_json["results"]

    increment = 20
    for offset in tqdm(range(increment, total_count, increment), desc="Fetching Nantes data"):
        response = requests.request("GET", get_url(offset))
        full_data.extend(response.json()["results"])
    
    raw_json = json.dumps(full_data)
    serialize_data(raw_json, "nantes_realtime_bicycle_data.json")


# ajout des codes Insee issus de l'API dans cities.json
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

