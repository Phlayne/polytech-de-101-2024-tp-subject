import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
TOULOUSE_CITY_CODE = 2
NANTES_CITY_CODE = 3

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    
    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]]

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

    # Consolidation logic for Toulouse Bicycle data
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["contract_name"] = toulouse_raw_data_df["contract_name"].apply(lambda s: s.capitalize()) # Ajout d'une majuscule sur le nom de la ville
    toulouse_raw_data_df["city_code"] = None
    toulouse_raw_data_df["created_date"] = date.today()

    toulouse_station_data_df = toulouse_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]]

    toulouse_raw_data_df.rename(columns={
        "number": "code",
        "name": "name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status": "status",
        "contract_name": "city_name",
        "bike_stands": "capacity"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")

    # Consolidation logic for Nantes Bicycle data
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["contract_name"] = nantes_raw_data_df["contract_name"].apply(lambda s: s.capitalize())
    nantes_raw_data_df["city_code"] = None
    nantes_raw_data_df["created_date"] = date.today()

    nantes_station_data_df = nantes_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]]

    nantes_raw_data_df.rename(columns={
        "number": "code",
        "name": "name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status": "status",
        "contract_name": "city_name",
        "bike_stands": "capacity"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")

    # Remplir la colonne city_code à partir de la city_name correspondante présente dans CONSOLIDATE_STATION 
    con.execute("""
        UPDATE CONSOLIDATE_STATION
        SET CITY_CODE = (
            SELECT id 
            FROM CONSOLIDATE_CITY 
            WHERE CONSOLIDATE_CITY.NAME = CONSOLIDATE_STATION.CITY_NAME
        )
        WHERE CITY_CODE IS NULL;
    """) 

def consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
     # Changement du path pour recuperer les données depuis cities.jso au lieu des données de Paris
    with open(f"data/raw_data/{today_date}/cities.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    city_data_df = raw_data_df[[
        "code",
        "nom",
        "population"
    ]]
    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population": "nb_inhabitants"
    }, inplace=True)

    city_data_df["created_date"] = date.today()
    print(city_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def consolidate_station_statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Consolidate station statement data for Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]
    
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")

    # Consolidate station statement data for Toulouse
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()
    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    
    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")

    # Consolidate station statement data for Nantes
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]
    
    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_station_statement_data_df;")
