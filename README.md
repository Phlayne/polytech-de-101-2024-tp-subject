# Sujet de travaux pratiques "Introduction à la data ingénierie"

Aboustait Kenzée et Rougier Arnaud 

## Travail effectué 
Pour la réalisation de ce TP nous avons d'abord pris connaissance de l'ensemble des fichiers fournis ainsi que des différentes tables que nous utiliserons. 
Cela permet de mieux comprendre le besoin et de connaitre le Dessin d'Enregistrements des tables à enrichir. 

Nous avons également pris connaissance du fichier main.py permettant de comprendre l'ordre d'execution des fichiers. 

### DATA INGESTION
La premiere tache de ce projet consiste en l'insertion de nouvelle données dans nos tables. 
Nous avons choisis d'inserrer les données de Toulouse grâce a l'API :  [Open data Toulouse](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)

Nous avons regardé dans le fichier main et nous avons donc regarder la fonction get_paris_realtime_bicycle_data(). Nous avons repris le meme code a quelques détails près pour créer la fonction get_toulouse_realtime_bicycle_data(). La différence étant que pour ces données, nous récuperons les données 20 lignes par 20 lignes car l'API possède une limite au nombre de lignes retournées lors de la récuperation des données.
-- Nous avons par la suite récupéré les données de Nantes de l'exacte même facon avec l'API [Open data Nantes](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)

Nous avons implémenté la fonction get_cities() dans la data ingestion car les données de Nante et Toulouse ne remontaient pas les code INSEE à partir de l'API [API Découpage Administratif](https://geo.api.gouv.fr/communes).
Ces données sont donc insérées dans le fichier cities.json.
Les trois fonctions créés ci-dessus sont inserées dans le main dans le processus d'ingestion. 

### DATA CONSOLIDATION

Nous avons commencé par modifier la fonction déjà implémentée consolidate_city_data() qui recupérait les codes INSEE du fichier paris_realtime_bycicle_data. Nous avons fait en sorte que cette fonction puisse enrichir la table CONSOLIDATE_CITY a partir des données recuperées dans le fichier cities.json créé précédemment. 

Pour les deux fonctions consolidate_station_data() et consolidate_station_statement_data(), nous avons repris la même logique de consolidation pour chacune des nouvelles villes, en faisant attention à respecter le DE de la table CONSOLIDATE_STATION. Nous avons pris en compte que les données des API n'avaient pas les même naming que les données de Paris et avons modifié en conséquence. 
À la fin de cette fonction, nous faisons un update sur la table CONSOLIDATE_STATION afin que les codes INSEE null soient enrichis avec ceux présents dans la table CONSOLIDATE_CITY. 

### DATA AGGREGATION 

Nous n'avons pas eu à modifier cette partie du code. 

### Test effectués : 
```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');
```
```
┌───────────┬─────────────────────────────┐
│   NAME    │ SUM_BICYCLE_DOCKS_AVAILABLE │
│  varchar  │           int128            │
├───────────┼─────────────────────────────┤
│ Paris     │                       18482 │
│ Vincennes │                         191 │
│ Toulouse  │                        4236 │
│ Nantes    │                        1466 │
___________________________________________
```
```sql
-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id
Limit  5
;
```
```
┌────────────────────────────────────────┬─────────┬─────────┬────────────────────┐
│                  NAME                  │  CODE   │ ADDRESS │ avg_dock_available │
│                varchar                 │ varchar │ varchar │       double       │
├────────────────────────────────────────┼─────────┼─────────┼────────────────────┤
│ Cassini - Denfert-Rochereau            │ 14111   │         │                8.0 │
│ Rouget de L'isle - Watteau             │ 44015   │         │               11.0 │
│ Le Brun - Gobelins                     │ 13007   │         │  5.333333333333333 │
│ Basilique                              │ 32017   │         │                9.0 │
│ Pierre et Marie Curie - Maurice Thorez │ 42016   │         │  4.666666666666667 │
└────────────────────────────────────────┴─────────┴─────────┴────────────────────┘
```