import json
from typing import Any
import urllib.parse
import datetime
from time import sleep

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import redis
from redis.commands.json.path import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

import pandas as pd

from dotenv import load_dotenv
import os
import re




REDIS_HOST="redis"
REDIS_PORT=6379
REDIS_DB=1

MONGO_HOST="admin:admin@mongo"
MONGO_PORT="27017"
MONGO_DB="ingestion"

# Load environment variables from .env file
load_dotenv('/opt/airflow/dags/.env', override=True)

# Get the API key from the environment variable
API_KEY = os.getenv('API_KEY')


default_args_dict = {
    "start_date": datetime.datetime(2024, 11, 24, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 0 * * *",  # Every day at midnight
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

ingestion_dag = DAG(
    dag_id="ingestion_dag",
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=["/opt/airflow/data/"],
)


### OFFLINE part

offline_sources_node = EmptyOperator(
    task_id="offline_sources", dag=ingestion_dag, trigger_rule="all_success"
)


def _load_JSON_wikidata(
    filepath_input: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
):
    
    # open the json
    try: 
        with open(filepath_input, 'r') as file:
            data = json.load(file)

        client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        for da in data:
            title = da['title']
            cpt = 0
            for loc in da["filmingLocation"]:
                client.hset(f"{title}:filmingLocation", str(cpt), loc)
                cpt += 1

            client.hset(f"{title}:filmingLocation", "count", cpt)

            cpt = 0
            for loc in da["narrativeLocation"]:
                client.hset(f"{title}:narrativeLocation", str(cpt), loc)
                cpt += 1

            client.hset(f"{title}:narrativeLocation", "count", cpt)
        
            
    except FileNotFoundError:
        print("Le fichier spécifié est introuvable.")

    except json.JSONDecodeError:
        print("Erreur de décodage JSON : le fichier est mal formé.")
        


load_wikidata_node = PythonOperator(
    task_id="load_wikidata_json",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_load_JSON_wikidata,
    op_kwargs={
        "filepath_input": "/opt/airflow/data/offline_wikidata_locations.json",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)


def _load_JSON_ai(
    filepath_input: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
):
    
    # open the json
    try: 
        with open(filepath_input, 'r') as file:
            data = json.load(file)

        client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        for da in data:
            title = da['title']
            cpt = 0
            for loc in da["places"]:
                client.hset(f"{title}:places", str(cpt), loc)
                cpt += 1

            client.hset(f"{title}:places", "count", cpt)

        
            
    except FileNotFoundError:
        print("Le fichier spécifié est introuvable.")

    except json.JSONDecodeError:
        print("Erreur de décodage JSON : le fichier est mal formé.")


        
load_ai_node = PythonOperator(
    task_id="load_ai_json",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_load_JSON_ai,
    op_kwargs={
        "filepath_input": "/opt/airflow/data/offline_ai_locations.json",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)
    


online_sources_node = EmptyOperator(
    task_id="online_sources", dag=ingestion_dag, trigger_rule="all_success"
)


def _extract_and_load_movies_titles(
        filepath_input: str,
        redis_output_key: str,
        redis_host: str,
        redis_port: str,
        redis_db: str,
) -> None:

    # read the movies csv file
    df = pd.read_csv(filepath_input)

    # get the titles
    titles = df['original_title']

    # connect to redisSave the result to redis db (to speed up the steps as it uses cache)
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    # Load titles into redis
    for title in titles:
        r.sadd(redis_output_key, title)


movie_titles_node_online = PythonOperator(
    task_id="movie_titles_loading_online",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_extract_and_load_movies_titles,
    op_kwargs={
        "filepath_input": "/opt/airflow/data/temp.csv",
        "redis_output_key": "movie_titles",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)

movie_titles_node_offline = PythonOperator(
    task_id="movie_titles_loading_offline",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_extract_and_load_movies_titles,
    op_kwargs={
        "filepath_input": "/opt/airflow/data/temp.csv",
        "redis_output_key": "movie_titles",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)

### TEST CONNECTION

def _check_connection():

    try:
        requests.get("https://google.com")
    except requests.exceptions.ConnectionError:
        return "offline_sources"

    return "online_sources"
        

connection_check_node = BranchPythonOperator(
    task_id='connection_check',
    dag=ingestion_dag,
    python_callable= _check_connection,
    op_kwargs={},
    trigger_rule='all_success',
)


### ONLINE PART : wikidata


def _get_filming_location_with_wikidata(movie_title: str, endpoint: str, url: str):

    # Wikidata query to find filming locations for a specific film by its title
    sparql_query = (
        "SELECT DISTINCT ?filmingLocationLabel WHERE {"
        "?film wdt:P31 wd:Q11424 ;"
        "wdt:P1476 ?filmLabel ."     
  
        f'FILTER (LANG(?filmLabel) = "en" && STR(?filmLabel) = "{movie_title}") .'
  
        "?film wdt:P915 ?filmingLocation ."
        
        "SERVICE wikibase:label { "
        "bd:serviceParam wikibase:language \"en\". "
        "}"
        "}"
    )
    
    r = requests.get(
        f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
    )
    if not r.ok:
        # Probable too many requests, so timeout and retry
        sleep(1)
        r = requests.get(
            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
        )
        
    return r.json()

def _get_narrative_location_with_wikidata(movie_title: str, endpoint: str, url: str):

    # Wikidata query to find narrative locations for a specific film by its title
    sparql_query = (
        "SELECT DISTINCT ?narrativeLocationLabel WHERE {"
        "?film wdt:P31 wd:Q11424 ;"
        "wdt:P1476 ?filmLabel ."     
  
        f'FILTER (LANG(?filmLabel) = "en" && STR(?filmLabel) = "{movie_title}") .'
  
        "?film wdt:P840 ?narrativeLocation ."
        
        "SERVICE wikibase:label { "
        "bd:serviceParam wikibase:language \"en\". "
        "}"
        "}"
    )
    
    r = requests.get(
        f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
    )
    if not r.ok:
        # Probable too many requests, so timeout and retry
        sleep(1)
        r = requests.get(
            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
        )
        
    return r.json()



def _find_all_filming_narrative_location_with_wikidata(
    redis_input_key: str,
    filepath_output: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    endpoint: str,
    url: str,
):

    # Get the movie titles saved in redis
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    movie_titles = client.smembers(redis_input_key)

    output = []
    i = 0
    stop = 0
    for title in movie_titles:

        if stop >= 5:
            break
        else:
            stop += 1

        title = title.decode('utf-8')
        
        output.append({'title': title})

        # filming location
        res = _get_filming_location_with_wikidata(title, endpoint, url)

        print(res)

        # Save in hashes with this format (key = title:filmingLocation, field=number, value) + one special field count that indicates the total number of filming location
        cpt = 0
        output[i]['filmingLocation'] = []
        for temp in res["results"]["bindings"]:
            client.hset(f"{title}:filmingLocation", str(cpt), temp["filmingLocationLabel"]["value"])
            output[i]['filmingLocation'].append(temp["filmingLocationLabel"]["value"])
            cpt += 1
        client.hset(f"{title}:filmingLocation", "count", cpt)

        # narrative location
        res = _get_narrative_location_with_wikidata(title, endpoint, url)

        print(res)

        cpt = 0
        output[i]['narrativeLocation'] = []
        for temp in res["results"]["bindings"]:
            client.hset(f"{title}:narrativeLocation", str(cpt), temp["narrativeLocationLabel"]["value"])
            output[i]['narrativeLocation'].append(temp["narrativeLocationLabel"]["value"])
            cpt += 1
        client.hset(f"{title}:narrativeLocation", "count", cpt)


        i += 1
        
        
    # Persist the data in a json in case no connection is available next time
    with open(filepath_output, "w+", encoding="utf-8") as f:
        json.dump(output, f)


wikidata_location_node = PythonOperator(
    task_id="wikidata_location",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_find_all_filming_narrative_location_with_wikidata,
    op_kwargs={
        "redis_input_key": "movie_titles",
        "filepath_output": "/opt/airflow/data/offline_wikidata_locations.json",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "endpoint": "/sparql",
        "url": "https://query.wikidata.org",
    },
    depends_on_past=False,
)


### ONLINE PART: ai


def _ask_ai_monuments_places(
        redis_input_key: str,
        filepath_output: str,
        redis_host: str,
        redis_port: str,
        redis_db: str,
        url: str,
) -> None:

    # Get the movie titles saved in redis
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    movie_titles = client.smembers(redis_input_key)

    stop = 0
    output = []  # Initialize output as an empty list
    for title in movie_titles:
        if stop >= 5:
            break
        else:
            stop += 1
        
        title = title.decode('utf-8')

        # Initialize movie data
        movie_data = {'title': title}

        # Get all filming locations for the title
        filmingLocations = []
        filming_count = int(client.hget(f"{title}:filmingLocation", "count"))
        for i in range(filming_count):
            location = client.hget(f"{title}:filmingLocation", str(i))
            if location:
                filmingLocations.append(location.decode('utf-8'))

        # AI headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"{API_KEY}",
        }

        # AI request data
        data = {
            "messages": [
                {"role": "system", "content": "You are a movie specialist."},
                {"role": "user", "content": f"Give me only the famous monuments and places appearing in {title} given that it takes place in those locations {', '.join(filmingLocations)}. The output format is just a list without any description. Each place and monument should be between @."}
            ],
            "model": "grok-beta",
            "stream": False,
            "temperature": 0
        }

        print(f"Movie: {title}, Cities: {', '.join(filmingLocations)}")

        # Execute the request
        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            # Extract places from the response
            text = response.text
            places = re.findall(r'@([^@]+)@', text)  # Extract places between @ symbols
            new_places = {place.strip() for place in places if place.strip()}  # Use a set for uniqueness

            # Add places to the movie data
            movie_data['places'] = list(new_places)

            # Add key title:places with the places and the count to Redis
            for cpt, pl in enumerate(new_places):
                client.hset(f"{title}:places", str(cpt), pl)
            
            print(f"Movie '{title}' updated successfully.")
        else:
            print(f"Error {response.status_code}: {response.text}")

        # Append the movie data to the output
        output.append(movie_data)

    # Persist the data in a json in case no connection is available next time
    with open(filepath_output, "w+", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    

    


ai_node = PythonOperator(
    task_id="ask_ai",
    dag=ingestion_dag,
    trigger_rule='one_success',
    python_callable=_ask_ai_monuments_places,
    op_kwargs={
        "redis_input_key": "movie_titles",
        "filepath_output": "/opt/airflow/data/offline_ai_locations.json",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "url": "https://api.x.ai/v1/chat/completions",
    },
    depends_on_past=False,
)


### MONGO DB 


def _mongodb_saver_csv(
    filepath_input: str,
    mongo_host: str,
    mongo_port: str,
    mongo_database: str,
    mongo_collection: str,
):

    # Open the cleaned csv file
    df = pd.read_csv(filepath_input)


    client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
    db = client[mongo_database]
    col = db[mongo_collection]

    # Convertir chaque ligne du DataFrame en un dictionnaire
    data = df.to_dict(orient='records')

    # Ajouter chaque document à la collection MongoDB
    col.insert_many(data)

    print("Les data ont été ajouté dans la collection mongodb")
    

mongo_csv_node = PythonOperator(
    task_id="mongodb_saver_csv",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_mongodb_saver_csv,
    op_kwargs={
        "filepath_input": "/opt/airflow/data/temp.csv",
        "mongo_host": MONGO_HOST,
        "mongo_port": MONGO_PORT,
        "mongo_database": MONGO_DB,
        "mongo_collection": "movies_csv",
    },
)


def _mongodb_saver_wikidata(
    redis_input_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    mongo_host: str,
    mongo_port: str,
    mongo_database: str,
    mongo_collection: str,
):

    
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    movie_titles = client.smembers(redis_input_key);

    clientMDB = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
    db = clientMDB[mongo_database]
    collection = db[mongo_collection]

    for title in movie_titles:

        title = title.decode('utf-8')

        # Get wikidata in redis 
        wikidata_filmingLocation = client.hgetall(f"{title}:filmingLocation")
        wikidata_narrativeLocation = client.hgetall(f"{title}:narrativeLocation")


        # Convert hash data to dict
        filmingDict = {field.decode('utf-8'): value.decode('utf-8') for field, value in wikidata_filmingLocation.items()}

        if 'count' in filmingDict:
            filmingDict.pop('count')

        narrativeDict = {field.decode('utf-8'): value.decode('utf-8') for field, value in wikidata_narrativeLocation.items()}

        if 'count' in narrativeDict:
            narrativeDict.pop('count')

        # Convert dict to JSON

        final_json = {'title': title, 'filmingLocation': list(filmingDict.values()), 'narrativeLocation': list(narrativeDict.values())}

        # Add document to collection MongoDB
        collection.insert_one(final_json)

    
    



mongo_wikidata_node = PythonOperator(
    task_id="mongodb_saver_wikidata",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_mongodb_saver_wikidata,
    op_kwargs={
        "redis_input_key": "movie_titles",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "mongo_host": MONGO_HOST,
        "mongo_port": MONGO_PORT,
        "mongo_database": MONGO_DB,
        "mongo_collection": "wikidata_location",
    },
)


def _mongodb_saver_ai(
    redis_input_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    mongo_host: str,
    mongo_port: str,
    mongo_database: str,
    mongo_collection: str,
):

    
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    movie_titles = client.smembers(redis_input_key);

    clientMDB = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
    db = clientMDB[mongo_database]
    collection = db[mongo_collection]

    for title in movie_titles:

        title = title.decode('utf-8')

        # Get ai data in redis 
        ai_places = client.hgetall(f"{title}:places")


        # Convert hash data to dict
        aiDict = {field.decode('utf-8'): value.decode('utf-8') for field, value in ai_places.items()}

        if 'count' in aiDict:
            aiDict.pop('count')

        # Convert dict to JSON

        final_json = {'title': title, 'places': list(aiDict.values())}

        # Add document to collection MongoDB
        collection.insert_one(final_json)

    
    



mongo_ai_node = PythonOperator(
    task_id="mongodb_saver_ai",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_mongodb_saver_ai,
    op_kwargs={
        "redis_input_key": "movie_titles",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "mongo_host": MONGO_HOST,
        "mongo_port": MONGO_PORT,
        "mongo_database": MONGO_DB,
        "mongo_collection": "ai_places",
    },
)


def _delete_mongodb_ingestion(
    mongo_host: str,
    mongo_port: str,
    mongo_database: str,
):
    try:

        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        
        # Vérifier si la base de données existe
        if mongo_database in client.list_database_names():
            # Supprimer la base de données
            client.drop_database(mongo_database)
            print(f"La base de données '{mongo_database}' a été supprimée avec succès.")
        else:
            print(f"La base de données '{mongo_database}' n'existe pas.")
        
        # Fermer la connexion
        client.close()
    except Exception as e:
        print(f"Une erreur est survenue lors de la suppression de la base de données : {e}")



mongo_delete_node = PythonOperator(
    task_id="mongodb_delete_ingestion",
    dag=ingestion_dag,
    trigger_rule="one_success",
    python_callable=_delete_mongodb_ingestion,
    op_kwargs={
        "mongo_host": MONGO_HOST,
        "mongo_port": MONGO_PORT,
        "mongo_database": MONGO_DB,
    },
)

        

### FINAL PART

last_node = EmptyOperator(
    task_id="final", dag=ingestion_dag, trigger_rule="all_success"
)


intermediate_offline_node = EmptyOperator(
    task_id = "intermediate_offline",
    dag= ingestion_dag,
    trigger_rule="all_success"
)



connection_check_node >> [online_sources_node, offline_sources_node]

#offline part
offline_sources_node >> [movie_titles_node_offline, load_ai_node, load_wikidata_node]

[movie_titles_node_offline, load_ai_node, load_wikidata_node] >> intermediate_offline_node >> mongo_delete_node >>[mongo_csv_node, mongo_wikidata_node, mongo_ai_node]

#online part
online_sources_node >> movie_titles_node_online >> wikidata_location_node

wikidata_location_node >> ai_node >> mongo_delete_node

mongo_delete_node >> [mongo_csv_node, mongo_wikidata_node, mongo_ai_node]


#final part

[mongo_csv_node, mongo_wikidata_node, mongo_ai_node]  >> last_node
