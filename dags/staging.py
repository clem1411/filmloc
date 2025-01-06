import json
from typing import Any
import urllib.parse
import datetime
from time import sleep
import requests

from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, relationship, Session

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

import redis
from redis.commands.json.path import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

import pandas as pd

import os
import re
import pyarrow as pa


REDIS_HOST="redis"
REDIS_PORT=6379
REDIS_DB=2

MONGO_HOST="admin:admin@mongo"
MONGO_PORT="27017"
MONGO_DB="ingestion"


default_args_dict = {
    "start_date": datetime.datetime(2025, 1, 4, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 0 * * *",  # Every day at midnight
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=30),
}

staging_dag = DAG(
    dag_id="staging_dag",
    default_args=default_args_dict,
    catchup=False,
)


### START

start_node = EmptyOperator(
    task_id="start", dag=staging_dag, trigger_rule="all_success"
)

### Load data from mongodb to redis

def _load_data_from_mongodb(
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    mongo_host: str,
    mongo_port: str,
    mongo_db: str,
    mongo_collection: str,
):

    mongo_client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
    db = mongo_client[mongo_db]
    collection = db[mongo_collection]

    # get data
    data = pd.DataFrame(list(collection.find()))
    
    # storing in redis
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    redis_client.set(
        redis_output_key, context.serialize(data).to_buffer().to_pybytes()
    )
    

load_ai_places_node = PythonOperator(
    task_id="load_ai_places",
    dag=staging_dag,
    trigger_rule="one_success",
    python_callable=_load_data_from_mongodb,
    op_kwargs={
        "redis_output_key": "ai_places",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "mongo_host": MONGO_HOST,
        "mongo_port": MONGO_PORT,
        "mongo_db": MONGO_DB,
        "mongo_collection": "ai_places",
    },
    depends_on_past=False,
)


load_wikidata_locations_node = PythonOperator(
    task_id="load_wikidata_locations",
    dag=staging_dag,
    trigger_rule="one_success",
    python_callable=_load_data_from_mongodb,
    op_kwargs={
        "redis_output_key": "wikidata_locations",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "mongo_host": MONGO_HOST,
        "mongo_port": MONGO_PORT,
        "mongo_db": MONGO_DB,
        "mongo_collection": "wikidata_location",
    },
    depends_on_past=False,
)

load_movies_csv_node = PythonOperator(
    task_id="load_movies_csv",
    dag=staging_dag,
    trigger_rule="one_success",
    python_callable=_load_data_from_mongodb,
    op_kwargs={
        "redis_output_key": "movies_csv",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "mongo_host": MONGO_HOST,
        "mongo_port": MONGO_PORT,
        "mongo_db": MONGO_DB,
        "mongo_collection": "movies_csv",
    },
    depends_on_past=False
)


### PREPARE MOVIE DATA

def _prepare_movie_data(
    redis_output_key: str,
    redis_input_key:str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
):
    # get movies
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    data = context.deserialize(redis_client.get(redis_input_key))
    df = pd.DataFrame(data)
    
    # drop unused columns
    df = df.drop(["genres","keywords","production_companies","_id"],axis=1)

    # drop rows with original_title or vote_average null
    df = df.dropna(subset=['original_title', 'vote_average'])

    # drop duplicated values
    df = df.drop_duplicates()

    # index = id
    df['id'] = df.index

    # store in redis
    redis_client.set(redis_output_key, context.serialize(df).to_buffer().to_pybytes())


prepare_movies_data_node = PythonOperator(
    task_id="prepare_movie_data",
    dag=staging_dag,
    trigger_rule="all_success",
    python_callable=_prepare_movie_data,
    op_kwargs={
        "redis_output_key": "movies_data",
        "redis_input_key": "movies_csv",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)


### PREPARE KEYWORDS, GENRES, PRODUCTION COMPANIES DATA

# Convertir la colonne spécifiée en liste de dictionnaires
# Appliquer json.loads seulement aux valeurs valides (str ou bytes)
def safe_json_loads(val):
    if isinstance(val, str):  # Vérifier que la valeur est une chaîne
        try:
            return json.loads(val)  # Convertir en liste de dictionnaires
        except json.JSONDecodeError:
            return []  # Retourner une liste vide si la conversion échoue
    return []  # Retourner une liste vide si ce n'est pas une chaîne



def _prepare_json_in_movies(
    column: str,
    redis_input_key_raw_movies: str,
    redis_input_key_cleaned_movies: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
):
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    
    movies_csv = context.deserialize(redis_client.get(redis_input_key_raw_movies))
    movies_prepared = context.deserialize(redis_client.get(redis_input_key_cleaned_movies))

    
    df = pd.DataFrame(movies_csv)
    df_movies = pd.DataFrame(movies_prepared)

    
    df = df[['original_title', column]]


    # Convert column into list of dict
    df[column] = df[column].apply(safe_json_loads)

    
    # Extract all value into one list
    all_value = []
    for value_list in df[column]:
        all_value.extend(value_list)
        
        
    # Create df with unique column value
    df_column = pd.DataFrame(all_value).drop_duplicates().reset_index(drop=True)

    relations = []

    # For each movie, search the relation
    for _, row in df_movies.iterrows():
        movie_id = row['id']
        movie_title = row['original_title']
        print(f"Processing movie: {movie_title}")
        
        column_list = df.loc[df['original_title'] == movie_title, [column]].iloc[0]
        for value in column_list[0]:
            if isinstance(value, dict):  # Vérifier que chaque élément est un dictionnaire
                column_id = value['id']
                relations.append({'movie_id': movie_id, f'{column}_id': column_id})
            else:
                print(f"Skipping invalid value: {value}")  # En cas d'élément non valide

            
    df_relations = pd.DataFrame(relations)
    
    #storing in redis
    redis_client.set(f'{column}_data', context.serialize(df_column).to_buffer().to_pybytes())
    redis_client.set(f'movies-{column}_data', context.serialize(df_relations).to_buffer().to_pybytes())



prepare_genres_data_node = PythonOperator(
    task_id="prepare_genres_data",
    dag=staging_dag,
    trigger_rule="one_success",
    python_callable=_prepare_json_in_movies,
    op_kwargs={
        "column": "genres",
        "redis_input_key_raw_movies": "movies_csv",
        "redis_input_key_cleaned_movies": "movies_data",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False
)


prepare_keywords_data_node = PythonOperator(
    task_id="prepare_keywords_data",
    dag=staging_dag,
    trigger_rule="one_success",
    python_callable=_prepare_json_in_movies,
    op_kwargs={
        "column": "keywords",
        "redis_input_key_raw_movies": "movies_csv",
        "redis_input_key_cleaned_movies": "movies_data",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)

prepare_production_companies_data_node = PythonOperator(
    task_id="prepare_production_companies_data",
    dag=staging_dag,
    trigger_rule="one_success",
    python_callable=_prepare_json_in_movies,
    op_kwargs={
        "column": "production_companies",
        "redis_input_key_raw_movies": "movies_csv",
        "redis_input_key_cleaned_movies": "movies_data",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)


###########################################################


def test(
    redis_host: str,
    redis_port: str,
    redis_db: str,
):

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    data = context.deserialize(redis_client.get("production_companies_data"))
    
    genres_df = pd.DataFrame(data)

    print(genres_df)


test_node = PythonOperator(
    task_id="test",
    dag=staging_dag,
    trigger_rule="none_failed",
    python_callable=test,
    op_kwargs={
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
)

    

start_node >> [load_ai_places_node, load_wikidata_locations_node, load_movies_csv_node] >> prepare_movies_data_node
prepare_movies_data_node >> [prepare_genres_data_node, prepare_keywords_data_node, prepare_production_companies_data_node] >> test_node


################################################################

#
#def _merging_data(
#    redis_output_key: str,
#    redis_host: str,
#    redis_port: int,
#    redis_db: int,
#):
#    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
#    context = pa.default_serialization_context()
#    wikidata_data = context.deserialize(redis_client.get("wikidata_df"))
#    dbpedia_data = context.deserialize(redis_client.get("dbpedia_df"))
#    wikidata_df = pd.DataFrame(wikidata_data)
#    dbpedia_df = pd.DataFrame(dbpedia_data)
#    wikidata_df["Song Title"] = wikidata_df["Song Title"].str[1:-1]
#    merged_df = pd.merge(wikidata_df, dbpedia_df, how="outer", on=["Song Title"])
#    # saving result to redis
#    redis_client.set(redis_output_key, context.serialize(merged_df).to_buffer().to_pybytes())
#
#merging_node = PythonOperator(
#    task_id="merging_data",
#    dag=wrangling_dag,
#    trigger_rule="none_failed",
#    python_callable=_merging_data,
#    op_kwargs={
#        "redis_output_key": "merged_df",
#        "redis_host": "rejson",
#        "redis_port": 6379,
#        "redis_db": 0,
#    },
#)
#
#def _cleansing_data(
#    redis_output_key: str,
#    redis_input_key:str,
#    redis_host: str,
#    redis_port: int,
#    redis_db: int,
#):
#    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
#    context = pa.default_serialization_context()
#    data = context.deserialize(redis_client.get(redis_input_key))
#    df = pd.DataFrame(data)
#    #droping unimportant columns
#    df = df.drop(["url","recordLabel","_id","Ref(s)","Wikipedia endpoint","Notes","origin"],axis=1)
#    #formating and adding two cols from metadata
#    target_type = []
#    artists = []
#    for elem in df["wikidata_metadata"]:
#        if(type(elem)!=dict):
#            target_type.append("")
#            artists.append("")
#        if(type(elem)==dict):
#            target_type.append(elem.get("target", "") and elem["target"][0]["type_label"]["value"])
#            artists.append(elem.get("artists", "") and elem["artists"][0]["author_label"]["value"])
#    df['Target Type'] = target_type
#    df['Song Artist'] = artists
#    # drop wikidata table after formating its content in cols
#    df = df.drop(["wikidata_metadata"],axis=1)
#    # merging information to handle nan
#    artists_df = df['Artist(s)'].combine_first(df['Song Artist'])
#    released_df = df['released'].combine_first(df['Date Released'])
#    df = df.join(artists_df, lsuffix='_caller', rsuffix='_song')
#    df = df.join(released_df, lsuffix='_caller', rsuffix='_song')
#    df = df.drop(["Artist(s)_caller","released_caller","Date Released","Song Artist"],axis=1)
#    #storing in redis
#    redis_client.set(redis_output_key, context.serialize(df).to_buffer().to_pybytes())
#
#cleansing_node = PythonOperator(
#    task_id="cleansing_data",
#    dag=wrangling_dag,
#    trigger_rule="none_failed",
#    python_callable=_cleansing_data,
#    op_kwargs={
#        "redis_output_key": "df",
#        "redis_input_key":"merged_df",
#        "redis_host": "rejson",
#        "redis_port": 6379,
#        "redis_db": 0,
#    },
#)
#
#def _person_request(target_id: str, endpoint: str, url: str):
#    # Wikidata query to get target information 
#    sparql_query = (
#    "SELECT DISTINCT ?occupation_label ?first_name ?last_name ?birth_place "
#        "WHERE { "
#        f"wd:{target_id} wdt:P106 ?occupation_id. "
#        "?occupation_id rdfs:label ?occupation_label. "
#        f"wd:{target_id} wdt:P735|wdt:P1477|wdt:P1559 ?first_name_id. "
#        "?first_name_id rdfs:label ?first_name. "
#        f"wd:{target_id} wdt:P734 ?last_name_id. "
#        "?last_name_id rdfs:label ?last_name. "
#        f"wd:{target_id} wdt:P19 ?birth_place_id. "
#        "?birth_place_id rdfs:label ?birth_place. "
#        "filter(lang(?occupation_label) = 'en') "
#        "filter(lang(?first_name) = 'en') "
#        "filter(lang(?last_name) = 'en') "
#        "filter(lang(?birth_place) = 'en') "
#        "}"
#    )
#    r = requests.get(f"{url}{endpoint}", params={"format": "json", "query": sparql_query})
#    if not r.ok:
#        # Probable too many requests, so timeout and retry
#        sleep(1)
#        r = requests.get(
#            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
#        )
#    return r.json()
#
#def _group_request(target_id: str, endpoint: str, url: str):
#    # Wikidata query to get target information 
#    sparql_query = (
#        "SELECT DISTINCT (sample(?name) as ?name) ?inception ?origin_country_label (count(?nominations) as ?nb_nominations) "
#        "WHERE "
#        "{ "
#            "OPTIONAL{ "
#                f"wd:{target_id} rdfs:label ?name. "
#                "filter(lang(?name) = 'en') "
#            "} "
#            "OPTIONAL{ "
#                f"wd:{target_id} wdt:P571 ?inception. "
#            "} "
#            "OPTIONAL{ "
#                f"wd:{target_id} wdt:P495 ?origin_country. "
#                "?origin_country rdfs:label ?origin_country_label. "
#                "filter(lang(?origin_country_label) = 'en') "
#            "} "
#            "OPTIONAL{ "
#                f"wd:{target_id} wdt:P1411 ?nominations. "
#            "} "
#        "} "
#        "GROUP BY ?inception ?origin_country_label "
#    )
#    r = requests.get(f"{url}{endpoint}", params={"format": "json", "query": sparql_query})
#    if not r.ok:
#        # Probable too many requests, so timeout and retry
#        sleep(1)
#        r = requests.get(
#            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
#        )
#    return r.json()
#
#def _data_enrichment(
#    redis_host: str,
#    redis_port: int,
#    redis_db: int,
#    endpoint: str,
#    url: str,
#):
#
#    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
#    context = pa.default_serialization_context()
#    df = context.deserialize(redis_client.get("df"))
#
#    groups_fdata = []
#    persons_fdata = []
#    for row in df.iterrows(): 
#        if(row[1]["Target Type"] == "human"):
#            target_id = row[1]["Wikidata target id"] 
#            person_data = _person_request(target_id,endpoint,url)
#            if person_data["results"]["bindings"]:
#                x = person_data["results"]["bindings"][0]
#                persons_fdata.append({
#                    "Occupation Label": x["occupation_label"]["value"],
#                    "First Name": x["first_name"]["value"],
#                    "Last Name": x["last_name"]["value"],
#                    "Birth Place": x["birth_place"]["value"],
#                    "person id": target_id
#                })
#        elif("group" in row[1]["Target Type"].lower() or "duo" in row[1]["Target Type"].lower()):
#            target_id = row[1]["Wikidata target id"] 
#            group_data = _group_request(target_id,endpoint,url)
#            if group_data["results"]["bindings"]:
#                x = group_data["results"]["bindings"][0]
#                groups_fdata.append({
#                    "Name": x["name"]["value"],
#                    "Inception": x["inception"]["value"],
#                    "Country": x["origin_country_label"]["value"],
#                    "Number of Nominations": x["nb_nominations"]["value"],
#                    "group id": target_id
#                })
#                
#    persons_df = pd.DataFrame(persons_fdata)
#    groups_df = pd.DataFrame(groups_fdata)
#    df = df.drop(["Target Type"],axis=1)
#    print(df)
#
#    #storing in redis
#    redis_client.set("persons_df", context.serialize(persons_df).to_buffer().to_pybytes())
#    redis_client.set("groups_df", context.serialize(groups_df).to_buffer().to_pybytes())
#    redis_client.set("df", context.serialize(df).to_buffer().to_pybytes())
#
#enrichment_node = PythonOperator(
#    task_id="data_enrichment",
#    dag=wrangling_dag,
#    trigger_rule="none_failed",
#    python_callable=_data_enrichment,
#    op_kwargs={
#        "redis_host": "rejson",
#        "redis_port": 6379,
#        "redis_db": 0,
#        "endpoint": "/sparql",
#        "url": "https://query.wikidata.org",
#    },
#)
#
#def _saving_to_postgres(
#    redis_songs_key: str,
#    redis_groups_key: str,
#    redis_persons_key: str,
#    redis_host: str,
#    redis_port: int,
#    redis_db: int,
#    postgres_host: str,
#    postgres_port: int,
#    postgres_db: str,
#    postgres_user: str,
#    postgres_pswd: str,
#):
#    Base = declarative_base()
#    class Song(Base):
#        __tablename__ = "song"
#        id = Column(Integer, primary_key=True)
#        title = Column(String, nullable=False)
#        release_date = Column(DateTime)
#        record_date = Column(DateTime)
#        genre = Column(String)
#        wikidata_id = Column(String)
#        artist_id = Column(Integer, ForeignKey("entity.id"), nullable=False)
#        target_id = Column(Integer, ForeignKey("entity.id"), nullable=False)
#      
#        artist = relationship(
#            "Entity", backref="produced_disses", foreign_keys=[artist_id]
#        )
#        target = relationship(
#            "Entity", backref="targeted_disses", foreign_keys=[target_id]
#        )
#        def __repr__(self):
#            return f"Song(id={self.id!r}, title={self.title!r}, release_date={self.release_date!r}, release_date={self.release_date!r})"
#  
#      
#    class Entity(Base):
#        __tablename__ = "entity"
#        id = Column(Integer, primary_key=True)
#        name = Column(String, nullable=False)
#        type = Column(String(50))
#        wikidata_id = Column(String)
#        __mapper_args__ = {
#            "polymorphic_identity": "entity",
#            "polymorphic_on": type,
#        }
#      
#        def __repr__(self):
#            return f"Entity(id={self.id!r}, name={self.name!r}, produced_disses={self.produced_disses!r},  targeted_disses={self.targeted_disses!r})"
#      
#    class Human(Entity):
#        __tablename__ = "human"
#        id = Column(Integer, ForeignKey("entity.id"), primary_key=True)
#        occupation = Column(String)
#        first_name = Column(String)
#        last_name = Column(String)
#        birth_place = Column(String)
#        __mapper_args__ = {
#            "polymorphic_identity": "human",
#        }
#      
#    class Group(Entity):
#        __tablename__ = "group"
#        id = Column(Integer, ForeignKey("entity.id"), primary_key=True)
#        country = Column(String)
#        nb_nominations = Column(Integer)
#        inception = Column(DateTime)
#        __mapper_args__ = {
#            "polymorphic_identity": "group",
#        }
#      
#    class Other(Entity):
#        __tablename__ = "other"
#        id = Column(Integer, ForeignKey("entity.id"), primary_key=True)
#        __mapper_args__ = {
#            "polymorphic_identity": "other",
#        }
#    engine = create_engine(
#        f"postgresql://{postgres_user}:{postgres_pswd}@{postgres_host}:{postgres_port}/{postgres_db}"
#    )
#  
#    Base.metadata.drop_all(engine)
#    Base.metadata.create_all(engine)
#    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
#    context = pa.default_serialization_context()
#    songs = context.deserialize(redis_client.get(redis_songs_key))
#    groups = context.deserialize(redis_client.get(redis_groups_key))
#    persons = context.deserialize(redis_client.get(redis_persons_key))
#    songs_df = pd.DataFrame(songs, dtype=str)
#    groups_df = pd.DataFrame(groups, dtype=str)
#    persons_df = pd.DataFrame(persons, dtype=str)
#
#    def convert_date(str_date: str):
#        try:
#            return datetime.datetime.strptime(str_date, "%Y-%m-%d")
#        except ValueError:
#            try:
#                return datetime.datetime.strptime(str_date, "%Y")
#            except ValueError:
#                return None
#          
#    with Session(engine) as session:
#        available_entities = {}
#      
#        for row in groups_df.iterrows():
#            name = row[1]["Name"]
#            country = row[1]["Country"]
#            nb_nominations = int(row[1]["Number of Nominations"])
#            inception = convert_date(row[1]["Inception"])
#            target_id = row[1]["group id"]
#            group = Group(name = name, wikidata_id = target_id, country = country, nb_nominations = nb_nominations, inception = inception)
#            available_entities[target_id] = group
#            session.add(group)
#      
#        for row in persons_df.iterrows():
#            name = f'{row[1]["Last Name"]} {row[1]["First Name"]}'
#            occupation = row[1]["Occupation Label"]
#            first_name = row[1]["First Name"]
#            last_name = row[1]["Last Name"]
#            birth_place = row[1]["Birth Place"]
#            target_id = row[1]["person id"]
#          
#            person = Human(name = name, wikidata_id = target_id, occupation = occupation, first_name = first_name, last_name = last_name, birth_place = birth_place)
#            available_entities[target_id] = person
#            session.add(person)
#        for row in songs_df.iterrows():
#            recorded = convert_date(row[1]["recorded"])
#            released = convert_date(row[1]["released_song"])
#            artists_names = row[1]["Artist(s)_song"]
#            song_wiki_id = row[1]["Wikidata song id"]
#            target_wiki_id = row[1]["Wikidata target id"]
#            Targets_names = row[1]["Target(s)"]
#            genre = row[1]["genre"]
#            song_title = row[1]["Song Title"]
#            song = Song(title=song_title, release_date = released, genre=genre, record_date = recorded, wikidata_id = song_wiki_id, artist=Other(name=artists_names), target=available_entities.get(target_wiki_id, "") or Other(name=Targets_names))
#            session.add(song)
#        session.commit()
#
#saving_node = PythonOperator(
#    task_id="saving_to_postgres",
#    dag=wrangling_dag,
#    trigger_rule="none_failed",
#    python_callable=_saving_to_postgres,
#    op_kwargs={
#        "redis_songs_key": "df",
#        "redis_groups_key": "groups_df",
#        "redis_persons_key": "persons_df",
#        "redis_host": "rejson",
#        "redis_port": 6379,
#        "redis_db": 0,
#        "postgres_host": "postgres",
#        "postgres_port": 5432,
#        "postgres_db": "postgres",
#        "postgres_user": "airflow",
#        "postgres_pswd": "airflow",
#    },
#)
#
#get_wikidata_node >> get_dbpedia_node >> merging_node >> cleansing_node >> enrichment_node >> saving_node
#
