import json
from typing import Any
import urllib.parse
import datetime
from time import sleep
import requests

from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, relationship, Session

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

import redis
from redis.commands.json.path import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

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

### LOAD DATE FROM MONGODB TO REDIS

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
    task_id="load_wikidata_location",
    dag=staging_dag,
    trigger_rule="one_success",
    python_callable=_load_data_from_mongodb,
    op_kwargs={
        "redis_output_key": "wikidata_location",
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
    df = df.reset_index(drop=True)
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
    df_relations['id'] = df_relations.index
    
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


preparation_finish_node = EmptyOperator(
    task_id="preparation_finish", dag=staging_dag, trigger_rule="all_success"
)



### MERGE LOCATIONS AND PLACES TO OBTAIN ONE DATAFRAME

def _merge_places_locations(
        redis_output_key: str,
        redis_input_key_locations:str,
        redis_input_key_places:str,
        redis_host: str,
        redis_port: str,
        redis_db: str,
):

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()

    # Get raw data
    wikidata_location = context.deserialize(redis_client.get(redis_input_key_locations))
    ai_places = context.deserialize(redis_client.get(redis_input_key_places))
    wikidata_location_df = pd.DataFrame(wikidata_location)
    ai_places_df = pd.DataFrame(ai_places)

    # First clean raw data
    wikidata_location_df = wikidata_location_df.drop(['_id'], axis=1)
    ai_places_df = ai_places_df.drop(['_id'], axis=1)

    # Merge
    merged_df = pd.merge(wikidata_location_df, ai_places_df, how='outer', on=["title"])


    # Clean
    merged_df = merged_df.dropna(thresh=2) # garde les lignes avec au moins 2 non null

    # Add movie id
    movies_df = pd.DataFrame(context.deserialize(redis_client.get("movies_data")))
    movies_df = movies_df.rename(columns={'original_title': 'title'})
    final_df = pd.merge(merged_df, movies_df[['title','id']], how='right', on='title')

    # saving result to redis
    redis_client.set(redis_output_key, context.serialize(final_df).to_buffer().to_pybytes())


merge_places_locations_node = PythonOperator(
    task_id="merge_places_locations",
    dag=staging_dag,
    trigger_rule="all_success",
    python_callable=_merge_places_locations,
    op_kwargs={
        "redis_output_key": "separated_loc_data",
        "redis_input_key_locations": "wikidata_location",
        "redis_input_key_places" : "ai_places",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)


### WORK ON LOCATION

def _merge_loc_data(
        redis_output_key: str,
        redis_input_key_loc: str,
        redis_host: str,
        redis_port: str,
        redis_db: str,
):

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()

    # Get loc data
    separated_loc = context.deserialize(redis_client.get(redis_input_key_loc))
    separated_loc_df = pd.DataFrame(separated_loc)


    # Switch  NaN to empty list
    separated_loc_df['filmingLocation'] = separated_loc_df['filmingLocation'].apply(lambda x: x if isinstance(x, list) else [])
    separated_loc_df['narrativeLocation'] = separated_loc_df['narrativeLocation'].apply(lambda x: x if isinstance(x, list) else [])
    separated_loc_df['places'] = separated_loc_df['places'].apply(lambda x: x if isinstance(x, list) else [])

    # Concat  3 list columns  (filmingLocation, narrativeLocation, places)
    separated_loc_df['location'] = separated_loc_df['filmingLocation'] + separated_loc_df['narrativeLocation'] + separated_loc_df['places']

    # Create DataFrame with columns 'title', 'id' and 'location'
    merged_loc_df = separated_loc_df[['title', 'id', 'location']]

    # saving result to redis
    redis_client.set(redis_output_key, context.serialize(merged_loc_df).to_buffer().to_pybytes())


merge_loc_node = PythonOperator(
    task_id="merge_loc",
    dag=staging_dag,
    trigger_rule="all_success",
    python_callable=_merge_loc_data,
    op_kwargs={
        "redis_output_key": "merged_loc_data",
        "redis_input_key_loc": "separated_loc_data",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)

def clustering_locations(location_df):


    locations_list = list(set(location_df['location']))
    
    # Step 1 : Transform locations to TF-IDF vectors
    vectorizer = TfidfVectorizer(stop_words='english')
    X = vectorizer.fit_transform(locations_list)

    # Step 2 : Compute cosine similarity
    similarity_matrix = cosine_similarity(X)

    # Step 3 : Convert similarity to distance
    distance_matrix = 1 - similarity_matrix

    # Force negative values to zero to avoid error
    distance_matrix[distance_matrix < 0] = 0
    
    # Step 4 : Clustering with DBSCAN
    # - eps : distance maximale entre les points dans un cluster
    # - min_samples : nombre minimal de points dans un cluster
    
    db = DBSCAN(metric="precomputed", eps=0.15, min_samples=1)
    labels = db.fit_predict(distance_matrix)
    

    # Step 5 : Create clusters
    clusters = {}
    for i, label in enumerate(labels):
        if label != -1:  # Ignorer le bruit (-1 signifie bruit)
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(locations_list[i])


    # Step 6 : Select the location with the fewest characters for each cluster 
    locations_dict = dict() # old location -> id of the new location
    unique_locations = [] # list of new location
    cpt_id = 0 # actual id
    for label, cluster in clusters.items():
        if len(cluster) > 1:  # If the cluster contains more than 1 element
            
            min_location = min(cluster, key=len) 
            unique_locations.append(min_location)
            for c in cluster:
                locations_dict[c] = cpt_id
        else:
            unique_locations.append(cluster[0]) 
            locations_dict[cluster[0]] = cpt_id

        cpt_id += 1 #increment id


            
    # Step 7 : Create a new dataframe with unique locations
    df_unique_locations = pd.DataFrame({
        'location': unique_locations,
        'id': [locations_dict[loc] for loc in unique_locations],
    })


    # Step 8 : Replace text value by id
    location_df['location'] = location_df['location'].replace(locations_dict)

    # Step 9: Remove rows where 'location' is not an ID (i.e., it is still text)
    location_df = location_df[location_df['location'].apply(lambda x: isinstance(x, int))]

    # Return the location table and relation table
    return df_unique_locations, location_df


    

def _prepare_locations_data(
        redis_output_key_loc: str,
        redis_output_key_relation: str,
        redis_input_key_loc: str,
        redis_host: str,
        redis_port: str,
        redis_db: str,
):

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()

    # Get loc data
    loc = context.deserialize(redis_client.get(redis_input_key_loc))
    loc_df = pd.DataFrame(loc)

    # Explode lists in the 'location' column to have one line per location
    loc_exploded_df = loc_df.explode('location', ignore_index=True)
    
    # Remove NaN values
    loc_exploded_df = loc_exploded_df.dropna(subset=['location']).reset_index(drop=True)

    # Clustering locations to remove duplicated values and replace values by id in the relation df
    unique_loc_df, relation_loc_df = clustering_locations(loc_exploded_df)

    # Reformat relation df
    relation_loc_df = relation_loc_df.drop(['title'], axis=1).reset_index(drop=True)
    relation_loc_df = relation_loc_df.rename(columns={'id': 'movie_id', 'location':'location_id'})
    relation_loc_df['id'] = relation_loc_df.index
    
    # saving result to redis
    redis_client.set(redis_output_key_loc, context.serialize(unique_loc_df).to_buffer().to_pybytes())
    redis_client.set(redis_output_key_relation, context.serialize(relation_loc_df).to_buffer().to_pybytes())
    
prepare_locations_data_node = PythonOperator(
    task_id="prepare_locations_data",
    dag=staging_dag,
    trigger_rule="all_success",
    python_callable=_prepare_locations_data,
    op_kwargs={
        "redis_output_key_loc": "locations_data",
        "redis_output_key_relation": "movies-locations_data",
        "redis_input_key_loc": "merged_loc_data",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
    },
    depends_on_past=False,
)



### SAVE TO POSTGRES


def _save_to_postgres(
    redis_movies_key: str,
    redis_locations_key: str,
    redis_movies_locations_key: str,
    redis_keywords_key: str,
    redis_movies_keywords_key: str,
    redis_genres_key: str,
    redis_movies_genres_key: str,
    redis_prodcompanies_key:str,
    redis_movies_prodcompanies_key: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_user: str,
    postgres_pswd: str,
):
    Base = declarative_base()

    # Entity class
    class Movie(Base):
        __tablename__ = "movie"
        id = Column(Integer, primary_key=True, autoincrement=False)
        original_title = Column(String, nullable=False)
        release_date = Column(DateTime)
        runtime = Column(Float)
        vote_average = Column(Float, nullable=False)
        budget = Column(Float)
        revenue = Column(Float)
        #target_id = Column(Integer, ForeignKey("entity.id"), nullable=False)
      
        
        #target = relationship(
        #    "Entity", backref="targeted_disses", foreign_keys=[target_id]
        #)
        def __repr__(self):
            return f"Movie(id={self.id!r}, original_title={self.title!r}, release_date={self.release_date!r}, runtime={self.runtime!r}, vote_average={self.vote_average!r}, budget={self.budget!r}, revenue={self.revenue!r})"
  

    class Keyword(Base):
        __tablename__ = "keyword"
        id = Column(Integer, primary_key=True, autoincrement=False)
        name = Column(String, nullable=False)

        def __repr__(self):
            return f"Keyword(id={self.id!r}, name={self.name!r})"

    class Genre(Base):
        __tablename__ = "genre"
        id = Column(Integer, primary_key=True, autoincrement=False)
        name = Column(String, nullable=False)

        def __repr__(self):
            return f"Genre(id={self.id!r}, name={self.name!r})"

    class ProdCompany(Base):
        __tablename__ = "prod_company"
        id = Column(Integer, primary_key=True, autoincrement=False)
        name = Column(String, nullable=False)

        def __repr__(self):
            return f"ProdCompany(id={self.id!r}, name={self.name!r})"


    class Location(Base):
        __tablename__ = "location"
        id = Column(Integer, primary_key=True, autoincrement=False)
        name = Column(String, nullable=False)

        def __repr__(self):
            return f"Location(id={self.id!r}, name={self.name!r})"


    # Join class

    class MovieKeyword(Base):
        __tablename__ = "movie-keyword"
        id = Column(Integer, primary_key=True, autoincrement=False)
        movie_id = Column(Integer, ForeignKey("movie.id"), nullable=False)
        keyword_id = Column(Integer, ForeignKey("keyword.id"), nullable=False)

        def __repr__(self):
            return f"MovieKeyword(id={self.id!r}, movie_id={self.movie_id!r}, keyword_id={self.keyword_id!r})"

    class MovieGenre(Base):
        __tablename__ = "movie-genre"
        id = Column(Integer, primary_key=True, autoincrement=False)
        movie_id = Column(Integer, ForeignKey("movie.id"), nullable=False)
        genre_id = Column(Integer, ForeignKey("genre.id"), nullable=False)

        def __repr__(self):
            return f"MovieGenre(id={self.id!r}, movie_id={self.movie_id!r}, genre_id={self.genre_id!r})"

    class MovieProdCompany(Base):
        __tablename__ = "movie-prod_company"
        id = Column(Integer, primary_key=True, autoincrement=False)
        movie_id = Column(Integer, ForeignKey("movie.id"), nullable=False)
        prod_company_id = Column(Integer, ForeignKey("prod_company.id"), nullable=False)

        def __repr__(self):
            return f"MovieProd_Company(id={self.id!r}, movie_id={self.movie_id!r}, prod_company_id={self.prod_company_id!r})"
    
    class MovieLocation(Base):
        __tablename__ = "movie-location"
        id = Column(Integer, primary_key=True, autoincrement=False)
        movie_id = Column(Integer, ForeignKey("movie.id"), nullable=False)
        location_id = Column(Integer, ForeignKey("location.id"), nullable=False)

        def __repr__(self):
            return f"MovieLocation(id={self.id!r}, movie_id={self.movie_id!r}, location_id={self.location_id!r})"


        
    # Connection to database
    engine = create_engine(
        f"postgresql://{postgres_user}:{postgres_pswd}@{postgres_host}:{postgres_port}/{postgres_db}"
    )

    # Tester la connexion
    try:
        # Essayer de se connecter et obtenir la version de la base de données
        with engine.connect() as connection:
            result = connection.execute("SELECT version()")
            print("Connexion réussie ! Version de PostgreSQL :", result.fetchone()[0])
    except OperationalError as e:
        print("Erreur lors de la connexion à PostgreSQL :", e)

        
    Base.metadata.drop_all(engine) # Drop all table
    Base.metadata.create_all(engine) # Create all table

    # Connection to redis
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()

    # Get all data
    movies_data = context.deserialize(redis_client.get(redis_movies_key))
    keywords_data = context.deserialize(redis_client.get(redis_keywords_key))
    genres_data = context.deserialize(redis_client.get(redis_genres_key))
    locations_data = context.deserialize(redis_client.get(redis_locations_key))
    prod_companies_data = context.deserialize(redis_client.get(redis_prodcompanies_key))

    movies_keywords_data = context.deserialize(redis_client.get(redis_movies_keywords_key))
    movies_genres_data = context.deserialize(redis_client.get(redis_movies_genres_key))
    movies_prodcompanies_data = context.deserialize(redis_client.get(redis_movies_prodcompanies_key))
    movies_locations_data = context.deserialize(redis_client.get(redis_movies_locations_key))


    movies_df = pd.DataFrame(movies_data, dtype=str)
    keywords_df = pd.DataFrame(keywords_data, dtype=str)
    genres_df = pd.DataFrame(genres_data, dtype=str)
    locations_df = pd.DataFrame(locations_data, dtype=str)
    prod_companies_df = pd.DataFrame(prod_companies_data, dtype=str)

    movies_keywords_df = pd.DataFrame(movies_keywords_data, dtype=str)
    movies_genres_df = pd.DataFrame(movies_genres_data, dtype=str)
    movies_prodcompanies_df = pd.DataFrame(movies_prodcompanies_data, dtype=str)
    movies_locations_df = pd.DataFrame(movies_locations_data, dtype=str)


    # Function to convert string to date
    def convert_date(str_date: str):
        try:
            return datetime.datetime.strptime(str_date, "%d/%m/%Y")
        except ValueError:
            return None

    # Open a session to insert data
    with Session(engine) as session:
      
        for row in movies_df.iterrows():
            id = row[1]["id"]
            original_title = row[1]["original_title"]
            release_date = convert_date(str(row[1]["release_date"]))
            runtime = float(row[1]["runtime"])
            vote_average = float(row[1]["vote_average"])
            budget = float(row[1]["budget"])
            revenue = float(row[1]["revenue"])
            movie = Movie(id = id, original_title = original_title, release_date = release_date, runtime = runtime, vote_average = vote_average, budget = budget, revenue = revenue)

            session.add(movie)
      
        for row in keywords_df.iterrows():
            id = row[1]["id"]
            name = row[1]["name"]
            keyword = Keyword(id = id, name = name)

            session.add(keyword)

        for row in genres_df.iterrows():
            id = row[1]["id"]
            name = row[1]["name"]
            genre = Genre(id = id, name = name)

            session.add(genre)

        for row in prod_companies_df.iterrows():
            id = row[1]["id"]
            name = row[1]["name"]
            prodcompany = ProdCompany(id = id, name = name)

            session.add(prodcompany)

        for row in locations_df.iterrows():
            id = row[1]["id"]
            name = row[1]["location"]
            location = Location(id = id, name = name)

            session.add(location)

        # Commit changes
        session.commit()

        for row in movies_genres_df.iterrows():
            id = row[1]["id"]
            movie_id = row[1]["movie_id"]
            genre_id = row[1]["genres_id"]
            mg = MovieGenre(id = id, movie_id = movie_id, genre_id = genre_id)

            session.add(mg)

        for row in movies_keywords_df.iterrows():
            id = row[1]["id"]
            movie_id = row[1]["movie_id"]
            keyword_id = row[1]["keywords_id"]
            mk = MovieKeyword(id = id, movie_id = movie_id, keyword_id = keyword_id)

            session.add(mk)

        for row in movies_prodcompanies_df.iterrows():
            id = row[1]["id"]
            movie_id = row[1]["movie_id"]
            prod_company_id = row[1]["production_companies_id"]
            mp = MovieProdCompany(id = id, movie_id = movie_id, prod_company_id = prod_company_id)

            session.add(mp)

        for row in movies_locations_df.iterrows():
            id = row[1]["id"]
            movie_id = row[1]["movie_id"]
            location_id = row[1]["location_id"]
            ml = MovieLocation(id = id, movie_id = movie_id, location_id = location_id)

            session.add(ml)

        # Commit changes
        session.commit()

save_postgres_node = PythonOperator(
    task_id="save_to_postgres",
    dag=staging_dag,
    trigger_rule="all_success",
    python_callable=_save_to_postgres,
    op_kwargs={
        "redis_movies_key": "movies_data",
        "redis_locations_key": "locations_data",
        "redis_movies_locations_key": "movies-locations_data",
        "redis_keywords_key": "keywords_data",
        "redis_movies_keywords_key": "movies-keywords_data",
        "redis_genres_key": "genres_data",
        "redis_movies_genres_key": "movies-genres_data",
        "redis_prodcompanies_key": "production_companies_data",
        "redis_movies_prodcompanies_key": "movies-production_companies_data",
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "redis_db": REDIS_DB,
        "postgres_host": "postgres",
        "postgres_port": 5432,
        "postgres_db": "postgres",
        "postgres_user": "airflow",
        "postgres_pswd": "airflow",
    },
    depends_on_past=False,
)



### END

end_node = EmptyOperator(
    task_id="end", dag=staging_dag, trigger_rule="all_success"
)










start_node >> [load_ai_places_node, load_wikidata_locations_node, load_movies_csv_node] >> prepare_movies_data_node
prepare_movies_data_node >> [prepare_genres_data_node, prepare_keywords_data_node, prepare_production_companies_data_node] >> preparation_finish_node

preparation_finish_node >> merge_places_locations_node >> merge_loc_node >> prepare_locations_data_node >> save_postgres_node >> end_node
