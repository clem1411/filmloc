import datetime
from os import system
from subprocess import CalledProcessError, check_output, STDOUT

import pandas as pd

from sqlalchemy import create_engine
from py2neo import Graph

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


# PostgreSQL connection settings
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "airflow"
POSTGRES_PSWD = "airflow"

# Neo4j connection settings
NEO4J_HOST = "neo4j"
NEO4J_PORT = 7687

# Default arguments
default_args_dict = {
    "start_date": datetime.datetime(2025, 1, 4, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 0 * * *",  # Every day at midnight
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=30),
}

# Define the DAG
production_dag = DAG(
    dag_id="production_dag",
    default_args=default_args_dict,
    catchup=False,
)

### START
start_node = EmptyOperator(
    task_id="start", dag=production_dag, trigger_rule="all_success"
)

### CREATE GRAPH IN NEO4J FROM POSTGRESQL
def _create_graph_from_postgres_batch(
    pg_user: str,
    pg_pwd: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    neo_host: str,
    neo_port: str,
    batch_size: int = 1000  # Taille des lots pour traiter les données par paquets
):
    # Connexion à PostgreSQL
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}'
    )

    # Connexion à Neo4j
    graph = Graph(f"bolt://{neo_host}:{neo_port}")

    # Interroger les films et leurs données associées
    query_movies = """
        SELECT m.id, m.original_title, m.release_date, m.runtime, m.vote_average, m.budget, m.revenue
        FROM movie m
    """
    df_movies = pd.read_sql(query_movies, con=engine)

    # Nettoyer les données
    df_movies = df_movies.fillna({
        'runtime': 0,
        'vote_average': 0,
        'budget': 0,
        'revenue': 0
    })
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')
    df_movies['release_date'] = df_movies['release_date'].fillna(pd.Timestamp('1970-01-01'))

    # Boucle à travers les données par petits lots
    for start in range(0, len(df_movies), batch_size):
        batch = df_movies.iloc[start:start + batch_size]
        
        # Créer une transaction dans Neo4j pour ce lot
        tx = graph.begin()

        # Traitement par lot pour les films
        tx.evaluate('''
        UNWIND $movies AS movie
        MERGE (m:Movie {id: movie.id, original_title: movie.original_title, release_date: movie.release_date, runtime: movie.runtime, vote_average: movie.vote_average, budget: movie.budget, revenue: movie.revenue})
        ''', parameters={"movies": batch.to_dict(orient="records")})

        print("movie:" + str(start))               

        # Récupérer les genres
        query_genres = """
            SELECT g."name", mg.movie_id
            FROM genre g
            JOIN "movie-genre" mg ON g.id = mg.genre_id
            WHERE mg.movie_id IN 
        """
        movie_ids = batch['id'].tolist()
        query_genres += str(tuple(movie_ids))
        df_genres = pd.read_sql(query_genres, con=engine)

        # Création des genres et des relations
        tx.evaluate('''
        UNWIND $genres AS genre
        MATCH (m:Movie {id: genre.movie_id})
        WITH m, genre
        MERGE (g:Genre {name: genre.name})
        MERGE (m)-[:HAS_GENRE]->(g)
        ''', parameters={"genres": df_genres.to_dict(orient="records")})

        print("genre:" + str(start))
        
        # Récupérer les mots-clés
        query_keywords = """
            SELECT k."name", mk.movie_id
            FROM keyword k
            JOIN "movie-keyword" mk ON k.id = mk.keyword_id
            WHERE mk.movie_id IN 
        """
        query_keywords += str(tuple(movie_ids))
        df_keywords = pd.read_sql(query_keywords, con=engine)
        
        # Création des mots-clés et des relations
        tx.evaluate('''
        UNWIND $keywords AS keyword
        MATCH (m:Movie {id: keyword.movie_id})
        WITH m, keyword
        MERGE (k:Keyword {name: keyword.name})
        MERGE (m)-[:HAS_KEYWORD]->(k)
        ''', parameters={"keywords": df_keywords.to_dict(orient="records")})

        print("keyword:" + str(start))
        
        # Récupérer les sociétés de production
        query_prod_companies = """
            SELECT p."name", mp.movie_id
            FROM prod_company p
            JOIN "movie-prod_company" mp ON p.id = mp.prod_company_id
            WHERE mp.movie_id IN 
        """
        query_prod_companies += str(tuple(movie_ids))
        df_prod_companies = pd.read_sql(query_prod_companies, con=engine)
        
        # Création des sociétés de production et des relations
        tx.evaluate('''
        UNWIND $prod_companies AS prod_company
        MATCH (m:Movie {id: prod_company.movie_id})
        WITH m, prod_company
        MERGE (p:ProdCompany {name: prod_company.name})
        MERGE (m)-[:HAS_PROD_COMPANY]->(p)
        ''', parameters={"prod_companies": df_prod_companies.to_dict(orient="records")})

        print("prodcompany:" + str(start))
        
        # Récupérer les lieux
        query_locations = """
            SELECT l."name", ml.movie_id
            FROM location l
            JOIN "movie-location" ml ON l.id = ml.location_id
            WHERE ml.movie_id IN 
        """
        query_locations += str(tuple(movie_ids))
        df_locations = pd.read_sql(query_locations, con=engine)
        
        # Création des lieux et des relations
        tx.evaluate('''
        UNWIND $locations AS location
        MATCH (m:Movie {id: location.movie_id})
        WITH m, location
        MERGE (l:Location {name: location.name})
        MERGE (m)-[:HAS_LOCATION]->(l)
        ''', parameters={"locations": df_locations.to_dict(orient="records")})

        print("location:" + str(start))
        
        # Valider la transaction pour ce lot
        tx.commit()

    # Fermer la connexion PostgreSQL
    engine.dispose()


    
create_graph_node = PythonOperator(
    task_id="create_graph_from_postgres",
    dag=production_dag,
    python_callable=_create_graph_from_postgres_batch,
    op_kwargs={
        "pg_user": POSTGRES_USER,
        "pg_pwd": POSTGRES_PSWD,
        "pg_host": POSTGRES_HOST,
        "pg_port": POSTGRES_PORT,
        "pg_db": POSTGRES_DB,
        "neo_host": NEO4J_HOST,
        "neo_port": NEO4J_PORT,
        "batch_size" : 1000,
    },
)


### LAUNCH JUPYTER ANALYTICS NOTEBOOK
notebook_task = PapermillOperator(
    task_id="run_analytics_notebook",
    dag=production_dag,
    trigger_rule="all_success",
    input_nb="/opt/airflow/data/analytics.ipynb",
    output_nb="/opt/airflow/results/out.ipynb",
    parameters={},
)


### END
end_node = EmptyOperator(
    task_id="end", dag=production_dag, trigger_rule="all_success"
)

### SET THE TASK SEQUENCE
start_node >> create_graph_node >> notebook_task >> end_node
