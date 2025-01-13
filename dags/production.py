import datetime
from sqlalchemy import create_engine
from py2neo import Graph
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

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

# Function to create graph in Neo4j from PostgreSQL
def _create_graph_from_postgres(
    pg_user: str,
    pg_pwd: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    neo_host: str,
    neo_port: str,
):
    # PostgreSQL connection
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}'
    )
    
    # Create a Neo4j connection
    graph = Graph(f"bolt://{neo_host}:{neo_port}")

    # Reset the database Neo4j
    graph.delete_all()

    # Start a transaction in Neo4j
    tx = graph.begin()

    # Query to fetch movies and their related data
    query_movies = """
        SELECT m.id, m.original_title, m.release_date, m.runtime, m.vote_average, m.budget, m.revenue
        FROM movie m
    """
    df_movies = pd.read_sql(query_movies, con=engine)
    
    # Replace NaN values with None or a default value
    df_movies = df_movies.fillna({
        'runtime': 0,
    })

    # Replace NaT (Not a Time) in datetime columns with None (or another default date)
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')  # Coerce invalid dates to NaT
    df_movies['release_date'] = df_movies['release_date'].fillna(pd.Timestamp('1900-01-01'))  # Replace NaT with a default date

    # Create movie nodes in Neo4j
    for _, row in df_movies.iterrows():
        tx.evaluate('''
        MERGE (m:Movie {id: $id, original_title: $original_title, release_date: $release_date, runtime: $runtime, vote_average: $vote_average, budget: $budget, revenue: $revenue})
        ''', parameters=row.to_dict())

    # Query to fetch genres related to movies
    query_genres = """
    select mg.movie_id, g."name" as genre_name
    from "movie-genre" mg 
    join genre g ON g.id = mg.genre_id 
    """
    df_genres = pd.read_sql(query_genres, con=engine)
    
    # Create genre nodes and relationships
    for _, row in df_genres.iterrows():
        tx.evaluate('''
        MATCH (m:Movie {id: $movie_id})
        WITH m
        MERGE (g:Genre {name: $genre_name})
        MERGE (m)-[:HAS_GENRE]->(g)
        ''', parameters={'genre_name': row['genre_name'], 'movie_id': row['movie_id']})
        print(row)
        
    ## Query to fetch keywords related to movies
    #query_keywords = """
    #    SELECT mk.movie_id, k."name" AS keyword_name
    #    FROM "movie-keyword" mk
    #    JOIN keyword k ON k.id = mk.keyword_id
    #"""
    #df_keywords = pd.read_sql(query_keywords, con=engine)
    #
    ## Create keyword nodes and relationships
    #for _, row in df_keywords.iterrows():
    #    tx.evaluate('''
    #    MERGE (k:Keyword {name: $keyword_name})
    #    MATCH (m:Movie {id: $movie_id})
    #    WITH m
    #    MERGE (m)-[:HAS_KEYWORD]->(k)
    #    ''', parameters={'keyword_name': row['keyword_name'], 'movie_id': row['movie_id']})
    #
    ## Query to fetch locations related to movies
    #query_locations = """
    #    SELECT ml.movie_id, l."name" AS location_name
    #    FROM "movie-location" ml
    #    JOIN location l ON l.id = ml.location_id
    #"""
    #df_locations = pd.read_sql(query_locations, con=engine)
    #
    ## Create location nodes and relationships
    #for _, row in df_locations.iterrows():
    #    tx.evaluate('''
    #    MERGE (l:Location {name: $location_name})
    #    MATCH (m:Movie {id: $movie_id})
    #    WITH m
    #    MERGE (m)-[:FILMED_AT]->(l)
    #    ''', parameters={'location_name': row['location_name'], 'movie_id': row['movie_id']})
    #
    ## Query to fetch production companies related to movies
    #query_prod_companies = """
    #    SELECT mp.movie_id, pc."name" AS prod_company_name
    #    FROM "movie-prod_company" mp
    #    JOIN prod_company pc ON pc.id = mp.prod_company_id
    #"""
    #df_prod_companies = pd.read_sql(query_prod_companies, con=engine)
    #
    ## Create production company nodes and relationships
    #for _, row in df_prod_companies.iterrows():
    #    tx.evaluate('''
    #    MERGE (pc:ProdCompany {name: $prod_company_name})
    #    MATCH (m:Movie {id: $movie_id})
    #    WITH m
    #    MERGE (m)-[:PRODUCED_BY]->(pc)
    #    ''', parameters={'prod_company_name': row['prod_company_name'], 'movie_id': row['movie_id']})

    # Commit the transaction
    tx.commit()

    # Dispose the engine connection
    engine.dispose()

# Node to create the graph in Neo4j
create_graph_node = PythonOperator(
    task_id="create_graph_from_postgres",
    dag=production_dag,
    python_callable=_create_graph_from_postgres,
    op_kwargs={
        "pg_user": POSTGRES_USER,
        "pg_pwd": POSTGRES_PSWD,
        "pg_host": POSTGRES_HOST,
        "pg_port": POSTGRES_PORT,
        "pg_db": POSTGRES_DB,
        "neo_host": NEO4J_HOST,
        "neo_port": NEO4J_PORT,
    },
)

# End node
end_node = EmptyOperator(
    task_id="end_task", dag=production_dag, trigger_rule="all_success"
)

# Set the task sequence
start_node >> create_graph_node >> end_node
