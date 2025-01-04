import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import json
import glob
import pandas as pd
import numpy as np

default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

movies_dag = DAG(
    dag_id='movies_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)



# TRANSFORM



# Remove unused columns

def _remove_unused_columns():
    csv_brut = pd.read_csv('/opt/airflow/data/movie-rating.csv', sep=';')
    unused_columns = ['homepage', 'id', 'original_language', 'overview', 'popularity', 'production_countries', 'spoken_languages', 'status', 'tagline', 'title', 'vote_count']
    new_csv = csv_brut.drop(unused_columns, axis=1)

    new_csv.to_csv('/opt/airflow/data/temp.csv', index=False)

    
first_node = PythonOperator(
    task_id='unused_columns',
    dag=movies_dag,
    trigger_rule='none_failed',
    python_callable=_remove_unused_columns,
    op_kwargs={

    },
    depends_on_past=False,
)

# Remove rows with data shift

def _remove_data_shift():
    csv_brut = pd.read_csv('/opt/airflow/data/movie-rating-cleaned.csv')

    # Convertir la colonne 'budget' en numérique, en forçant les erreurs à NaN
    csv_brut['budget'] = pd.to_numeric(csv_brut['budget'], errors='coerce')

    # Supprimer les lignes où 'budget' est NaN (celles qui ne sont pas un nombre)
    new_csv = csv_brut.dropna(subset=['budget'])

    # Optionnel : réinitialiser l'index après suppression
    new_csv = new_csv.reset_index(drop=True)

    new_csv.to_csv('/opt/airflow/data/movie-rating-cleaned.csv', index=False)
    
second_node = PythonOperator(
    task_id='data_shift',
    dag=movies_dag,
    trigger_rule='all_success',
    python_callable=_remove_data_shift,
    op_kwargs={

    },
    depends_on_past=True,
)


# Remove empty columns

def _remove_empty_columns():
    csv_brut = pd.read_csv('/opt/airflow/data/movie-rating-cleaned.csv')
    new_csv = csv_brut.dropna(axis=1, how='all')
    new_csv.to_csv('/opt/airflow/data/movie-rating-cleaned.csv', index=False)

    
third_node = PythonOperator(
    task_id='empty_columns',
    dag=movies_dag,
    trigger_rule='all_success',
    python_callable=_remove_empty_columns,
    op_kwargs={

    },
    depends_on_past=True,
)

# Remove rows with vote_average == null

def _remove_null_vote_average():
    df = pd.read_csv('/opt/airflow/data/movie-rating-cleaned.csv')

    new_df = df.dropna(subset=['vote_average'])

    new_df.to_csv('/opt/airflow/data/movie-rating-cleaned.csv', index=False)

fourth_node = PythonOperator(
    task_id='null_vote_average',
    dag=movies_dag,
    trigger_rule='all_success',
    python_callable=_remove_null_vote_average,
    op_kwargs={

    },
    depends_on_past=True,
)    




first_node >> second_node >> third_node >> fourth_node
