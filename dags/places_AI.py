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
import requests
import re
from dotenv import load_dotenv
import os

default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

places_dag = DAG(
    dag_id='places_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

# Définition de l'URL et des en-têtes
url = "https://api.x.ai/v1/chat/completions"
# Load environment variables from .env file
load_dotenv('/opt/airflow/.env')

# Get the API key from the environment variable
api_key = os.getenv('API_KEY')

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}


# TRANSFORM



# Remove unused columns

def ask_ai():

    with open('/opt/airflow/data/movie-cities.json', 'r') as file:
        data = json.load(file)

    movies = data.get('movies', [])
    for movie in movies:
        title = movie.get('name')
        cities = movie.get('locations', [])
        data = {
        "messages": [
            {"role": "system", "content": "You are a movie specialist."},
            {"role": "user", "content": f"Give me only the famous monuments and places appearing in {title} given that it takes place in those locations {', '.join(cities)}. The output format is just a list without any description. Each places and monuments should be between @."}
        ],
        "model": "grok-beta",
        "stream": False,
        "temperature": 0
        }
        print(f"Movie: {title}, Cities: {', '.join(cities)}")

        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            # Extract places from the response
            text = response.text
            places = re.findall(r'@([^@]+)@', text)  # Remove @ symbols during extraction
            new_locations = {place.strip() for place in places if place.strip()}  # Use a set for uniqueness

            # Load existing data
            try:
                with open('./data/movie-places.json', 'r') as file:
                    content = file.read().strip()  # Remove extra whitespace
                    if content:
                        data = json.loads(content)
                    else:
                        data = {"movies": []}  # Handle empty file
            except (FileNotFoundError, json.JSONDecodeError):
                data = {"movies": []}  # Initialize if file is missing or invalid

            movie['locations'] = sorted(cities)
            movie['places'] = sorted(new_locations)
            data['movies'].append(movie)

            # Save updated data back to the file
            with open('./data/movie-places.json', 'w') as file:
                json.dump(data, file, indent=4)

            print(f"Movie '{movie['name']}' updated successfully.")
        else:
            print(f"Error {response.status_code}: {response.text}") 

    
first_node = PythonOperator(
    task_id='movie_cities',
    dag=places_dag,
    trigger_rule='none_failed',
    python_callable=ask_ai,
    op_kwargs={

    },
    depends_on_past=False,
)


first_node
