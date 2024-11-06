import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests
import random
import json
import glob
import pandas as pd
from faker import Faker

default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


fake = Faker()
output_folder = "/opt/airflow/dags"


# Generate randomly name, level

def _names_levels(output_folder: str):
    random_names = [fake.first_name() for i in range(5)]
    random_levels = [random.randint(1, 3) for i in range(5)]
    with open(f'{output_folder}/names_levels.json', 'w') as f:
        json.dump({"levels": random_levels, "names": random_names}, f, ensure_ascii=False)

first_node = PythonOperator(
    task_id='names_levels',
    dag=dnd_dag,
    trigger_rule='none_failed',
    python_callable=_names_levels,
    op_kwargs={
        "output_folder": output_folder,
    },
    depends_on_past=False,
)

# Generate randomly attributes

def _attributes(output_folder: str):
    attributes = []
    for j in range(5):
        l = list()
        l.append(random.randint(6,18)) #strength
        for i in range(5):
            l.append(random.randint(2,18))
        attributes.append(l)
    final_dict = {"attributes": attributes}
    with open(f'{output_folder}/attributes.json', 'w') as f:
        json.dump(final_dict, f, ensure_ascii=False)

second_node = PythonOperator(
    task_id='attributes',
    dag=dnd_dag,
    trigger_rule='none_failed',
    python_callable=_attributes,
    op_kwargs={
        "output_folder": output_folder,
    },
    depends_on_past=False,
)


# Choose race

def _races(output_folder: str):
    races_unfiltered = requests.get("https://www.dnd5eapi.co/api/races").json()
    races = random.sample([i.get('index') for i in races_unfiltered.get('results')], 5)
    final_dict = {"races": races}
    with open(f'{output_folder}/races.json', 'w') as f:
        json.dump(final_dict, f, ensure_ascii=False)

third_node = PythonOperator(
    task_id = 'races',
    dag=dnd_dag,
    trigger_rule='none_failed',
    python_callable=_races,
    op_kwargs={
        "output_folder": output_folder,
    },
    depends_on_past=False,
)

# Choose language

def _languages(output_folder: str):
    languages_unfiltered = requests.get("https://www.dnd5eapi.co/api/languages").json()
    languages = random.sample([i.get('index') for i in languages_unfiltered.get('results')],5)
    final_dict = {"languages": languages}
    with open(f'{output_folder}/languages.json', 'w') as f:
        json.dump(final_dict, f, ensure_ascii=False)

fourth_node = PythonOperator(
    task_id='languages',
    dag=dnd_dag,
    trigger_rule='none_failed',
    python_callable=_languages,
    op_kwargs={
        "output_folder": output_folder,
    },
    depends_on_past=False,
)


# Choose class

def _classes(output_folder: str):
    classes_unfiltered = requests.get("https://www.dnd5eapi.co/api/classes").json()
    classes = random.sample([i.get('index') for i in classes_unfiltered.get('results')], 5)
    final_dict = {"classes": classes}
    with open(f'{output_folder}/classes.json', 'w') as f:
        json.dump(final_dict, f, ensure_ascii=False)


fifth_node = PythonOperator(
    task_id='classes',
    dag=dnd_dag,
    trigger_rule='none_failed',
    python_callable=_classes,
    op_kwargs={
        "output_folder": output_folder,
    },
    depends_on_past=False,
)
    

# Choose proficiencies (=compétences)

def _proficiency_choices(output_folder: str):
    #Open classes
    with open(f"{output_folder}/classes.json", "r") as read_file:
        load_classes = json.load(read_file)

    classes = load_classes.get('classes')

    # Get proficiences for each class
    final = []
    for classs in classes:
        proficiences_unfiltered = requests.get(f"https://www.dnd5eapi.co/api/classes/{classs}").json()
        proficiency_choices = proficiences_unfiltered.get('proficiency_choices')[0]
        proficiency_options = proficiency_choices.get('from').get('options')
        choices = proficiency_choices.get('choose')
        proficiences = random.sample([i.get('item').get('index') for i in proficiency_options], choices)
        final.append(proficiences)

    final_dict = {"proficiences": final}
    with open(f'{output_folder}/proficiences.json', 'w') as f:
        json.dump(final_dict, f, ensure_ascii=False)


sixth_node = PythonOperator(
    task_id='proficiency_choices',
    dag=dnd_dag,
    trigger_rule='none_failed',
    python_callable=_proficiency_choices,
    op_kwargs={
        "output_folder": output_folder,
    },
    depends_on_past=False,
)

# Check if there are spells for classes or not

def _spell_check(output_folder: str):
    with open(f"{output_folder}/classes.json", "r") as read_file:
        load_classes = json.load(read_file)

    classes = load_classes.get('classes')

    spell_counts = []
    for classs in classes:
        spellcount = requests.get(f"https://www.dnd5eapi.co/api/classes/{classs}/spells").json().get('count')
        spell_counts.append(spellcount)

    if sum(spell_counts) > 0:
        return 'spells' #go to the task spells
    else:
        return 'merge' #go to the task merge


seventh_node = BranchPythonOperator(   #A workflow can "branch" or follow a path after the execution of this task.
    task_id='spell_check',
    dag=dnd_dag,
    python_callable=_spell_check,
    op_kwargs={
        "output_folder": output_folder,
    },
    trigger_rule='all_success',
)


# Choose spells

def _spells(output_folder):
    with open(f"{output_folder}/classes.json", "r") as read_file:
        load_classes = json.load(read_file)

    classes = load_classes.get('classes')

    spell_lists = []
    for classs in classes:
        spells_unfiltered = requests.get(f"https://www.dnd5eapi.co/api//classes/{classs}/spells").json()
        spell_count = spells_unfiltered.get('count')

        if spell_count > 0:
            spells_filtered = spells_unfiltered.get('results')
            spells = random.sample([i.get('index') for i in spells_filtered], random.randint(1, 3))
            spell_lists.append(spells)
        else:
            spells = "none"
            spell_lists.append(spells)

            
    with open(f'{output_folder}/spells.json', 'w') as f:
        json.dump({"spells": spell_lists}, f, ensure_ascii=False)


eighth_node = PythonOperator(
    task_id='spells',
    dag=dnd_dag,
    python_callable=_spells,
    op_kwargs={
        "output_folder": output_folder,
    },
    trigger_rule='all_success',
)


# Merge the json results

def _merge(output_folder):
    jsons = glob.glob(f'{output_folder}/*.json')
    dfs = pd.concat(map(pd.read_json, jsons), axis=1)
    dfs.to_csv(path_or_buf=f'{output_folder}/final.csv')


ninth_node = PythonOperator(
    task_id='merge',
    dag=dnd_dag,
    python_callable=_merge,
    op_kwargs={
        "output_folder": output_folder
    },
    trigger_rule='all_success',
)


# Prepare the insert sql command

def _generate_insert(output_folder):
    df = pd.read_csv(f'{output_folder}/final.csv')
    with open("/opt/airflow/dags/inserts.sql", "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS characters (\n"
            "race VARCHAR(255),\n"
            "name VARCHAR(255),\n"
            "proficiences VARCHAR(255),\n"
            "language VARCHAR(255),\n"
            "spells VARCHAR(255),\n"
            "classs VARCHAR(255),\n"
            "levels VARCHAR(255),\n"
            "attributes VARCHAR(255));\n"
        )
        for index, row in df_iterable:
            race = row['races']
            name = row['names']
            proficiences = row['proficiences']
            language = row['languages']
            spells = row['spells']
            classs = row['classes']
            levels = row['levels']
            attributes = row['attributes']

            f.write(
                "INSERT INTO characters VALUES ("
                f"'{race}', '{name}', $${proficiences}$$, '{language}', $${spells}$$, '{classs}', '{levels}', $${attributes}$$"
                ");\n"
            )
            if index == 4: # ajout de juste 5 perso -> pour eviter les valeurs bizarres
                break
            

        f.close()


tenth_node = PythonOperator(
    task_id='generate_insert',
    dag=dnd_dag,
    python_callable=_generate_insert,
    op_kwargs={
        "output_folder": output_folder
    },
    trigger_rule='none_failed',
)

# Execute the insert sql command

eleventh_node = PostgresOperator(
    task_id='insert_inserts',
    dag=dnd_dag,
    postgres_conn_id='postgres_default',
    sql='inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

# end node

twelfth_node = DummyOperator(
    task_id='end',
    dag=dnd_dag,
    trigger_rule='none_failed'
)



[first_node, second_node, third_node, fourth_node, fifth_node] >> sixth_node
sixth_node >> seventh_node >> [eighth_node, ninth_node]
eighth_node >> ninth_node >> tenth_node >> eleventh_node >> twelfth_node
