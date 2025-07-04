import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) #Pour la modification du chemin de recherche de module pour trouver les modules situe dans d'autre repertoire 
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract_current_weather import extract_meteo
from scripts.merge import merge_files
from scripts.transform import transform_to_star

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
}

CITIES = ['Paris', 'London', 'Tokyo', 'New York', 'Sydney']

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',  # Exécution quotidienne
    catchup=False,              # Ne pas rattraper les exécutions passées
    max_active_runs=1,          # Pour éviter les conflits
) as dag:
    
    #extraction des donnees de meteo des villes
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[city, os.environ.get('WEATHER_API_KEY'), "{{ ds }}"]
        )
        for city in CITIES
    ]
    
    #fusion des donnees des villes recue separement
    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        op_args=["{{ ds }}"],
    )
    
    #transformation des donnees
    transform_task = PythonOperator(
        task_id='transform_to_star',
        python_callable=transform_to_star
    )
    
extract_tasks >> merge_task >> transform_task
