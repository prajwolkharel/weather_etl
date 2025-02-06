from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Latitude and longitude for the desired location. eg, London
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

# default arguments for the DAG
default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

# DAG
with DAG(dag_id='weather_etl',
         default_args=default_args,
         Schedule_interval='@daily',
         catchup=False) as dags:

        @task()
        def extract_weather_data():
            """Extract weather data from Open-Meteo API using Airflow Connection."""

            # HTTP Hook to get connection details from Airflow connection
            http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')

            # Build the API enpoint
            endpoint = f'v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

            # Make request via httphook
            response = http_hook(endpoint)









