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

            if response.status_code == 200:
                  return response.json()
            else:
                  raise Exception("Failed to fetch weather dataL {response.status_code}")

        @task
        def transform_weather_data(weather_data):
            """Transform the extracted weather data."""
            current_weather = weather_data['current_weather']
            transformed_data = {
                  'latitude': LATITUDE,
                  'longitude': LONGITUDE,
                  'temperature': current_weather['temperature'],
                  'windspeed': current_weather['windspeed'],
                  'winddirection': current_weather['winddirection'],
                  'weathercode': current_weather['weathercode']
            }
            return transformed_data
<<<<<<< HEAD

        @task
        def load_weather_data(transformed_data):
            """Load transformed data into PostgreSQL"""
            pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            # create table if it does not exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # insert transformed data into table
            cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, windirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                transformed_data['latitude'],
                transformed_data['longitude'],
                transformed_data['temperature'],
                transformed_data['windspeed'],
                transformed_data['winddirection'],
                transformed_data['weathercode'],
            ))

            conn.commit()
            cursor.close()

        # DAG Workflow -  ETL Pipeline
        weather_data = extract_weather_data()
        transformed_data = transform_weather_data(weather_data)
        load_weather_data(transformed_data)
=======
>>>>>>> 705557af7909b982ffd37217fa35b71f6ff3094f
