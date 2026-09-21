from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime

LATITUDE = "51.5074"
LONGITUDE = "-0.1278"
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data.get("current_weather", {})
        transformed_data = {
            "temperature": current_weather.get("temperature"),
            "windspeed": current_weather.get("windspeed"),
            "winddirection": current_weather.get("winddirection"),
            "weathercode": current_weather.get("weathercode"),
            "time": current_weather.get("time")
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Table banao
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_data (
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            time TIMESTAMP
        );
        """
        postgres_hook.run(create_table_query)

        insert_query = """
            INSERT INTO weather_data
            (temperature, windspeed, winddirection, weathercode, time)
            VALUES (%s, %s, %s, %s, %s)
        """
        postgres_hook.run(
            insert_query,
            parameters=(
                transformed_data["temperature"],
                transformed_data["windspeed"],
                transformed_data["winddirection"],
                transformed_data["weathercode"],
                transformed_data["time"],
            ),
        )

    # Flow
    weather_data = extract_weather_data()
    transformed = transform_weather_data(weather_data)
    load_weather_data(transformed)