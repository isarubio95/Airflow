from airflow.sdk import dag, task, get_current_context, TaskGroup
from airflow.models.variable import Variable
from datetime import datetime, timedelta
import os
import io
import requests
import pandas as pd
import base64
import json

CITIES_JSON = Variable.get("weather_cities", default_var='{}')
CITIES = json.loads(CITIES_JSON)
OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY = [
    "temperature_2m", "apparent_temperature", "relative_humidity_2m",
    "precipitation", "rain", "snowfall", "cloud_cover",
    "windspeed_10m", "winddirection_10m", "surface_pressure"
]
default_args = { "retries": 10, "retry_delay": timedelta(seconds=15) }

@dag(
    dag_id="weather",
    start_date=datetime(2025, 1, 1),
    schedule="0 */4 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["weather", "medallion", "bronze", "silver", "gold"],
)
def weather_forecast_full_pipeline():
    @task
    def extract_open_meteo(city_params: dict):
        print(f"Extrayendo datos para las coordenadas: {city_params}")
        params = { "latitude": city_params["lat"], "longitude": city_params["lon"], "hourly": ",".join(HOURLY), "timezone": "Europe/Madrid", "forecast_days": 7 }
        r = requests.get(OPENMETEO_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    @task
    def transform_to_bronze_parquet(payload: dict, city_name: str) -> str:
        hourly = payload.get("hourly", {})
        df = pd.DataFrame(hourly)
        df["time"] = pd.to_datetime(df["time"])
        df["city"] = city_name
        df["latitude"] = payload.get("latitude")
        df["longitude"] = payload.get("longitude")
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        parquet_bytes = buf.getvalue()
        return base64.b64encode(parquet_bytes).decode('utf-8')
    
    @task
    def validate_bronze_data(encoded_data: str) -> str:
        """Valida los datos extraídos antes de cargarlos a Bronze."""
        print("Validando la calidad de los datos extraídos...")
        data_bytes = base64.b64decode(encoded_data)
        df = pd.read_parquet(io.BytesIO(data_bytes))
        if df.empty:
            raise ValueError("El DataFrame está vacío. No se recibieron datos de la API.")
        temp_col = "temperature_2m"
        if not df[temp_col].between(-90, 60).all():
            raise ValueError(f"Se encontraron temperaturas fuera del rango lógico (-90 a 60°C).")
        required_cols = ["time", "city"]
        if df[required_cols].isnull().values.any():
            raise ValueError(f"Se encontraron valores nulos en columnas críticas: {required_cols}.")
        print("Validación de datos superada con éxito.")
        return encoded_data

    @task
    def load_to_bronze(encoded_data: str, city_name: str) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        data_bytes = base64.b64decode(encoded_data)
        ctx = get_current_context()
        logical_date = ctx["data_interval_start"].to_date_string()
        bucket_name = os.getenv("S3_BUCKET", "weather")
        hook = S3Hook(aws_conn_id='minio')
        daily_prefix = f"bronze/weather/city={city_name}/date={logical_date}/"
        keys_to_delete = hook.list_keys(bucket_name=bucket_name, prefix=daily_prefix)
        if keys_to_delete:
            print(f"Limpiando {len(keys_to_delete)} archivo(s) antiguo(s) de Bronze en: {daily_prefix}")
            hook.delete_objects(bucket=bucket_name, keys=keys_to_delete)
        key = f"{daily_prefix}{ctx['run_id']}.parquet"
        hook.load_bytes(data_bytes, key, bucket_name=bucket_name, replace=True)
        s3_uri = f"s3://{bucket_name}/{key}"
        print(f"Subido a Bronze: {s3_uri}")
        return s3_uri

    @task
    def transform_and_load_to_silver(bronze_s3_uri: str, city_name: str) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id='minio')
        bucket_name = os.getenv("S3_BUCKET", "weather")
        bronze_key = bronze_s3_uri.replace(f"s3://{bucket_name}/", "")
        s3_object = hook.get_key(key=bronze_key, bucket_name=bucket_name)
        file_bytes = s3_object.get()['Body'].read()
        bronze_df = pd.read_parquet(io.BytesIO(file_bytes))
        silver_df = bronze_df.copy()
        silver_df.rename(columns={ "temperature_2m": "temperature_celsius", "relative_humidity_2m": "humidity_percent", "windspeed_10m": "wind_speed_kmh" }, inplace=True)
        temp = silver_df["temperature_celsius"]
        wind = silver_df["wind_speed_kmh"]
        silver_df["wind_chill"] = 13.12 + 0.6215 * temp - 11.37 * (wind**0.16) + 0.3965 * temp * (wind**0.16)
        silver_df = silver_df[[ "time", "city", "temperature_celsius", "apparent_temperature", "wind_chill", "humidity_percent", "wind_speed_kmh", "precipitation" ]]
        ctx = get_current_context()
        logical_date = ctx["data_interval_start"].to_date_string()
        silver_daily_prefix = f"silver/weather/city={city_name}/date={logical_date}/"
        silver_keys_to_delete = hook.list_keys(bucket_name=bucket_name, prefix=silver_daily_prefix)
        if silver_keys_to_delete:
            print(f"Limpiando {len(silver_keys_to_delete)} archivo(s) antiguo(s) de Silver en: {silver_daily_prefix}")
            hook.delete_objects(bucket=bucket_name, keys=silver_keys_to_delete)
        silver_key = f"{silver_daily_prefix}{ctx['run_id']}.parquet"
        buf = io.BytesIO()
        silver_df.to_parquet(buf, index=False)
        hook.load_bytes(buf.getvalue(), silver_key, bucket_name=bucket_name, replace=True)
        silver_s3_uri = f"s3://{bucket_name}/{silver_key}"
        print(f"Subido a Silver: {silver_s3_uri}")
        return silver_s3_uri

    @task
    def create_gold_daily_summary(silver_s3_uri: str, city_name: str) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id='minio')
        bucket_name = os.getenv("S3_BUCKET", "airflow-dags")
        silver_key = silver_s3_uri.replace(f"s3://{bucket_name}/", "")
        s3_object = hook.get_key(key=silver_key, bucket_name=bucket_name)
        file_bytes = s3_object.get()['Body'].read()
        silver_df = pd.read_parquet(io.BytesIO(file_bytes))
        silver_df.set_index('time', inplace=True)
        daily_summary = silver_df.resample('D').agg(
            temp_max=('temperature_celsius', 'max'),
            temp_min=('temperature_celsius', 'min'),
            precipitation_total=('precipitation', 'sum'),
            wind_speed_avg=('wind_speed_kmh', 'mean'),
            humidity_avg=('humidity_percent', 'mean')
        )
        daily_summary.reset_index(inplace=True)
        daily_summary["city"] = city_name
        ctx = get_current_context()
        logical_date = ctx["data_interval_start"].to_date_string()
        gold_key = f"gold/daily_summary/city={city_name}/date={logical_date}.parquet"
        buf = io.BytesIO()
        daily_summary.to_parquet(buf, index=False)
        hook.load_bytes(buf.getvalue(), gold_key, bucket_name=bucket_name, replace=True)
        gold_s3_uri = f"s3://{bucket_name}/{gold_key}"
        print(f"Subido a Gold: {gold_s3_uri}")
        return gold_s3_uri

    for city_name, city_params in CITIES.items():
        with TaskGroup(group_id=f"pipeline_for_{city_name}") as city_pipeline:
            extracted_payload = extract_open_meteo(
                city_params=city_params
            )
            
            bronze_parquet_encoded = transform_to_bronze_parquet(
                payload=extracted_payload, 
                city_name=city_name
            )

            validated_data = validate_bronze_data(
                encoded_data=bronze_parquet_encoded
            )
            
            bronze_uri = load_to_bronze(
                encoded_data=bronze_parquet_encoded, 
                city_name=city_name
            )
            
            silver_uri = transform_and_load_to_silver(
                bronze_s3_uri=bronze_uri, 
                city_name=city_name
            )
            
            create_gold_daily_summary(
                silver_s3_uri=silver_uri, 
                city_name=city_name
            )

weather_forecast_full_pipeline()