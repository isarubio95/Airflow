from airflow.sdk import dag, task, get_current_context, TaskGroup
from airflow.models.variable import Variable
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
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
    def transform_load_silver_and_index(bronze_s3_uri: str, city_name: str) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from urllib.parse import urlparse
        from airflow.sdk import get_current_context
        import pandas as pd
        import io, os

        s3 = S3Hook(aws_conn_id='minio')
        BUCKET = os.getenv("S3_BUCKET", "weather")

        u = urlparse(bronze_s3_uri)
        bronze_bucket = u.netloc
        bronze_key = u.path.lstrip('/')

        obj = s3.get_key(key=bronze_key, bucket_name=bronze_bucket)
        file_bytes = obj.get()['Body'].read()
        bronze_df = pd.read_parquet(io.BytesIO(file_bytes))

        # --- Transformación → Silver ---
        silver_df = bronze_df.copy()
        silver_df.rename(columns={
            "temperature_2m": "temperature_celsius",
            "relative_humidity_2m": "humidity_percent",
            "windspeed_10m": "wind_speed_kmh"
        }, inplace=True)
        temp = silver_df["temperature_celsius"]
        wind = silver_df["wind_speed_kmh"]
        silver_df["wind_chill"] = 13.12 + 0.6215*temp - 11.37*(wind**0.16) + 0.3965*temp*(wind**0.16)
        silver_df = silver_df[[
            "time","city","temperature_celsius","apparent_temperature",
            "wind_chill","humidity_percent","wind_speed_kmh","precipitation"
        ]]

        # --- Persistir Silver en S3 ---
        ctx = get_current_context()
        logical_date = ctx["data_interval_start"].to_date_string()
        silver_daily_prefix = f"silver/weather/city={city_name}/date={logical_date}/"
        old = s3.list_keys(bucket_name=BUCKET, prefix=silver_daily_prefix)
        if old:
            s3.delete_objects(bucket=BUCKET, keys=old)

        silver_key = f"{silver_daily_prefix}{ctx['run_id']}.parquet"
        buf = io.BytesIO()
        silver_df.to_parquet(buf, index=False)
        s3.load_bytes(buf.getvalue(), silver_key, bucket_name=BUCKET, replace=True)
        silver_s3_uri = f"s3://{BUCKET}/{silver_key}"
        print(f"[Silver] Subido: {silver_s3_uri}")

        # --- Cliente OpenSearch desde Connection de Airflow ---
        def _as_bool(v, default=False):
            if isinstance(v, bool):
                return v
            if v is None:
                return default
            return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

        conn = BaseHook.get_connection("opensearch_default")
        host = conn.host
        port = conn.port or 9200
        user = conn.login
        pwd  = conn.password
        extra = conn.extra_dejson or {}

        use_ssl = _as_bool(extra.get("use_ssl", True))
        verify_certs = _as_bool(extra.get("verify_certs", False))
        ca_certs = extra.get("ca_certs")  # ruta al CA si verify_certs=True
        scheme = "https" if use_ssl else "http"

        client = OpenSearch(
            hosts=[{"host": host, "port": port, "scheme": scheme}],
            http_auth=(user, pwd) if user else None,   # usa basic_auth si tu lib es opensearch-py >=2.5
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            ca_certs=ca_certs,
            ssl_assert_hostname=False if not verify_certs else True,
            ssl_show_warn=not verify_certs,
            timeout=30, max_retries=3, retry_on_timeout=True,
        )

        index_name = "weather_silver_hourly"

        if not client.indices.exists(index=index_name):
            client.indices.create(
                index=index_name,
                body={
                    "settings": {"number_of_shards": 1, "number_of_replicas": 1},
                    "mappings": {
                        "properties": {
                            "time": {"type": "date"},
                            "city": {"type": "keyword"},
                            "temperature_celsius": {"type": "float"},
                            "apparent_temperature": {"type": "float"},
                            "wind_chill": {"type": "float"},
                            "humidity_percent": {"type": "float"},
                            "wind_speed_kmh": {"type": "float"},
                            "precipitation": {"type": "float"},
                        }
                    },
                },
            )

        # --- Bulk idempotente ---
        actions = []
        for row in silver_df.itertuples(index=False):
            ts = pd.to_datetime(row.time).to_pydatetime()
            doc_id = f"{row.city}-{ts.isoformat()}"
            actions.append({
                "_index": index_name,
                "_id": doc_id,
                "_source": {
                    "time": ts,
                    "city": row.city,
                    "temperature_celsius": float(row.temperature_celsius) if row.temperature_celsius is not None else None,
                    "apparent_temperature": float(row.apparent_temperature) if row.apparent_temperature is not None else None,
                    "wind_chill": float(row.wind_chill) if row.wind_chill is not None else None,
                    "humidity_percent": float(row.humidity_percent) if row.humidity_percent is not None else None,
                    "wind_speed_kmh": float(row.wind_speed_kmh) if row.wind_speed_kmh is not None else None,
                    "precipitation": float(row.precipitation) if row.precipitation is not None else None,
                }
            })

        if actions:
            bulk(client, actions, raise_on_error=False)
            print(f"[OpenSearch] Indexed {len(actions)} docs into {index_name}")

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
                encoded_data=validated_data,
                city_name=city_name
            )
            
            silver_uri = transform_load_silver_and_index(
                bronze_s3_uri=bronze_uri, 
                city_name=city_name
            )
   
            create_gold_daily_summary(
                silver_s3_uri=silver_uri, 
                city_name=city_name
            )

weather_forecast_full_pipeline()