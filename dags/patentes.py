from airflow.sdk import dag, task
from datetime import datetime, timedelta
from validadores_patentes import validate_patent_batch
import os
import json

DAG_FOLDER = os.path.dirname(__file__)
PATENTES_FILE_PATH = os.path.join(DAG_FOLDER, "patente_invenes.json")

try:
    with open(PATENTES_FILE_PATH, 'r', encoding='utf-8') as f:
        PATENTES = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    print(f"ADVERTENCIA: No se pudo cargar o encontrar el archivo '{PATENTES_FILE_PATH}'.")
    PATENTES = [] 

print(f"CARGADAS {len(PATENTES)} PATENTES DESDE '{PATENTES_FILE_PATH}'")

BATCH_SIZE = 500
BUCKET = "patentes"
default_args = {"retries": 1, "retry_delay": timedelta(seconds=15)}

@dag(
    dag_id="patent_processing_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["patents", "medallion", "bronze", "silver"],
)
def patent_processing_dag():
    """
    Extraer una lista de patentes de un archivo JSON, las divide en lotes,
    valida cada lote en paralelo y luego carga las patentes válidas.
    """
    @task
    def create_batches() -> list[list[dict]]:
        """
        Paso 1: Divide la lista completa de patentes en lotes más pequeños.
        En lugar de 100,000 tareas, crearemos 200 tareas (si BATCH_SIZE es 500).
        """
        if not PATENTES:
            raise ValueError("El archivo de patentes está vacío o no se pudo cargar. No hay datos para procesar.")
        
        batches = [PATENTES[i:i + BATCH_SIZE] for i in range(0, len(PATENTES), BATCH_SIZE)]
        print(f"Se crearon {len(batches)} lotes de aproximadamente {BATCH_SIZE} patentes cada uno.")
        return batches

    @task
    def validate_batch(batch: list[dict]) -> list[dict]:
        """
        Paso 2: Recibe y procesa UN LOTE de patentes.
        Valida cada patente dentro del lote y devuelve solo las que son válidas.
        """
        return validate_patent_batch(batch)

    @task
    def load_all_valid_patents(validated_batches: list):
        """
        Paso 3: Recibe una lista de lotes (que contienen patentes válidas),
        los une en una sola lista y los carga en el destino final.
        """
        # Aplanamos la lista de listas en una sola gran lista de patentes válidas
        final_valid_patents = [patent for batch in validated_batches if batch for patent in batch]
        
        print(f"Carga total: {len(final_valid_patents)} patentes validadas al destino final (ej: S3, base de datos)...")
        if not final_valid_patents:
            print("No hubo patentes válidas para cargar.")
            return
        pass

    patent_batches = create_batches()
    validated_results = validate_batch.expand(batch=patent_batches)
    load_all_valid_patents(validated_results)

patent_processing_dag()