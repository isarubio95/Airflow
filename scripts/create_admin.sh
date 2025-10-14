set -euo pipefail

echo ">>> Migrando metabase..."
airflow db migrate && airflow db check

echo ">>> Creando usuario admin si no existe..."
if ! airflow users list | awk '{print $2}' | grep -qx "${AIRFLOW_ADMIN_USER:-admin}"; then
  airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
    --role "${AIRFLOW_ADMIN_ROLE:-Admin}" \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}"
else
  echo "Usuario ya existe, saltando creación."
fi
