"""DAG de Olist en AWS: CSV -> S3 (raw) -> tablas externas Athena -> dbt build (Parquet) -> validacion."""
# pyright: reportMissingImports=false
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Cifras ya validadas contra el warehouse de Postgres.
ESPERADO = {
    "olist_dbt.fact_order_items": ("112650", "13591643.70"),
    "olist_dbt.fact_payments": ("103886", None),
}
ESPERADO_PAGOS = "16008872.12"

# --target-path y --log-path van a /tmp: evita escribir en la carpeta montada desde Windows.
DBT_BUILD = (
    "cd /opt/airflow/dbt/olist_athena && "
    "/opt/airflow/dbt_venv/bin/dbt build "
    "--profiles-dir . "
    "--target-path /tmp/dbt_target_athena "
    "--log-path /tmp/dbt_logs_athena"
)


def _validar() -> None:
    import boto3
    from create_athena_tables import PROFILE, run_query

    athena = boto3.Session(profile_name=PROFILE).client("athena")

    def consulta(sql: str) -> list[str]:
        qid = run_query(athena, sql)
        resultado = athena.get_query_results(QueryExecutionId=qid)
        filas = resultado["ResultSet"].get("Rows", [])
        if len(filas) < 2:  # la fila 0 es la cabecera
            raise ValueError(f"Athena no devolvio datos para: {sql}")
        return [c.get("VarCharValue", "") for c in filas[1].get("Data", [])]

    n, total = consulta(
        "SELECT CAST(COUNT(*) AS varchar), CAST(SUM(price) AS varchar) FROM olist_dbt.fact_order_items"
    )
    if (n, total) != ESPERADO["olist_dbt.fact_order_items"]:
        raise ValueError(f"fact_order_items: filas={n} price={total}")

    n, total = consulta(
        "SELECT CAST(COUNT(*) AS varchar), CAST(SUM(payment_value) AS varchar) FROM olist_dbt.fact_payments"
    )
    if (n, total) != (ESPERADO["olist_dbt.fact_payments"][0], ESPERADO_PAGOS):
        raise ValueError(f"fact_payments: filas={n} pagos={total}")


with DAG(
    dag_id="olist_aws_pipeline",
    description="Olist en AWS: CSV -> S3 -> Athena (raw) -> dbt-athena (Parquet) -> validacion",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["olist", "aws", "athena", "dbt", "mes3"],
) as dag:
    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command="python /opt/airflow/src/upload_olist_to_s3.py",
    )

    create_athena_tables = BashOperator(
        task_id="create_athena_tables",
        bash_command="python /opt/airflow/src/create_athena_tables.py",
    )

    # Sin reintentos: si un test falla, volver a ejecutar no cambia los datos.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT_BUILD,
        retries=0,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=_validar,
        retries=0,
    )

    upload_to_s3 >> create_athena_tables >> dbt_build >> validate