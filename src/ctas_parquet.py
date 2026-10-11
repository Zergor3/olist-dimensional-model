import logging
import os
import time

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BUCKET = os.environ["S3_BUCKET"]
PROFILE = os.getenv("AWS_PROFILE", "de-journey")
RAW_DB = "olist_raw"
CURATED_DB = "olist_curated"
CURATED_PREFIX = "curated/olist"
RESULTS = f"s3://{BUCKET}/athena-results/"

session = boto3.Session(profile_name=PROFILE)
athena = session.client("athena")
s3 = session.resource("s3")


def run_query(sql: str) -> tuple[str, int]:
    """Ejecuta una consulta y devuelve (id, bytes escaneados)."""
    qid = athena.start_query_execution(
        QueryString=sql, ResultConfiguration={"OutputLocation": RESULTS}
    )["QueryExecutionId"]
    while True:
        qe = athena.get_query_execution(QueryExecutionId=qid).get("QueryExecution", {})
        status = qe.get("Status", {})
        state = status.get("State")
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena {state}: {status.get('StateChangeReason')}")
    return qid, qe.get("Statistics", {}).get("DataScannedInBytes", 0)


def first_row(qid: str) -> list[str | None]:
    result = athena.get_query_results(QueryExecutionId=qid)
    rows = result.get("ResultSet", {}).get("Rows", [])
    return [c.get("VarCharValue") for c in rows[1].get("Data", [])]


def recreate(table: str, ctas_sql: str) -> None:
    run_query(f"DROP TABLE IF EXISTS {CURATED_DB}.{table}")
    s3.Bucket(BUCKET).objects.filter(Prefix=f"{CURATED_PREFIX}/{table}/").delete()
    run_query(ctas_sql)
    log.info("Tabla %s.%s creada", CURATED_DB, table)


def ts(col: str) -> str:
    return f"TRY_CAST(NULLIF({col}, '') AS timestamp)"


def main() -> None:
    run_query(f"CREATE DATABASE IF NOT EXISTS {CURATED_DB}")

    recreate("order_items", f"""
        CREATE TABLE {CURATED_DB}.order_items
        WITH (format = 'PARQUET', parquet_compression = 'SNAPPY',
              external_location = 's3://{BUCKET}/{CURATED_PREFIX}/order_items/')
        AS SELECT
            order_id,
            TRY_CAST(order_item_id AS integer)       AS order_item_id,
            product_id,
            seller_id,
            {ts('shipping_limit_date')}              AS shipping_limit_date,
            TRY_CAST(price AS decimal(10,2))         AS price,
            TRY_CAST(freight_value AS decimal(10,2)) AS freight_value
        FROM {RAW_DB}.order_items""")

    recreate("orders", f"""
        CREATE TABLE {CURATED_DB}.orders
        WITH (format = 'PARQUET', parquet_compression = 'SNAPPY',
              partitioned_by = ARRAY['order_month'],
              external_location = 's3://{BUCKET}/{CURATED_PREFIX}/orders/')
        AS SELECT
            order_id,
            customer_id,
            order_status,
            {ts('order_purchase_timestamp')}         AS order_purchase_timestamp,
            {ts('order_approved_at')}                AS order_approved_at,
            {ts('order_delivered_carrier_date')}     AS order_delivered_carrier_date,
            {ts('order_delivered_customer_date')}    AS order_delivered_customer_date,
            {ts('order_estimated_delivery_date')}    AS order_estimated_delivery_date,
            date_format({ts('order_purchase_timestamp')}, '%Y-%m') AS order_month
        FROM {RAW_DB}.orders""")

    print("\n=== Comparacion CSV vs Parquet ===")

    # 1) Una sola columna: Parquet lee solo 'price'; el CSV debe leer el archivo completo
    q_csv, b_csv = run_query(
        f"SELECT COUNT(*), SUM(TRY_CAST(price AS decimal(10,2))) FROM {RAW_DB}.order_items")
    q_pq, b_pq = run_query(f"SELECT COUNT(*), SUM(price) FROM {CURATED_DB}.order_items")
    print(f"order_items  CSV     : {first_row(q_csv)}  escaneo={b_csv:>12,} bytes")
    print(f"order_items  Parquet : {first_row(q_pq)}  escaneo={b_pq:>12,} bytes")

    # 2) Filtro por mes: el CSV lee todo; Parquet solo la particion 2017-11
    q_csv, b_csv = run_query(
        f"SELECT COUNT(*) FROM {RAW_DB}.orders WHERE substr(order_purchase_timestamp, 1, 7) = '2017-11'")
    q_pq, b_pq = run_query(
        f"SELECT COUNT(*) FROM {CURATED_DB}.orders WHERE order_month = '2017-11'")
    print(f"orders 2017-11 CSV     : {first_row(q_csv)}  escaneo={b_csv:>12,} bytes")
    print(f"orders 2017-11 Parquet : {first_row(q_pq)}  escaneo={b_pq:>12,} bytes")
    print("  (COUNT(*) en Parquet se resuelve con metadatos: por eso 0 bytes)")

    # 3) Igual, pero leyendo una columna: el escaneo de Parquet deja de ser cero
    q_csv, b_csv = run_query(
        f"SELECT COUNT(DISTINCT customer_id) FROM {RAW_DB}.orders "
        f"WHERE substr(order_purchase_timestamp, 1, 7) = '2017-11'")
    q_pq, b_pq = run_query(
        f"SELECT COUNT(DISTINCT customer_id) FROM {CURATED_DB}.orders WHERE order_month = '2017-11'")
    print(f"orders 2017-11 (customer_id) CSV     : {first_row(q_csv)}  escaneo={b_csv:>12,} bytes")
    print(f"orders 2017-11 (customer_id) Parquet : {first_row(q_pq)}  escaneo={b_pq:>12,} bytes")


if __name__ == "__main__":
    main()