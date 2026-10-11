import csv
import logging
import os
import time
from pathlib import Path

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BUCKET = os.environ["S3_BUCKET"]
PROFILE = os.getenv("AWS_PROFILE", "de-journey")
SRC_DIR = Path(os.getenv("OLIST_DIR", "data/olist"))
DATABASE = "olist_raw"
RAW_PREFIX = "raw/olist"
RESULTS = f"s3://{BUCKET}/athena-results/"


def folder_name(path: Path) -> str:
    """Carpeta en S3 (igual que en upload_olist_to_s3.py)."""
    return path.stem.removesuffix("_dataset")


def athena_table_name(path: Path) -> str:
    return folder_name(path).removeprefix("olist_")


def read_columns(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f))


def build_ddl(path: Path) -> str:
    cols = ",\n  ".join(f"`{c}` string" for c in read_columns(path))
    location = f"s3://{BUCKET}/{RAW_PREFIX}/{folder_name(path)}/"
    return f"""CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{athena_table_name(path)} (
  {cols}
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION '{location}'
TBLPROPERTIES ('skip.header.line.count' = '1')"""


def run_query(athena, sql: str) -> str:
    qid = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": RESULTS},
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena {status['State']}: {status.get('StateChangeReason')}")
    return qid


def main() -> None:
    athena = boto3.Session(profile_name=PROFILE).client("athena")

    run_query(athena, f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    log.info("Base %s lista", DATABASE)

    files = sorted(SRC_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No hay CSV en {SRC_DIR}")
    for path in files:
        run_query(athena, build_ddl(path))
        log.info("Tabla %s.%s creada", DATABASE, athena_table_name(path))

    # Prueba: contar filas de orders (esperado: 99441)
    qid = run_query(athena, f"SELECT COUNT(*) AS filas FROM {DATABASE}.orders")
    result = athena.get_query_results(QueryExecutionId=qid)
    rows = result.get("ResultSet", {}).get("Rows", [])
    log.info("orders -> %s filas", rows[1].get("Data", [{}])[0].get("VarCharValue"))


if __name__ == "__main__":
    main()