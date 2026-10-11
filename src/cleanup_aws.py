"""Limpia los recursos de AWS del proyecto: bases de Athena/Glue y objetos del bucket de S3."""
import argparse
import logging
import os

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BUCKET = os.environ["S3_BUCKET"]
PROFILE = os.getenv("AWS_PROFILE", "de-journey")
DATABASES = ["olist_raw", "olist_curated", "olist_dbt"]
# athena-results va al final: Athena sigue escribiendo ahi mientras se ejecutan consultas.
PREFIXES = ["raw/", "curated/", "dbt/", "athena-results/"]


def list_tables(glue, database: str) -> list[str] | None:
    """Nombres de las tablas de una base de Glue, o None si la base no existe."""
    try:
        names: list[str] = []
        for page in glue.get_paginator("get_tables").paginate(DatabaseName=database):
            names += [t.get("Name", "?") for t in page.get("TableList", [])]
        return names
    except glue.exceptions.EntityNotFoundException:
        return None


def list_keys(s3, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if "Key" in o]
    return keys


def delete_keys(s3, keys: list[str]) -> None:
    for i in range(0, len(keys), 1000):  # el maximo por llamada es 1000
        batch = [{"Key": k} for k in keys[i : i + 1000]]
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--execute", action="store_true", help="borrar de verdad (por defecto: simulacion)")
    parser.add_argument("--delete-bucket", action="store_true", help="borrar tambien el bucket (queda vacio)")
    args = parser.parse_args()

    session = boto3.Session(profile_name=PROFILE)
    glue = session.client("glue")
    s3 = session.client("s3")

    # 1) Inventario
    plan_dbs: list[str] = []
    for db in DATABASES:
        tables = list_tables(glue, db)
        if tables is None:
            log.info("Base %s: no existe", db)
        else:
            log.info("Base %s: %d tablas %s", db, len(tables), tables)
            plan_dbs.append(db)

    plan_keys: dict[str, list[str]] = {}
    for prefix in PREFIXES:
        keys = list_keys(s3, prefix)
        plan_keys[prefix] = keys
        log.info("s3://%s/%s: %d objetos", BUCKET, prefix, len(keys))

    if not args.execute:
        log.info("SIMULACION: no se borro nada. Usa --execute para borrar.")
        return

    # 2) Confirmacion explicita
    answer = input(f"Se borraran las bases y los objetos de s3://{BUCKET}. Escribe el nombre del bucket para confirmar: ")
    if answer.strip() != BUCKET:
        log.info("Cancelado.")
        return

    # 3) Borrado: primero el catalogo, luego los datos
    for db in plan_dbs:
        glue.delete_database(Name=db)  # borra tambien las tablas de la base
        log.info("Base %s borrada", db)

    for prefix in PREFIXES:
        # se vuelve a listar: Athena pudo escribir resultados durante el proceso
        keys = list_keys(s3, prefix)
        delete_keys(s3, keys)
        log.info("s3://%s/%s: %d objetos borrados", BUCKET, prefix, len(keys))

    if args.delete_bucket:
        s3.delete_bucket(Bucket=BUCKET)
        log.info("Bucket %s borrado", BUCKET)


if __name__ == "__main__":
    main()