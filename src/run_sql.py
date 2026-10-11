import logging
import os
from pathlib import Path

import psycopg2

log = logging.getLogger(__name__)


def run_sql_file(path: str | Path) -> None:
    path = Path(path)
    sql = path.read_text(encoding="utf-8")
    dsn = os.environ["DB_URL"].replace("postgresql+psycopg2://", "postgresql://")

    conn = psycopg2.connect(dsn)
    try:
        with conn:  # una sola transaccion: COMMIT si todo sale bien, ROLLBACK si falla
            with conn.cursor() as cur:
                log.info("Ejecutando %s", path.name)
                cur.execute(sql)
        for notice in conn.notices:
            log.info("%s", notice.strip())
    finally:
        conn.close()
    log.info("OK: %s", path.name)