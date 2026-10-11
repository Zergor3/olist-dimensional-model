import logging
import os
from pathlib import Path

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SRC_DIR = Path(os.getenv("OLIST_DIR", "data/olist"))
BUCKET = os.environ["S3_BUCKET"]          # obligatorio: falla claro si falta
PREFIX = "raw/olist"
PROFILE = os.getenv("AWS_PROFILE", "de-journey")


def table_name(path: Path) -> str:
    return path.stem.removesuffix("_dataset")


def upload_all() -> int:
    files = sorted(SRC_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No hay CSV en {SRC_DIR}")

    s3 = boto3.Session(profile_name=PROFILE).client("s3")
    for path in files:
        key = f"{PREFIX}/{table_name(path)}/{path.name}"
        s3.upload_file(str(path), BUCKET, key)

        remote_size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        if remote_size != path.stat().st_size:
            raise RuntimeError(f"Tamano distinto en {key}: local={path.stat().st_size} s3={remote_size}")
        log.info("OK s3://%s/%s (%d bytes)", BUCKET, key, remote_size)
    return len(files)


if __name__ == "__main__":
    n = upload_all()
    log.info("%d archivos subidos y verificados", n)