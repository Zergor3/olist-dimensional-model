FROM apache/airflow:3.3.2

# Dependencias extra que usan tus scripts (pandas, etc.)
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# mmh3 (dependencia de dbt-athena) se compila desde el codigo fuente: necesita gcc y g++
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# dbt en un entorno virtual aparte: sus dependencias no deben chocar con las de Airflow.
# Versiones fijadas a las mismas que usas en local.
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.5 dbt-postgres==1.11.0 dbt-athena==1.11.1 "botocore[crt]"