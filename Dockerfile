FROM apache/airflow:2.6.2

USER root

# Instalar librerías GEOS
RUN apt-get update && \
    apt-get install -y libgeos-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow
