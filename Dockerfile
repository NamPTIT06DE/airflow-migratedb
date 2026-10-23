FROM apache/airflow:2.10.5

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       curl \
       libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir --no-deps "apache-airflow-providers-oracle<4.0.0"

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
