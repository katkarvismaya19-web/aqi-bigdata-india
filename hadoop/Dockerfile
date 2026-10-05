# Spark 4.2 (same version as the original run) + Python 3.11 + Java 17, with the libraries the pipeline needs.
# pip's PySpark ships the Hadoop client, so Spark can read and write hdfs:// paths.
FROM python:3.11-slim-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "pyspark==4.2.0" "pandas>=2.0" "matplotlib>=3.7" "seaborn>=0.13"
ENV PYSPARK_PYTHON=python3 \
    MPLBACKEND=Agg
WORKDIR /app
