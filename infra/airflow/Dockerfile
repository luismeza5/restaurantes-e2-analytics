# Airflow image that can also act as the Spark driver (spark-submit --master
# local[*]) and run the Neo4j loader. Bundles a JDK + pyspark + the JDBC/graph
# clients so the whole ELT runs from one worker — no separate Spark cluster
# needed for the course-scale dataset (scale out by pointing SPARK_MASTER at a
# standalone cluster).
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# pyspark provides spark-submit; the rest are used by the jobs/loaders.
RUN pip install --no-cache-dir \
      pyspark==3.5.1 \
      psycopg2-binary==2.9.9 \
      neo4j==5.23.0 \
      pandas==2.2.2 \
      pyarrow==16.1.0 \
      faker==25.8.0 \
      requests==2.32.3

# Pre-cache the Postgres JDBC jar into Ivy so spark-submit doesn't hit the
# network on every DAG run (best-effort; ignored if offline at build time).
RUN spark-submit --packages org.postgresql:postgresql:42.7.3 \
      --class org.apache.spark.deploy.SparkSubmit /dev/null 2>/dev/null || true

ENV PYTHONPATH=/opt/project
WORKDIR /opt/project
