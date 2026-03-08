FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY --chown=airflow:0 requirements/airflow.txt /opt/project/requirements/airflow.txt
RUN pip install --no-cache-dir -r /opt/project/requirements/airflow.txt
