FROM apache/airflow:2.6.1-python3.9
COPY airflow_services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root

RUN apt-get update
RUN apt-get install -y openjdk-11-jdk

RUN mkdir -p /opt/airflow/data

USER root
RUN apt-get update
RUN apt-get install -y openjdk-11-jdk

WORKDIR /opt/airflow

COPY airflow_services .
COPY scripts/airflow/dags ./dags
COPY scripts/airflow/plugins ./plugins
COPY scripts/airflow/modules ./modules
COPY data ./data

RUN chmod -R 777 /opt/airflow
RUN chown airflow logs
RUN chown airflow dags
RUN chown airflow plugins
RUN chown airflow modules
RUN chown -R airflow /opt/airflow/data

WORKDIR /opt/airflow/dags