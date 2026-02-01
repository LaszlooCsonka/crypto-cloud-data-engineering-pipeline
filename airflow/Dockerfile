FROM apache/airflow:2.7.2

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.1 \
    pyspark==3.4.1 \
    apache-airflow-providers-openlineage==1.1.0 \
    apache-airflow-providers-google==10.1.0