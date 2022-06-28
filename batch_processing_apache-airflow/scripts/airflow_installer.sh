#!/bin/bash
# -- use chmod u+x airflow_installer.sh 
export AIRFLOW_HOME=~/airflow

AIRFLOW_VERSION=2.2.5
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.6.txt

pip install "apache-airflow[celery]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install "apache-airflow-providers-apache-spark"

airflow db init

airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org 


airflow webserver --port 8090 -D

airflow scheduler