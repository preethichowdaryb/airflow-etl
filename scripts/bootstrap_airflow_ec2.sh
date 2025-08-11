#!/usr/bin/env bash
set -euo pipefail

PY=3.10
AF=2.9.3
AIRFLOW_HOME=${AIRFLOW_HOME:-$HOME/airflow}

# system deps
sudo apt update -y
sudo apt install -y python${PY}-venv python3-pip build-essential

# venv
python3 -m venv $HOME/venv
source $HOME/venv/bin/activate
pip install --upgrade pip wheel

# airflow + providers + libs
pip install "apache-airflow==${AF}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AF}/constraints-${PY}.txt"
pip install apache-airflow-providers-amazon pandas pyarrow

# init airflow & admin user
export AIRFLOW_HOME
airflow db init
airflow users create --username admin --firstname Admin --lastname User \
  --role Admin --email admin@example.com --password admin

echo "Bootstrap done. Next: copy dags/ and lib/ under \$AIRFLOW_HOME, start services."

