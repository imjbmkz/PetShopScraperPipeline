import sys
import datetime as dt
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

AIRFLOW_HOME = "/home/josh/airflow"
PYTHON_ENV_PATH = "/venvs/pet_scraper_env/bin/python3"

with DAG(
    dag_id="get_links",
    start_date=dt.datetime(2021, 1, 1),
    schedule="@daily",
):
    BashOperator(
        task_id="test_bash",
        bash_command=f"echo {AIRFLOW_HOME}{PYTHON_ENV_PATH} {AIRFLOW_HOME}/jobs/job_get_links_zooplus.py",

    )

print("success")