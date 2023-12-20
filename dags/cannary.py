import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator

HOME = os.path.expanduser(os.environ['USER'])
VIRTUALENV = f'{HOME}/.virtualenvs/crypto_bot/bin/activate'

default_args = {
    'owner': 'A',
    'depends_on_past': False,
    'email': ['kqureshi@mit.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='cannary',
    description='A test DAG to make sure bot script functions correctly',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2021, 7, 2),
    tags=['test'],
    catchup=False,
) as dag:
    latest_only = LatestOnlyOperator(task_id='latest_only')
    test = BashOperator(
        task_id='test',
        bash_command=f'source {VIRTUALENV} && bot --help',
    )
    latest_only >> test


if __name__ == "__main__":
    dag.cli()
