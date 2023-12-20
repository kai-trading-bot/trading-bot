import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime, timedelta

HOME = os.path.expanduser(os.environ['USER'])
VIRTUALENV = f'{HOME}/.virtualenvs/crypto_bot/bin/activate'

default_args = {
    'owner': 'A',
    'depends_on_past': False,
    'email': ['ein.kro@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='upcoming_ipo',
    default_args=default_args,
    description='Fetch and report upcoming IPOs',
    schedule_interval='0 0 * * 0',  # Every Sunday at midnight UTC
    start_date=datetime(2021, 6, 27),  # Sunday
    tags=['report'],
    catchup=False,
) as dag:
    latest_only = LatestOnlyOperator(task_id='latest_only')
    report_upcoming_ipo = BashOperator(
        task_id='report_upcoming_ipo',
        bash_command=f'source {VIRTUALENV} && bot report ipo',
    )
    latest_only >> report_upcoming_ipo


if __name__ == "__main__":
    dag.cli()
