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
    'email': ['kqureshi@mit.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='signal_performance_report',
    default_args=default_args,
    description='Individual signal performance report',
    schedule_interval='0 22 * * 1-5',  # Every Weekday at 10pm
    start_date=datetime(2021, 7, 2, 22),
    tags=['report'],
    catchup=False,
) as dag:
    latest_only = LatestOnlyOperator(task_id='latest_only')
    report_signal_performance = BashOperator(
        task_id='report_signal_performance',
        bash_command=f'source {VIRTUALENV} && bot report signal',
    )
    latest_only >> report_signal_performance


if __name__ == "__main__":
    dag.cli()
