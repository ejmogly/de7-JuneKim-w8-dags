from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="hello_gitsync",
    default_args=default_args,
    description="과제 04: git-sync 배포 확인용 테스트 DAG",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "git-sync", "8주차"],
) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "hello from git-sync - $(date)"',
    )
