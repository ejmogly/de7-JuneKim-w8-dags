from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.models import DagRun

# ==============================================================================
# [DAG 정의: silver_realestate_transform]
# - 조건 1: ExternalTaskSensor로 bronze_realestate_collect 완료 대기
# - 조건 2: Spark Job 실행은 spark-submit 호출 (PythonOperator 사용 금지)
# ==============================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def get_most_recent_bronze_execution_date(logical_date, **kwargs):
    """가장 최근에 성공한 bronze_realestate_collect의 실행 시각을 감지"""
    runs = DagRun.find(dag_id="bronze_realestate_collect", state="success")
    runs.sort(key=lambda x: x.execution_date, reverse=True)
    if runs:
        return runs[0].execution_date
    return logical_date

with DAG(
    dag_id="silver_realestate_transform",
    default_args=default_args,
    description="과제 02: PySpark 기반 부동산 실거래가 Silver 정제 및 UDF 변환 파이프라인",
    schedule_interval="@monthly",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["silver", "realestate", "spark", "8주차"],
) as dag:

    # 1. 과제 01 DAG (bronze_realestate_collect) 완료 대기 센서
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id="summary_done",
        execution_date_fn=get_most_recent_bronze_execution_date,
        mode="reschedule",
        poke_interval=5,
        timeout=600,
    )

    # 2. spark-submit 명령으로 PySpark 정제 잡 실행
    # (주의: 문제지 11페이지 채점 기준 준수 - BashOperator + spark-submit)
    run_silver_spark = BashOperator(
        task_id="run_silver_spark",
        bash_command="spark-submit --master local[*] /opt/airflow/scripts/Q2/silver_spark.py",
    )

    wait_for_bronze >> run_silver_spark
