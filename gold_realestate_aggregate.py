from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from sqlalchemy import create_engine, text

# ==============================================================================
# [DAG 정의: gold_realestate_aggregate]
# - 조건 1: ExternalTaskSensor로 silver_realestate_transform 완료 대기
# - 조건 2: Spark Job 실행은 spark-submit 호출 (PythonOperator 사용 금지)
# - 조건 3: 적재 후 검증 task (각 테이블 row count > 0 확인)
# ==============================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def get_most_recent_silver_execution_date(logical_date, **kwargs):
    """가장 최근에 성공한 silver_realestate_transform의 실행 시각을 감지"""
    runs = DagRun.find(dag_id="silver_realestate_transform", state="success")
    runs.sort(key=lambda x: x.execution_date, reverse=True)
    if runs:
        return runs[0].execution_date
    return logical_date

def verify_gold_tables_row_count():
    """적재 후 검증 task: 5개 테이블 row count > 0 확인"""
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    target_tables = [
        "gold_realestate_district_avg",
        "gold_realestate_top10",
        "gold_realestate_size_dist",
        "gold_realestate_age_avg",
        "gold_realestate_mom_change",
    ]
    with engine.connect() as conn:
        for tbl in target_tables:
            res = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            print(f"검증 확인: 테이블 [{tbl}] -> 총 행 수: {res}")
            assert res > 0, f"테이블 {tbl}의 데이터가 0건입니다!"
    print(">> 모든 5개 Gold 테이블 검증(row count > 0) 완료!")

with DAG(
    dag_id="gold_realestate_aggregate",
    default_args=default_args,
    description="과제 03: PySpark SQL 기반 5대 집계 및 PostgreSQL 적재 파이프라인",
    schedule_interval="@monthly",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["gold", "realestate", "postgres", "8주차"],
) as dag:

    # 1. 과제 02 DAG (silver_realestate_transform) 완료 대기 센서
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        execution_date_fn=get_most_recent_silver_execution_date,
        mode="reschedule",
        poke_interval=5,
        timeout=600,
    )

    # 2. spark-submit 명령으로 PySpark Gold 집계 실행
    run_gold_spark = BashOperator(
        task_id="run_gold_spark",
        bash_command="spark-submit --master local[*] /opt/airflow/scripts/Q3/gold_spark_sql.py",
    )

    # 3. PostgreSQL 적재 후 row count > 0 검증 태스크
    validate_gold_tables = PythonOperator(
        task_id="validate_gold_tables",
        python_callable=verify_gold_tables_row_count,
    )

    wait_for_silver >> run_gold_spark >> validate_gold_tables
