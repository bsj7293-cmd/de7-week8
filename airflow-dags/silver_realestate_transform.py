from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DAG_ID = "silver_realestate_transform"

with DAG(
    dag_id=DAG_ID,
    schedule="@monthly",
    start_date=datetime(2026, 9, 1),
    catchup=True,
    tags=["week8", "q2", "silver", "realestate"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id="summary_done",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=10,
        timeout=600,
    )

    run_silver_spark = BashOperator(
        task_id="run_silver_spark",
        bash_command="""
        /home/airflow/.local/bin/spark-submit \
            --master "local[*]" \
            --conf spark.ui.port=4040 \
            --packages org.apache.hadoop:hadoop-aws:3.3.4 \
            /opt/airflow/scripts/Q2/silver_spark.py \
            --yyyymm "{{ logical_date.strftime('%Y%m') }}"
        """,

        env={
        "AWS_ACCESS_KEY_ID": "{{ conn.aws_default.login }}",
        "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_default.password }}",
        "AWS_DEFAULT_REGION": "ap-northeast-2",
        },

        append_env=True,
    )

    wait_for_bronze >> run_silver_spark