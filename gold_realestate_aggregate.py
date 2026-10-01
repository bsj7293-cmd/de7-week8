import psycopg2

from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DAG_ID = "gold_realestate_aggregate"

GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def verify_postgres_tables():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow",
    )

    try:
        with conn.cursor() as cursor:

            for table_name in GOLD_TABLES:

                cursor.execute(
                    f"SELECT COUNT(*) "
                    f"FROM {table_name}"
                )

                row_count = cursor.fetchone()[0]

                print(
                    f"[VERIFY] "
                    f"table={table_name}, "
                    f"row_count={row_count}"
                )

                if row_count <= 0:
                    raise RuntimeError(
                        f"{table_name} "
                        "row_count가 0입니다."
                    )

        print(
            "[VERIFY SUCCESS] "
            "5개 Gold 테이블 모두 "
            "row_count > 0"
        )

    finally:
        conn.close()


with DAG(
    dag_id=DAG_ID,
    schedule="@monthly",
    start_date=datetime(2026, 9, 1),
    catchup=True,
    tags=[
        "week8",
        "q3",
        "gold",
        "realestate",
    ],
) as dag:

    #
    # Q2 완료 대기
    #
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id=(
            "silver_realestate_transform"
        ),
        external_task_id=(
            "run_silver_spark"
        ),
        allowed_states=[
            "success"
        ],
        failed_states=[
            "failed"
        ],
        mode="reschedule",
        poke_interval=10,
        timeout=600,
    )

    #
    # Gold Spark 집계 + PostgreSQL 적재
    #
    run_gold_spark = BashOperator(
        task_id="run_gold_spark",

        bash_command="""
        /home/airflow/.local/bin/spark-submit \
          --master "local[*]" \
          --packages \
          org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4 \
          /opt/airflow/scripts/Q3/gold_spark_sql.py
        """,

        env={
            "AWS_ACCESS_KEY_ID":
                "{{ conn.aws_default.login }}",

            "AWS_SECRET_ACCESS_KEY":
                "{{ conn.aws_default.password }}",

            "AWS_DEFAULT_REGION":
                "ap-northeast-2",

            "POSTGRES_HOST":
                "postgres",

            "POSTGRES_PORT":
                "5432",

            "POSTGRES_DB":
                "airflow",

            "POSTGRES_USER":
                "airflow",

            "POSTGRES_PASSWORD":
                "airflow",
        },

        append_env=True,
    )

    #
    # PostgreSQL 5개 테이블 row count 검증
    #
    verify_gold_tables = PythonOperator(
        task_id="verify_postgres_tables",
        python_callable=verify_postgres_tables,
    )

    (
        wait_for_silver
        >> run_gold_spark
        >> verify_gold_tables
    )