import os
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

DAG_ID = "bronze_realestate_collect"

API_URL = (
    "https://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)

LAWD_CODES = [
    "11680",
    "11650",
    "11710",
    "11440",
    "11170",
    "11200",
]

AWS_CONN_ID = "aws_default"


def collect_realestate(lawd_cd, deal_ymd):
    print(
        f"collector=백승진, "
        f"time={datetime.now()}, "
        f"lawd={lawd_cd}"
    )

    service_key = os.environ["DATA_GO_KR_SERVICE_KEY"]
    bucket_name = os.environ["S3_BUCKET_NAME"]

    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": "1",
        "numOfRows": "1000",
    }

    try:
        response = requests.get(
            API_URL,
            params=params,
            timeout=60,
        )

        response.raise_for_status()

        xml_data = response.text

        root = ET.fromstring(xml_data)

        total_count_node = root.find(".//totalCount")

        if total_count_node is None:
            print(f"[INVALID XML] lawd={lawd_cd}")
            return {
                "lawd_cd": lawd_cd,
                "status": "invalid_xml",
                "count": 0,
            }

        total_count = int(total_count_node.text or 0)

        if total_count == 0:
            print(
                f"[NO DATA] lawd={lawd_cd}, "
                f"deal_ymd={deal_ymd}"
            )

            return {
                "lawd_cd": lawd_cd,
                "status": "empty",
                "count": 0,
            }

        s3_key = f"bronze/{deal_ymd}/{lawd_cd}.xml"

        s3_hook = S3Hook(
            aws_conn_id=AWS_CONN_ID
        )

        s3_hook.load_string(
            string_data=xml_data,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True,
        )

        print(
            f"[S3 UPLOAD SUCCESS] "
            f"s3://{bucket_name}/{s3_key}"
        )

        print(
            f"[COUNT] lawd={lawd_cd}, "
            f"total_count={total_count}"
        )

        return {
            "lawd_cd": lawd_cd,
            "status": "success",
            "count": total_count,
            "s3_key": s3_key,
        }

    except ET.ParseError as e:
        print(
            f"[XML PARSE ERROR] "
            f"lawd={lawd_cd}, error={e}"
        )

        return {
            "lawd_cd": lawd_cd,
            "status": "invalid_xml",
            "count": 0,
        }

    except Exception as e:
        print(
            f"[COLLECT ERROR] "
            f"lawd={lawd_cd}, error={e}"
        )

        return {
            "lawd_cd": lawd_cd,
            "status": "failed",
            "count": 0,
        }


def branch_after_collect(ti):
    task_ids = [
        f"collect_group.collect_{lawd_cd}"
        for lawd_cd in LAWD_CODES
    ]

    results = ti.xcom_pull(task_ids=task_ids)

    print(f"collect_results={results}")

    if not results:
        return "skip_collect"

    for result in results:
        if not result:
            return "skip_collect"

        if result.get("status") != "success":
            return "skip_collect"

    return "summary_done"


with DAG(
    dag_id=DAG_ID,
    schedule="@monthly",
    start_date=datetime(2026, 9, 1),
    catchup=True,
    tags=["week8", "q1", "bronze", "realestate"],
) as dag:

    with TaskGroup(
        group_id="collect_group"
    ) as collect_group:

        for lawd_cd in LAWD_CODES:

            PythonOperator(
                task_id=f"collect_{lawd_cd}",
                python_callable=collect_realestate,
                op_kwargs={
                    "lawd_cd": lawd_cd,
                    "deal_ymd": (
                        "{{ data_interval_start.strftime('%Y%m') }}"
                    ),
                },
            )

    branch_after_collect_task = BranchPythonOperator(
        task_id="branch_after_collect",
        python_callable=branch_after_collect,
    )

    summary_done = EmptyOperator(
        task_id="summary_done"
    )

    skip_collect = EmptyOperator(
        task_id="skip_collect"
    )

    collect_group >> branch_after_collect_task

    branch_after_collect_task >> [
        summary_done,
        skip_collect,
    ]