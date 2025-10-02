import pytz
from datetime import datetime, timezone
from airflow.operators.python import PythonOperator
from airflow import DAG
import logging
import requests
from pkg.utility.bigquery_functionalities import *
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.hooks.base import BaseHook
import json

check_unhealthy_dhp_status_parameters = {
    "report_name": "data_asset_health",
    "publisher": "gcp-dataplatform@apexclearing.com",
    "skipped_datasets": ["data_platform_test", "data_platform_testing"],
    "is_healthy_false_value": "false",
    "check_results_table_name": "apex-datalake-mgmt-dev-00.dhp.dhp_unhealthy_results"

}
DATALAKE_TEST_V2 = "apex-internal-hub-dev-00.datalake_status.datalake_test_v2"


def check_unhealthy_dhp_status():

    current_time = datetime.now(timezone.utc)
    sql = f"""
    insert into `{check_unhealthy_dhp_status_parameters["check_results_table_name"]}`
    SELECT project_id, dataset_name, table_name, report_name, process_date, is_healthy, TIMESTAMP("{current_time}") AS status_checked_at
    FROM (
    SELECT
        s.publish_time,
        s.project_id ,
        s.dataset_name,
        s.table_name,
        s.process_date,
        s.publisher,
        s.report_name,
        test_description,
        is_healthy,
        description,
        ROW_NUMBER() OVER (
            PARTITION BY
            s.project_id,
            s.dataset_name,
            s.table_name,
            s.process_date,
            s.publisher,
            s.report_name
            ORDER BY s.publish_time DESC
        ) latest_record_identifier
    FROM
        `{DATALAKE_TEST_V2}` AS s
    Qualify latest_record_identifier = 1
    ) 
    where report_name = '{check_unhealthy_dhp_status_parameters["report_name"]}'
    and dataset_name not in ({', '.join(f"'{ds}'" for ds in check_unhealthy_dhp_status_parameters["skipped_datasets"])})
    and publisher = '{check_unhealthy_dhp_status_parameters["publisher"]}'
    and is_healthy = '{check_unhealthy_dhp_status_parameters["is_healthy_false_value"]}'"""

    logging.info(f"Executing SQL: {sql}")

    result = list(run_query(sql))
    if not result:
        logging.info("No unhealthy DHP statuses found.")
        return
    logging.info(f"Found {len(result)} unhealthy DHP statuses.")

    for row in result:
        logging.info(f"Processing row: {row}")


def create_dag(dag_id, schedule):

    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=datetime(
            2024, 6, 24, tzinfo=pytz.timezone('US/Central')),
        max_active_runs=1,
        catchup=False,
        tags=["team:datalake"],
        default_args={
            "owner": "datalake",
            "retries": 3,
        },
    )

    with dag:
        check_all_unhealthy_dhp_status = PythonOperator(
            task_id="check_all_unhealthy_dhp_status",
            python_callable=check_unhealthy_dhp_status,
            dag=dag,
            provide_context=True,
        )

        return dag


dag_id = "datalake_check_unhealthy_dhp_status"
schedule = None
dag = create_dag(dag_id, schedule)
