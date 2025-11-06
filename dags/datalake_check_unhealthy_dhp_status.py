import pytz
from datetime import datetime, timezone
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
import logging
import requests
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.hooks.base import BaseHook
import json
from airflow.models import Variable
from dags.pkg.utility.bigquery_functionalities import *  # needs path changed

check_unhealthy_dhp_status_parameters = {
    "data_asset_health_report_name": "data_asset_health",
    "dhp_validation_config_table_name": "apex-datalake-mgmt-dev-00.snapshot_service.pre_snapshot_dhp_configuration",
    "datalake_report_latest_health_table_name": "apex-internal-hub-dev-00.datalake_status.datalake_report_latest_health",
}
ACTIVE_SNAPSHOT_JOBS_TABLE = "apex-internal-hub-dev-00.common.active_snapshot_jobs"
INTERNAL_HUB = "apex-internal-hub-dev-00"


def get_process_date():
    PROCESS_DATE = Variable.get("process_date")
    mm, dd, yyyy = PROCESS_DATE.split('-')
    return yyyy+"-"+mm+"-"+dd


def fetch_eod_job_names():
    query_to_fetch_eod_snapshots = f"""
    SELECT job_name FROM `{ACTIVE_SNAPSHOT_JOBS_TABLE}` where  ("EOD") in UNNEST(labels)
    """
    eod_job_name_list = list(result['job_name']
                             for result in run_query(query_to_fetch_eod_snapshots))
    return eod_job_name_list


def check_unhealthy_eod_dhp_validations_method():
    process_date = get_process_date()
    eod_job_name_list = fetch_eod_job_names()

    if not eod_job_name_list:
        logging.info("No active EOD snapshot jobs found.")
        return

    query_to_find_related_assets = f"""
    DECLARE EOD_SNAPSHOTS ARRAY<STRING> DEFAULT [{', '.join(f"'{job_name}'" for job_name in eod_job_name_list)}];
    SELECT DISTINCT table_name
    FROM `{check_unhealthy_dhp_status_parameters["dhp_validation_config_table_name"]}`
    WHERE is_active = TRUE
    and job_name in UNNEST(EOD_SNAPSHOTS)
    """
    logging.info(
        f"Executing SQL to find assets related to DHP Validtions in EOD: {query_to_find_related_assets}")
    results = list(run_query(query_to_find_related_assets))
    if not results:
        logging.info("No related assets found for DHP Validations in EOD.")
        return
    related_tables = [row['table_name'] for row in results]
    logging.info(f"Related assets found: {related_tables}")
    tables_dict = {}
    for full_table_name in related_tables:
        project_id, dataset_name, table_name = full_table_name.split('.')
        if project_id not in tables_dict:
            tables_dict[project_id] = {}
        if dataset_name not in tables_dict[project_id]:
            tables_dict[project_id][dataset_name] = []
        tables_dict[project_id][dataset_name].append(table_name)
    logging.info(f"Organized tables dictionary: {tables_dict}")
    sql_prefix = f"""
    SELECT project_id, dataset_name, table_name, report_name, process_date, publisher, is_healthy
    FROM `{check_unhealthy_dhp_status_parameters["datalake_report_latest_health_table_name"]}`
    where report_name = '{check_unhealthy_dhp_status_parameters["data_asset_health_report_name"]}'
    and is_healthy = 'false'
    and process_date <= "{process_date}"
    """
    sql_to_fetch_status = ""
    if tables_dict:
        for project_id in tables_dict:
            for dataset_name in tables_dict[project_id]:
                sql_to_fetch_status += sql_prefix + " and project_id = '{project_id}' and dataset_name = '{dataset_name}' and table_name in ( {table_names} ) ".format(
                    project_id=project_id,
                    dataset_name=dataset_name,
                    table_names=", ".join(
                        f"'{table}'" for table in tables_dict[project_id][dataset_name])
                ) + f"\n UNION ALL \n"
        sql_to_fetch_status = sql_to_fetch_status.rstrip(f"\n UNION ALL \n")
    logging.info(
        f"Executing SQL to fetch unhealthy DHP statuses related to EOD validations: {sql_to_fetch_status}")
    result = list(run_query(sql_to_fetch_status))
    if not result:
        logging.info("No unhealthy DHP statuses found for EOD validations.")
        return
    raise AirflowException(
        f"Found {len(result)} unhealthy DHP statuses for EOD validations. please check the query above to see all unhealthy reports")


def check_unhealthy_snapshot_service_updates_method():
    query_to_fetch_eod_snapshots = f"""
    SELECT job_name FROM `{ACTIVE_SNAPSHOT_JOBS_TABLE}` where  ("EOD") in UNNEST(labels)
    """
    eod_job_name_list = list(result['job_name']
                             for result in run_query(query_to_fetch_eod_snapshots))

    if not eod_job_name_list:
        logging.info("No active EOD snapshot jobs found.")
        return
    sql_to_fetch_unhealthy_snapshot_updates = f"""
    DECLARE EOD_SNAPSHOTS ARRAY<STRING> DEFAULT [{', '.join(f"'{job_name}'" for job_name in eod_job_name_list)}];
        SELECT project_id, dataset_name, table_name, report_name, process_date, publisher, is_healthy
        FROM `{check_unhealthy_dhp_status_parameters["datalake_report_latest_health_table_name"]}`
        where is_healthy = 'false'
        and process_date <= "{get_process_date()}"
        and project_id = '{INTERNAL_HUB}'
        and dataset_name = 'snapshots'
        and table_name in UNNEST(EOD_SNAPSHOTS)
        """

    logging.info(
        f"Executing SQL to fetch unhealthy snapshot service assets updates: {sql_to_fetch_unhealthy_snapshot_updates}")
    results = list(run_query(sql_to_fetch_unhealthy_snapshot_updates))
    if not results:
        logging.info("No unhealthy snapshot service assets updates found.")
        return
    raise AirflowException(
        f"Found {len(results)} unhealthy snapshot service assets updates. please check the query above to see all unhealthy reports")


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
        start = EmptyOperator(
            task_id="Start",
            dag=dag,
        )
        end = EmptyOperator(
            task_id="End",
            dag=dag,
        )

        check_unhealthy_eod_dhp_validations = PythonOperator(
            task_id="check_unhealthy_eod_dhp_validations",
            python_callable=check_unhealthy_eod_dhp_validations_method,
            dag=dag,
            provide_context=True,
            retries=0,
        )

        check_unhealthy_snapshot_service_updates = PythonOperator(
            task_id="check_unhealthy_snapshot_service_updates",
            python_callable=check_unhealthy_snapshot_service_updates_method,
            dag=dag,
            provide_context=True,
            retries=0,
        )
        start >> [
            check_unhealthy_eod_dhp_validations,
            check_unhealthy_snapshot_service_updates,
        ] >> end
        return dag


dag_id = "datalake_check_unhealthy_dhp_status"
schedule = None
dag = create_dag(dag_id, schedule)
