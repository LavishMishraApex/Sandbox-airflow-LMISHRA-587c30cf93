import json
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context

# from ascend.datalake.config.globals import CLOUDRUN_URL, ENVIRONMENT
from airflow.exceptions import AirflowException
from pkg.utility import get_id_token
from pkg.utility.bigquery_functionalities import *

from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

import datetime
import requests
import logging
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator


CLOUDRUN_URL = "https://dbt-apex-datalake-h3n6ulr52q-uc.a.run.app"
ENVIRONMENT = "dev"


def fetch_list_of_tests_for_job(job_name: str):
    DATALAKE_MGMT = "apex-datalake-mgmt-dev-00"
    bigquery_configurations_table_name = "{}.snapshot_service.pre_snapshot_test_configuration".format(
        DATALAKE_MGMT)  # define how this table would be stored
    column_name_for_list_of_dbt_tests = "precheck_validation_names_array"
    query_string = f"""
        SELECT * from `{bigquery_configurations_table_name}` where job_name = '{job_name}'
    """
    logging.info("executing query {}".format(query_string))
    results = list(run_query(query_string))
    logging.info("results fetched from bigquery")
    logging.info(results)
    if results:
        precheck_validation_names_array = results[0][column_name_for_list_of_dbt_tests]
    else:
        # return empty list string if no tests are found
        precheck_validation_names_array = "[]"
    # return precheck_validation_names_array
    logging.info("precheck_validation_names_array fetched from bigquery")
    logging.info(precheck_validation_names_array)
    return precheck_validation_names_array
    # return ['internal_hub__snapshots__pre_snapshot_validation__daily_positions']


parameters = {
    "PROCESS_DATE": "2025-01-19",
    "SNAPSHOT_NAME": "daily_positions"
}
process_date = parameters["PROCESS_DATE"]


def run_dbt_test(test_name: str, parameters: dict):
    """
       Runs DBT test and publishes result to xcom

       :param table: Snapshot table name (e.g. 'daily_accounts')
       :param process_date: e.g. '2021-01-01'
       :param test_name: DBT Test name to run (e.g. 'internal_hub__snapshots__daily_accounts_row_count')
       :return:
       """
    logging.info("Running dbt test {}".format(test_name))
    test_command = 'dbt test --select {0} --vars '.format(test_name)
    command = test_command + json.dumps(parameters).replace(' ', '')
    logging.info("Executing the dbt test command: {}".format(command))
    http_conn_id = "http_cloudrun_dbt"
    service_url = BaseHook.get_connection(http_conn_id).host + '/run_test'

    # add session to keep alive
    session = requests.Session()
    keep_alive = TCPKeepAliveAdapter(interval=10, idle=20, count=10)
    session.mount(CLOUDRUN_URL, keep_alive)
    context = get_current_context()
    r = session.post(
        url=service_url,
        headers={
            "Authorization": f"Bearer {get_id_token(CLOUDRUN_URL)}",
            "Content-Type": "application/json"
        },
        data=json.dumps({"command": command, "source": "DAG",
                         "log": context.get("task_instance").log_url}),
        timeout=7200,
    )
    print(f'Response Code: {r.status_code}')


def execute_tests(job_name: str):
    '''
    Args:
        job_name (str): The process_date for which the tests are to be run
    This function is used to run the dbt tests for the given process_date
    '''
    logging.info("fetching the list of tests for job_name {}".format(job_name))
    precheck_validation_names_array = json.loads(
        fetch_list_of_tests_for_job(job_name))
    logging.info("list of tests fetched for job_name {} is".format(job_name))
    logging.info(precheck_validation_names_array)
    for record in precheck_validation_names_array:
        parameters = {}
        if "PROCESS_DATE" in record["variables"]:
            parameters["PROCESS_DATE"] = process_date
        if "JOB_NAME" in record["variables"]:
            parameters["JOB_NAME"] = job_name
        run_dbt_test(record["test_name"], parameters)


def create_dag(dag_id, schedule):

    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=datetime.datetime(
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
        # dummy operator to start the dag

        datalake_dbt_test = PythonOperator(
            task_id="datalake_dbt_test",
            python_callable=execute_tests,
            op_kwargs={'job_name': parameters["SNAPSHOT_NAME"]},
            dag=dag,
            provide_context=True,
        )

        return dag


dag_id = "datalake_dbt_tests_execution"
schedule = None  # needs to be changed to 2 am CST
dag = create_dag(dag_id, schedule)
