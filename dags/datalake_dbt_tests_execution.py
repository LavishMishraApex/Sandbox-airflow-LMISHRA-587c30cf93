import json
from airflow.hooks.base import BaseHook
import requests
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter
import logging
from airflow.operators.python import get_current_context
# from ascend.datalake.config.globals import CLOUDRUN_URL, ENVIRONMENT
from airflow.exceptions import AirflowException
from pkg.utility import get_id_token


CLOUDRUN_URL = "https://dbt-apex-datalake-h3n6ulr52q-uc.a.run.app"
ENVIRONMENT = "dev"


def fetch_list_of_tests_for_job(job_name: str):
    return ['internal_hub__snapshots__pre_snapshot_validation__daily_positions']


parameters = {
    "PROCESS_DATE": "2025-13-07",
    "SNAPSHOT_NAME": "daily_positions"
}


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
    logging.info("Executing the dbt test command: {}".format(test_command))
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
    precheck_validation_names_array = fetch_list_of_tests_for_job(job_name)
    logging.info("list of tests fetched for job_name {} is".format(job_name))
    logging.info(precheck_validation_names_array)
    for test_name in precheck_validation_names_array:
        run_dbt_test(test_name, parameters)
