import json
import logging
import requests
import datetime
import pytz
from pkg.utility import get_id_token

from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context


CLOUDRUN_URL = "https://dbt-apex-datalake-h3n6ulr52q-uc.a.run.app"
ENVIRONMENT = "dev"
tests = {
    "intentional_fail_test": {
        "test_name": "intentional_fail_test",
        "parameters": {
            "FLAG": "TRUE",
        }
    },
    "internal_hub__snapshots__daily_positions": {
        "test_name": "internal_hub__snapshots__daily_positions",
        "parameters": {
            "PROCESS_DATE": "2025-04-01",
            "SNAPSHOT_NAME": "daily_positions"
        }
    },
}


def execute_tests(tests):
    errors_in_dbt_test = {}
    for test_name, test_details in tests.items():
        test_name = test_details.get("test_name")
        parameters = test_details.get("parameters")
        logging.info(
            f"Running DBT test: {test_name} with parameters: {parameters}")
        r = run_dbt_test(test_name, parameters)
        if r.status_code != 200:
            errors_in_dbt_test[test_name] = {
                "text": "ERROR IN DBT TEST EXECUTION " + r.text, "status_code": r.status_code}
        elif 'returned non-zero exit status' in r.text:
            errors_in_dbt_test[test_name] = {
                "text": "TEST EXECUTED BUT FAILED " + r.text, "status_code": r.status_code}
    if errors_in_dbt_test:
        logging.error(
            f"DBT test failed for the following tests: {list(errors_in_dbt_test.keys())}")
        for test_name, error_details in errors_in_dbt_test.items():
            logging.info(f"Error in Test: {test_name}")
            logging.info(f"::group::details")
            logging.info(f"Status Code: {error_details['status_code']}")
            logging.info(f"Error Details: {error_details['text']}")
            logging.info("::endgroup::")
        raise Exception(
            f"DBT test failed for the following tests: {list(errors_in_dbt_test.keys())}, please check the logs above for details on errors")


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
                         "log": context.get("task_instance").log_url,
                         "slack_alert": False}),
        timeout=7200)
    # print(f'Response Code: {r.status_code}, Response: {r.text}')
    return r


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
            op_kwargs={"tests": tests},
            dag=dag,
            provide_context=True,
        )

        return dag


dag_id = "datalake_dbt_test_check"
schedule = None  # needs to be changed to 2 am CST
dag = create_dag(dag_id, schedule)
