# This pakckage can be imported as from ascend.datalake.pkg.utility.dbt_functionalities import *
import logging
import json
from airflow.hooks.base import BaseHook
import datetime
import requests
from dags.pkg.utility import get_id_token

from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter
from airflow.operators.python import get_current_context

ENVIRONMENT = "dev"


def run_dbt_test(test_name: str, parameters: dict, slack_alert: bool = True):
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
    session.mount(service_url, keep_alive)
    context = get_current_context()
    r = session.post(
        url=service_url,
        headers={
            "Authorization": f"Bearer {get_id_token(service_url)}",
            "Content-Type": "application/json"
        },
        data=json.dumps({"command": command, "source": "DAG",
                         "log": context.get("task_instance").log_url,
                         "slack_alert": slack_alert}),
        timeout=7200,
    )
    print(f'Response Code: {r.status_code}')
    # you would have to check the response code and text to see if the test was successful or not
    return r
