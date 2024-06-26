from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException

from  config.globals import CLOUDRUN_URL, ENVIRONMENT
from  pkg.utility import get_id_token
#from dags.pkg.utility import get_id_token
#CLOUDRUN_URL = "https://dbt-apex-datalake-h3n6ulr52q-uc.a.run.app"
#ENVIRONMENT = 'dev'

import json
import requests
from datetime import datetime, date


def determine_health(context, table):
    """
    Determines health of snapshot test based on xcom result.
    A DBT test will return a successful 200 response code even though the test itself failed.
    This checks the xcom to see if the underlying tests succeeded

    :param context: current context to pull xcom from
    :param table: snapshot table name (e.g. 'daily_accounts')
    :return: 1 if healthy, 0 if unhealthy
    """
    result_code = context['ti'].xcom_pull(key=f'{table}_result')
    result_text = context['ti'].xcom_pull(key=f'{table}_result_text')

    if result_code == 200:
        if 'returned non-zero exit status' in result_text:
            print(f'returned non-zero exit status, but returned status code of 200. Failing test.')
            return 0
        return 1
    else:
        return 0

def is_dbt_test_rerunnable(context, table):
    """
    Determines if a DBT test is rerunnable based on xcom result.
    It's rerunnable if the return result is a 500, 'Internal Server Error'.
    :param context: current context to pull xcom from
    :param table: snapshot table name (e.g. 'daily_accounts')
    :return: 1 if the test is rerunnable, 0 if not.
    """
    result_code = context['ti'].xcom_pull(key=f'{table}_result')
    result_text = context['ti'].xcom_pull(key=f'{table}_result_text')

    if result_code == 500:
        if 'Internal Server Error' in result_text:
            print(f'Returned result is \'Internal Server Error\'. Marking test as rerunnable.')
            return 1
        return 0
    else:
        return 0

@task(retries=5)
def run_dbt_test(table: str, process_date: str, test_name: str):
    """
    Runs DBT test and publishes result to xcom

    :param table: Snapshot table name (e.g. 'daily_accounts')
    :param process_date: e.g. '2021-01-01'
    :param test_name: DBT Test name to run (e.g. 'internal_hub__snapshots__daily_accounts_row_count')
    :return:
    """
    formatted_process_date = date.fromisoformat(process_date)
    test_command = 'dbt test --select {0} --vars '.format(test_name)
    parameters = {
        "PROCESS_DATE": str(formatted_process_date),
        "SNAPSHOT_NAME": table
    }
    command = test_command + json.dumps(parameters).replace(' ', '')

    print(f'command = {command}')
    context = get_current_context()
    http_conn_id = "http_cloudrun_dbt"
    service_url = BaseHook.get_connection(http_conn_id).host + '/run_test'
    r = requests.post(
        url=service_url,
        headers={
            "Authorization": f"Bearer {get_id_token(CLOUDRUN_URL)}",
            "Content-Type": "application/json"
        },
        data=json.dumps({"command": command, "source": "DAG", "log": context.get("task_instance").log_url}),
    )
    print(f'Response Code: {r.status_code}')

    task_instance = context['ti']
    task_instance.xcom_push(key=f'{table}_result', value=r.status_code)
    task_instance.xcom_push(key=f'{table}_result_text', value=r.text)
    print(f'Response: {r.text}')

    if is_dbt_test_rerunnable(context, table):
        raise AirflowException('DBT Test failed, but in a rerunnable state.  Check logs for details.')

@task(retries=0)
def publish_health_report(table: str, process_date: str, test_name: str):
    """
    Publishes health report to DHP

    :param table: snapshot table name (e.g. 'daily_accounts')
    :param process_date: e.g. '2021-01-01'
    :param test_name: test_name and test_description that will be included in the DHP report
    :return: None
    """
    context = get_current_context()
    is_healthy = determine_health(context, table)
    print(f'is_healthy = {is_healthy}')

    http_conn_id = "http_cloudrun_datahealth"
    service_url = BaseHook.get_connection(http_conn_id).host
    print(f'service_url = {service_url}')

    data_health_report = {
        "project_name": "apex-internal-hub-{}-00".format(ENVIRONMENT),
        "dataset_name": "snapshots",
        "table_name": f"{table}",
        "process_date": f"{process_date}",
        "tester_email_address": "gcp-dataplatform@apexclearing.com",
        "test_name": f"{test_name}",
        "test_description": f"{test_name}",
        "is_healthy": is_healthy,
        "report_timestamp": datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S.%f"),
    }
    formatted_report = json.dumps(data_health_report)
    print(f'publishing health report = {formatted_report}')

    r = requests.post(
        url=service_url,
        headers={
            "Authorization": f"Bearer {get_id_token(service_url)}",
            "Content-Type": "application/json"
        },
        data=formatted_report
    )
    print(f'Response Code: {r.status_code}')
    if r.status_code != 200:
        print(f'Response text: {r.text}')
        r.raise_for_status()


@task(retries=0)
def check_for_snapshot_failures(table: str):
    """
    Check's the current context's xcom for the result of the test

    :param table: Snapshot name (e.g. 'daily_accounts')
    :return: None
    """
    context = get_current_context()
    is_healthy = determine_health(context, table)
    print(f'is_healthy = {is_healthy}')

    if is_healthy == 0:
        print(f'is_healthy = {is_healthy}')
        raise Exception('Snapshot test failed.  Check logs for details.')


def validate_row_count(table: str, process_date: str) -> TaskGroup:
    """
    Runs DBT test for row counts and publishes health report
    Compares source table to snapshot output row counts

    :param table: snapshot table name (e.g. 'daily_accounts')
    :param process_date: e.g. '2021-01-01'
    :return: A task group for the validation
    """
    with TaskGroup(group_id=f"snapshot_row_count_validation__{table}") as validation_tg:
        #run_dbt_test.override(task_id=f'internal_hub__snapshots__row_count_test__{table}')(table, process_date, f'internal_hub__snapshots__row_count_test__{table}') >> publish_health_report(table, process_date, 'row_count') >> check_for_snapshot_failures(table)
        run_dbt_test(table, process_date, f'internal_hub__snapshots__row_count_test__{table}') >> publish_health_report(table, process_date, 'row_count') >> check_for_snapshot_failures(table)
    return validation_tg


def validate_accessibility(table: str, process_date: str) -> TaskGroup:
    """
    Runs DBT test for accessibility and publishes health report

    :param table: snapshot table name (e.g. 'daily_accounts')
    :param process_date: e.g. '2021-01-01'
    :return: A task group for the validation
    """
    with TaskGroup(group_id=f"snapshot_accessibility_validation__{table}") as validation_tg:
        #run_dbt_test.override(task_id=f'internal_hub__snapshots__accessibility_test__{table}')(table, process_date, f'internal_hub__snapshots__accessibility_test') >> publish_health_report(table, process_date, 'accessibility') >> check_for_snapshot_failures(table)
        run_dbt_test(table, process_date, f'internal_hub__snapshots__accessibility_test') >> publish_health_report(table, process_date, 'accessibility') >> check_for_snapshot_failures(table)
    return validation_tg

def validate_clear_text(table: str, process_date: str) -> TaskGroup:
    """
    Runs DBT test for ensuring snapshot is saved as clear text and publishes health report

    :param table: snapshot table name (e.g. 'daily_accounts')
    :param process_date: e.g. '2021-01-01'
    :return: A task group for the validation
    """
    with TaskGroup(group_id=f"snapshot_clear_text_validation__{table}") as validation_tg:
        #run_dbt_test.override(task_id=f'internal_hub__snapshots__clear_text_test')(table, process_date, f'internal_hub__snapshots__clear_text_test') >> publish_health_report(table, process_date, 'clear_text') >> check_for_snapshot_failures(table)
        run_dbt_test(table, process_date, f'internal_hub__snapshots__clear_text_test') >> publish_health_report(table, process_date, 'clear_text') >> check_for_snapshot_failures(table)
    return validation_tg
