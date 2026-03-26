import pytz
import logging
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import datetime
from datetime import datetime, date, timedelta
import json

# need to add ascend.datalake instead of dags to the path
from dags.pkg.utility.bigquery_functionalities import *
# need to add ascend.datalake. instead of dags to the path
from dags.pkg.utility.dbt_functionalities import *
# need to add ascend.datalake. instead of dags to the path
from dags.pkg.utility.dhp_functionalities import *

# need to add ascend.datalake. instead of dags to the path
# from dags.config.globals import GCP_PROJECTS, ENVIRONMENT

# need to remove this block and import configs
ENVIRONMENT = "dev"
GCP_PROJECTS = {
    "DATALAKE_MGMT": "apex-datalake-mgmt-dev-00",
    "INTERNAL_HUB": "apex-internal-hub-dev-00"
}


def fetch_dict_of_assets_and_tests_for_job(job_name: str) -> list:
    '''
    Args:
        job_name (str): The name of the job for which dbt tests are to be fetched
    Returns a list of dbt tests for a given job_name
    This functions take a job_name as input and returns a list of dbt tests for that job_name as mentioned in Bigquery Configurations

    '''
    DATALAKE_MGMT = GCP_PROJECTS["DATALAKE_MGMT"]
    bigquery_configurations_table_name = "{}.snapshot_service.sync_report_checks".format(
        DATALAKE_MGMT)  # define how this table would be stored
    column_name_for_list_of_sync_reports = "sync_report_array"

    query_string = f"""
        SELECT * from `{bigquery_configurations_table_name}` where job_name = '{job_name}' and is_active = True
    """

    results = list(run_query(query_string))
    dict_of_asset_and_related_tests = {}
    if results:
        for row in results:
            dict_of_asset_and_related_tests[row["full_asset_name"]
                                            ] = row[column_name_for_list_of_sync_reports]

    else:
        # return empty list string if no tests are found
        dict_of_asset_and_related_tests = {}
    return dict_of_asset_and_related_tests


def execute_tests(job_name: str, process_date: str) -> None:
    logging.info(
        f"Fetching the list of tests for job_name {job_name} and receive process_date {process_date} from xcom")
    dict_of_asset_and_related_tests = fetch_dict_of_assets_and_tests_for_job(
        job_name)
    logging.info(
        f"Dict of assets and related tests fetched for job_name {job_name} is {dict_of_asset_and_related_tests}")
    sync_report_asset_failures = set()
    if not dict_of_asset_and_related_tests:
        logging.info(
            f"No active sync report checks found for job_name {job_name}")
        return
    for full_asset_name, sync_report_array in dict_of_asset_and_related_tests.items():
        # Convert string back to list of dicts
        sync_report_array = json.loads(sync_report_array)
        logging.info(
            f"::group::Checking Sync Reports for the asset {full_asset_name}")
        errors_in_dbt_test = {}
        for record in sync_report_array:
            logging.info(
                f"::group::     Validating sync report {record['test_name']}")
            parameters = {}
            if "PROCESS_DATE" in record["variables"]:
                parameters["PROCESS_DATE"] = process_date
            if "JOB_NAME" in record["variables"]:
                parameters["JOB_NAME"] = job_name

            r = run_dbt_test(record["test_name"], parameters,
                             slack_alert=False, target="ascend_eod")

            if r.status_code != 200:
                errors_in_dbt_test[record["test_name"]] = {
                    "text": "ERROR IN DBT TEST EXECUTION " + r.text, "status_code": r.status_code}
                sync_report_asset_failures.add(full_asset_name)
            elif 'returned non-zero exit status' in r.text:
                errors_in_dbt_test[record["test_name"]] = {
                    "text": "TEST EXECUTED BUT FAILED " + r.text, "status_code": r.status_code}
            logging.info(f"::endgroup::")
        if errors_in_dbt_test:
            logging.error(
                f"Sync Reports failed for the following tests: {list(errors_in_dbt_test.keys())}")
            for test_name, error_details in errors_in_dbt_test.items():
                logging.error(f"Error in Test: {test_name}")
                logging.error(f"::group::details")
                logging.error(f"Status Code: {error_details['status_code']}")
                logging.error(f"Error Details: {error_details['text']}")
                logging.error("::endgroup::")
        logging.info(f"::endgroup::")
    if sync_report_asset_failures:
        logging.error(
            f"Sync Report Checks failed for the following assets: {sync_report_asset_failures}, please check the logs for more details on the failed tests and errors")
        raise AirflowFailException(
            f"Sync Report Checks failed for the following assets: {sync_report_asset_failures}")
    for full_asset_name in dict_of_asset_and_related_tests.keys():
        dhp_dict = {
            "full_table_name": full_asset_name,
            "report_name": "data_asset_health",
            "description": "Sync Report health certification",
            "publisher": "datalake@apexclearing.com",
            "process_date": process_date,
        }
        if full_asset_name not in sync_report_asset_failures:
            dhp_dict["is_healthy"] = True
            logging.info(
                f"All Sync Report Checks passed for the asset {full_asset_name}, sent out Healthy message to DHP")
        else:
            dhp_dict["is_healthy"] = False
            logging.info(
                f"Sync Report Checks failed for the asset {full_asset_name}, sent out Unhealthy message to DHP")
        certify_asset(dhp_dict)


def sync_report_checks(job_name: str, process_date: str) -> PythonOperator:

    retries = 3
    minutes = 5

    sync_report_checks_task = PythonOperator(
        task_id="sync_report_checks_"+job_name,
        python_callable=execute_tests,
        op_kwargs={
            "job_name": job_name,
            "process_date": process_date
        },
        retries=retries,
        retry_delay=timedelta(minutes=minutes),
    )
    return sync_report_checks_task
