import pytz
import logging
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import datetime
from datetime import timedelta
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


def fetch_dict_of_assets_and_tests_for_job(job_name: str) -> dict:
    """
    Fetches a dictionary of assets and their associated sync report tests for a given job.

    This function queries the BigQuery configurations table to retrieve active sync report
    checks for the specified job and returns them as a dictionary mapping asset names to
    their test configurations.

    Args:
        job_name (str): The name of the job for which to fetch sync report tests.
                       This should match a job_name value in the BigQuery configurations table.

    Returns:
        dict: A dictionary where keys are full asset names (table names) and values are
              JSON strings containing arrays of sync report test configurations.
              Returns an empty dictionary if no active tests are found for the job.
    """
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


def run_single_test(test_record: dict, job_name: str, process_date: str) -> tuple:
    """
    Executes a single sync report test with appropriate parameters.

    Args:
        test_record (dict): Test configuration containing test_name and variables
        job_name (str): The name of the job for parameter substitution
        process_date (str): The process date for parameter substitution

    Returns:
        tuple: (test_name, error_dict) where error_dict is None if test passed,
               otherwise contains 'text' and 'status_code' keys
    """
    logging.info(
        f"::group::     Validating sync report {test_record['test_name']}")
    parameters = {}
    if "PROCESS_DATE" in test_record["variables"]:
        parameters["PROCESS_DATE"] = process_date
    if "JOB_NAME" in test_record["variables"]:
        parameters["JOB_NAME"] = job_name

    r = run_dbt_test(test_record["test_name"], parameters,
                     slack_alert=False, target="ascend_eod")

    error_dict = None
    if r.status_code != 200:
        error_dict = {
            "text": "ERROR IN DBT TEST EXECUTION " + r.text,
            "status_code": r.status_code
        }
    elif 'returned non-zero exit status' in r.text:
        error_dict = {
            "text": "TEST EXECUTED BUT FAILED " + r.text,
            "status_code": r.status_code
        }

    logging.info(f"::endgroup::")
    return test_record["test_name"], error_dict


def run_tests_for_asset(full_asset_name: str, sync_report_array: str, job_name: str, process_date: str) -> dict:
    """
    Runs all sync report tests for a single asset.

    Args:
        full_asset_name (str): The full name of the asset being tested
        sync_report_array (str): JSON string containing array of test configurations
        job_name (str): The name of the job for parameter substitution
        process_date (str): The process date for parameter substitution

    Returns:
        dict: Dictionary of test failures, empty if all tests passed
    """
    sync_report_array = json.loads(sync_report_array)
    logging.info(
        f"::group::Checking Sync Reports for the asset {full_asset_name}")
    errors_in_dbt_test = {}

    for record in sync_report_array:
        dhp_parameters = {}
        test_name, error = run_single_test(record, job_name, process_date)
        if error:
            errors_in_dbt_test[test_name] = error
            dhp_parameters["test_passed"] = False
        else:
            dhp_parameters["test_passed"] = True
        dhp_parameters["full_table_name"] = full_asset_name
        dhp_parameters["report_name"] = test_name
        dhp_parameters["description"] = f"Sync Report check for {test_name} for asset {full_asset_name}"
        dhp_parameters["publisher"] = "datalake@apexclearing.com"
        dhp_parameters["process_date"] = process_date
        is_dhp_publish_success, response_json = post_report_to_dhp(
            dhp_parameters)
        if not is_dhp_publish_success:
            logging.error(
                f"Failed to publish test result to DHP for test {test_name} on asset {full_asset_name}. Response: {response_json}")

    if errors_in_dbt_test:
        log_test_errors(errors_in_dbt_test)

    logging.info(f"::endgroup::")
    return errors_in_dbt_test


def log_test_errors(errors_in_dbt_test: dict) -> None:
    """
    Logs detailed error information for failed tests.

    Args:
        errors_in_dbt_test (dict): Dictionary mapping test names to error details
    """
    logging.error(
        f"Sync Reports failed for the following tests: {list(errors_in_dbt_test.keys())}")
    for test_name, error_details in errors_in_dbt_test.items():
        logging.error(f"Error in Test: {test_name}")
        logging.error(f"::group::details")
        logging.error(f"Status Code: {error_details['status_code']}")
        logging.error(f"Error Details: {error_details['text']}")
        logging.error("::endgroup::")


def publish_asset_health_status(full_asset_name: str, is_healthy: bool, process_date: str) -> None:
    """
    Publishes health certification status to DHP for an asset.

    Args:
        full_asset_name (str): The full name of the asset
        is_healthy (bool): Whether the asset passed all sync report checks
        process_date (str): The process date for the certification
    """
    dhp_dict = {
        "full_table_name": full_asset_name,
        "report_name": "data_asset_health",
        "description": "Sync Report health certification",
        "publisher": "datalake@apexclearing.com",
        "process_date": process_date,
        "is_healthy": is_healthy
    }

    status = "Healthy" if is_healthy else "Unhealthy"
    logging.info(
        f"::group::{status} state publish for Asset {full_asset_name}")
    is_dhp_publish_success, response_json = certify_asset(dhp_dict)
    if not is_dhp_publish_success:
        logging.error(
            f"Failed to publish health status to DHP for asset {full_asset_name}. Response: {response_json}")
    logging.info(f"::endgroup::")


def execute_tests(job_name: str, process_date: str) -> None:
    """
    Executes sync report validation tests for a given job and certifies asset health status.

    This function fetches all configured sync report tests for a job, executes them using dbt,
    tracks failures, and publishes health certification status to DHP (Data Health Platform)
    for each asset based on test results.

    Args:
        job_name (str): The name of the job whose sync report tests should be executed.
                       Used to filter tests and as a parameter for test execution.
        process_date (str): The process date to use when executing tests that require
                          a PROCESS_DATE parameter. Typically in YYYY-MM-DD format.

    Raises:
        AirflowFailException: If any sync report checks fail for any assets.

    Returns:
        None
    """
    logging.info(
        f"Fetching the list of tests for job_name {job_name} and receive process_date {process_date} from xcom")
    dict_of_asset_and_related_tests = fetch_dict_of_assets_and_tests_for_job(
        job_name)
    logging.info(
        f"Dict of assets and related tests fetched for job_name {job_name} is {dict_of_asset_and_related_tests}")

    if not dict_of_asset_and_related_tests:
        logging.info(
            f"No active sync report checks found for job_name {job_name}")
        return

    sync_report_asset_failures = set()

    # Run tests for each asset
    for full_asset_name, sync_report_array in dict_of_asset_and_related_tests.items():
        errors_in_dbt_test = run_tests_for_asset(
            full_asset_name, sync_report_array, job_name, process_date
        )
        if errors_in_dbt_test:
            sync_report_asset_failures.add(full_asset_name)

    # Check for failures and raise exception if any
    if sync_report_asset_failures:
        logging.error(
            f"Sync Report Checks failed for the following assets: {sync_report_asset_failures}, "
            f"please check the logs for more details on the failed tests and errors"
        )
        raise AirflowFailException(
            f"Sync Report Checks failed for the following assets: {sync_report_asset_failures}"
        )

    # Publish health status for all assets
    for full_asset_name in dict_of_asset_and_related_tests.keys():
        is_healthy = full_asset_name not in sync_report_asset_failures
        publish_asset_health_status(full_asset_name, is_healthy, process_date)


def sync_report_checks(job_name: str, process_date: str) -> PythonOperator:
    """
    Creates an Airflow PythonOperator task for executing sync report validation checks.

    This function creates and configures an Airflow task that will execute sync report
    tests for a given job. The task is configured with retry logic to handle transient
    failures.

    Args:
        job_name (str): The name of the job for which to create the sync report checks task.
                       This will be appended to the task_id and passed to execute_tests.
        process_date (str): The process date to pass to the execute_tests function.
                          Typically represents the data date being validated.

    Returns:
        PythonOperator: A configured Airflow PythonOperator that will execute the
                       sync report checks when triggered. The task includes 3 retries
                       with a 5-minute delay between attempts.
    """

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
