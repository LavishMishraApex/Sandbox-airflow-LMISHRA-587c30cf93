import logging
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowFailException
import datetime
from datetime import datetime, date, timedelta
import json

from dags.pkg.utility.bigquery_functionalities import *  # needs path changed
from dags.pkg.utility.dhp_functionalities import *  # needs path changed
from dags.pkg.utility.table_function_functionalities import *  # needs path changed
from dags.pkg.utility.dbt_functionalities import *  # needs path changed


INTERNAL_HUB = "apex-internal-hub-dev-00"

# INTERNAL_HUB = GCP_PROJECTS["INTERNAL_HUB"]


def replication_validation(test_description_json, dbt_test_name):

    parameters = {"source_asset_params": test_description_json}
    r = run_dbt_test(dbt_test_name, parameters, slack_alert=False)
    if r.status_code != 200:
        logging.info(
            f"DBT test {dbt_test_name} failed with status code {r.status_code} and response {r.text}")
        return False, f"Row count verification failed for dbt test {dbt_test_name}"
    elif 'returned non-zero exit status' in r.text:
        logging.info(
            f"DBT test {dbt_test_name} failed with status code {r.status_code} and response {r.text}")
        return False, f"Row count verification failed for dbt test {dbt_test_name}"

    return True, f"Row count verification succeeded for dbt test {dbt_test_name}  "


def validate_dhp_tests_for_job(job_name: str, process_date: str, results: list):
    '''
    Description:-
        Takes job_name, process_date and results as input , creates the sql for each test and validates the results fetched from DHP
        This function will run all the tests mentioned in the configuration and raise an AirflowException if any one of the tests fail
    params:-
        job_name: job_name for which the tests need to be run, example: 'latest_assets'
        process_date: process_date for which we are checking DHP statuse.g. '2021-01-01'
        results:- list of rows where each row has a single table_name and multiple test_names associated with it(in form of an array) as fetched from Bigquery Configurations of DHP tests to be evaluated
    returns:- None
        This function raises an AirflowException if any of the tests fail
    '''
    dhp_report_parameters = {
        "description": "Certification of asset to be ready to be consumed by snapshot name {job_name}",
        "process_date": process_date,
        "publisher": "gcp-dataplatform@apexclearing.com"
    }
    failed_tables = []
    all_tables_succeeded = True
    for row in results:
        max_id_test_name = row["max_id_test_name"]
        dbt_test_name_for_replication_validation = row["dbt_test_name_for_replication_validation"]
        dhp_test_names_array = json.loads(row["dhp_test_names_array"])
        table_name = row["table_name"]
        # split the table name into project_id, dataset_id, table_id
        project_id, dataset_id, table_id = table_name.split(".")
        parameters = {}
        parameters["project_name"] = project_id
        parameters["dataset_name"] = dataset_id
        parameters["table_name"] = table_id
        parameters["process_date"] = process_date
        logging.info(
            "::group:: validations for table_name {}".format(table_name))
        logging.info("max_id_test_name is {} and dhp_test_names_array is {} and dbt_test_name_for_replication_validation is {}".format(
            max_id_test_name, dhp_test_names_array, dbt_test_name_for_replication_validation))
        results_for_table = {}
        table_tests_succeeded = True
        results_for_table = {}
        results_for_table["test_status_fetched_from_dhp"] = {}
        results_for_table["row_count_validation"] = {}
        results_for_table["dhp_report_status"] = {}
        for test_name in dhp_test_names_array:
            parameters["test_name"] = test_name
            entry_found, is_healthy, test_description_json = fetch_from_dhp(
                parameters)  # this line was changed to fetch_from_dhp
            table_tests_succeeded = table_tests_succeeded and entry_found and is_healthy
            results_for_table["test_status_fetched_from_dhp"][test_name] = {
                "entry_found": entry_found, "is_healthy": is_healthy, }
            if test_name == max_id_test_name and entry_found and is_healthy:
                results_for_table["row_count_validation"]["test_name"] = test_name
                results_for_table["row_count_validation"]["test_description_json"] = test_description_json
                test_succeeded, row_count_message = replication_validation(
                    test_description_json, dbt_test_name_for_replication_validation)
                table_tests_succeeded = table_tests_succeeded and test_succeeded
                results_for_table["row_count_validation"]["test_succeeded"] = test_succeeded
                results_for_table["row_count_validation"]["row_count_verification_message"] = row_count_message
                if not test_succeeded:
                    logging.info(
                        "failed to validate row counts : {}".format(row_count_message))
        # publishing results to DHP v2
        dhp_report_parameters["full_table_name"] = table_name
        dhp_report_parameters["is_healthy"] = table_tests_succeeded
        is_dhp_publish_success, response_json = certify_asset(
            dhp_report_parameters)
        results_for_table["dhp_report_status"]["is_dhp_publish_success"] = is_dhp_publish_success
        results_for_table["dhp_report_status"]["response_json"] = response_json.json(
        )
        table_tests_succeeded = table_tests_succeeded and is_dhp_publish_success
        if not is_dhp_publish_success:
            logging.info("Failed to publish DHP report for table {}, status_code is {}, response is {}".format(
                table_name, response_json.status_code, response_json.text))
        all_tables_succeeded = all_tables_succeeded and table_tests_succeeded
        logging.info("results_for_table for table_name {} is {}".format(
            table_name, results_for_table))
        logging.error("::endgroup::")
        if not table_tests_succeeded:
            failed_tables.append(table_name)
    logging.info("all_tables_succeeded is {}".format(all_tables_succeeded))
    if not all_tables_succeeded:
        raise AirflowException(
            "One or more tests tables for job_name {}, list of tables failed is {}".format(job_name, failed_tables))


def check_dhp_status(**kwargs):
    """
       Takes job_name and process_date as arguments, finds the list of tests associated with the job_name by callilng fetch_list_of_tests_for_job function and calls validate_dhp_tests_for_job function to validate all the tests

       :param job_name: job_name for which the tests need to be run, example: 'latest_assets'
       :param process_date: e.g. '2021-01-01'
       :return: None, raises AirflowException if any of the tests fail from within validate_dhp_tests_for_job function
       """
    job_name = kwargs.get("job_name")
    logging.info("job_name is {}".format(job_name))
    process_date = kwargs.get("process_date")
    test_configurations_for_job = fetch_list_of_tests_for_job(
        job_name)
    if test_configurations_for_job == []:
        logging.info(
            "No tests found for job_name {}, if you feel there should be DHP validations for this job, please update the configurations ".format(job_name))
        return
    validate_dhp_tests_for_job(
        job_name, process_date, test_configurations_for_job)


def dhp_validations(job_name: str, process_date: str) -> PythonOperator:
    dhp_validations_task = PythonOperator(
        task_id="dhp_validations_"+job_name,
        python_callable=check_dhp_status,
        op_kwargs={
            "job_name": job_name,
            "process_date": process_date
        },
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    return dhp_validations_task
