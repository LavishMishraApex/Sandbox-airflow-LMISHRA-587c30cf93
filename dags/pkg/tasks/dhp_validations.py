import logging
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowFailException
import datetime
from datetime import datetime, date, timedelta
import json

from dags.pkg.utility.bigquery_functionalities import *  # needs path changed
from dags.pkg.utility.dhp_functionalities import *  # needs path changed
from dags.pkg.utility.table_function_functionalities import *  # needs path changed


INTERNAL_HUB = "apex-internal-hub-dev-00"

# INTERNAL_HUB = GCP_PROJECTS["INTERNAL_HUB"]


def replication_validation(test_description_json, table_function_for_replication_validation):
    table_function_arguments = fetch_list_of_arguments_of_a_table_function(
        table_function_for_replication_validation)
    list_of_arguments_not_found_in_dhp = []
    asset_params = {}
    for argument in table_function_arguments:
        if argument not in test_description_json:
            list_of_arguments_not_found_in_dhp.append(argument)
        else:
            asset_params[argument] = f"{ test_description_json[argument] }"
    if list_of_arguments_not_found_in_dhp:
        return False, f"Arguments {list_of_arguments_not_found_in_dhp} not found in DHP table_function_arguments required for table function {table_function_for_replication_validation} are {table_function_arguments}  but details found in DHP are {test_description_json}"
    table_function_invocation_phrase = get_table_function_invocation_phrase(
        table_function_for_replication_validation, asset_params)
    query_for_replication_validation = f"""
    SELECT * FROM {table_function_invocation_phrase}
    """
    logging.info("query_for_replication_validation is {}".format(
        query_for_replication_validation))

    replication_validation_result = list(
        run_query(query_for_replication_validation))
    logging.info("replication_validation_result is {}".format(
        replication_validation_result))
    if replication_validation_result[0]:
        return True, f"Row count verification succeeded for table function {table_function_for_replication_validation} row_counts found are :- expected_row_count {replication_validation_result[0]['expected_row_count']} and actual_row_count {{replication_validation_result[0]['actual_row_count']}}  "
    else:
        return False, f"Row count verification failed for table function {table_function_for_replication_validation}, row counts are :- expected_row_count {replication_validation_result[0]['expected_row_count']} and actual_row_count {{replication_validation_result[0]['actual_row_count']}}  "


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
    results_for_job = {}
    all_tests_succeeded = True
    row_count_test_succeeded = True
    row_count_failure_tables_list = []
    dhp_report_parameters = {
        "description": "Certification of asset to be ready to be consumed by snapshot name {job_name}",
        "process_date": process_date,
        "publisher": "gcp-dataplatform@apexclearing.com"
    }
    dhp_publish_succeeded = True
    dhp_publish_failed_tables = []
    for row in results:
        max_id_test_name = row["max_id_test_name"]
        dhp_test_names_array = json.loads(row["dhp_test_names_array"])
        table_function_for_replication_validation = row["table_function_for_replication_validation"]
        logging.info("max_id_test_name is {} and dhp_test_names_array is {} and table_function_for_replication_validation is {}".format(
            max_id_test_name, dhp_test_names_array, table_function_for_replication_validation))
        table_name = row["table_name"]
        # split the table name into project_id, dataset_id, table_id
        project_id, dataset_id, table_id = table_name.split(".")
        parameters = {}
        parameters["project_name"] = project_id
        parameters["dataset_name"] = dataset_id
        parameters["table_name"] = table_id
        parameters["process_date"] = process_date
        results_for_table = {}
        table_tests_succeeded = True
        for test_name in dhp_test_names_array:
            parameters["test_name"] = test_name
            entry_found, is_healthy, test_description_json = fetch_from_dhp(
                parameters)  # this line was changed to fetch_from_dhp
            table_tests_succeeded = table_tests_succeeded and entry_found and is_healthy
            results_for_table[test_name] = {
                "entry_found": entry_found, "is_healthy": is_healthy, }
            if test_name == max_id_test_name and entry_found and is_healthy:
                results_for_table[test_name]["test_description_json"] = test_description_json
                test_succeeded, row_count_message = replication_validation(
                    test_description_json, table_function_for_replication_validation)
                table_tests_succeeded = table_tests_succeeded and test_succeeded
                row_count_test_succeeded = row_count_test_succeeded and test_succeeded
                results_for_table[test_name]["row_count_verification_message"] = row_count_message
                if not test_succeeded:
                    row_count_failure_tables_list.append(table_name)
        all_tests_succeeded = all_tests_succeeded and table_tests_succeeded
        # publishing results to DHP v2
        dhp_report_parameters["full_table_name"] = table_name
        dhp_report_parameters["is_healthy"] = table_tests_succeeded
        is_dhp_publish_success, response_json = certify_asset(
            dhp_report_parameters)
        dhp_publish_succeeded = dhp_publish_succeeded and is_dhp_publish_success
        if not is_dhp_publish_success:
            dhp_publish_failed_tables.append(table_name)
            logging.info("Failed to publish DHP report for table {}, status_code is {}, response is {}".format(
                table_name, response_json.status_code, response_json.text))
        results_for_job[table_name] = results_for_table

    logging.info("results_for_job are {}".format(results_for_job))
    logging.info("all_tests_succeeded is {}".format(all_tests_succeeded))
    if not all_tests_succeeded:
        raise AirflowException(
            "One or more tests failed for job_name {}".format(job_name))

    if not row_count_test_succeeded:
        raise AirflowException(
            f"row count validation failed for the following tables, please check their status {row_count_failure_tables_list} for job_name {job_name}")
    if not dhp_publish_succeeded:
        raise AirflowFailException(
            f"Failed to publish health report to DHP v2 for the following tables, please check their status {dhp_publish_failed_tables} for job_name {job_name}")


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
