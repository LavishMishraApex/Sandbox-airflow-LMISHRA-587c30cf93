import logging
import json

from airflow.exceptions import AirflowFailException
from dags.pkg.utility.bigquery_functionalities import *  # needs path changed


from datetime import date, timedelta


def fetch_data_from_test_description(test_description):
    test_description = test_description.replace("'", "\"")
    test_description_current_json = json.loads(
        test_description)
    return test_description_current_json["max_id"], test_description_current_json["column_name"], test_description_current_json["row_count"]


def fetch_list_of_tests_for_job(job_name: str):
    '''
        Description:-
            Takes job_name as input and fetches the list of dbt tests associated with the job_name w.r.t the tables its dependent on
        params 
            job_name: job_name for which the tests need to be fetched, example: 'latest_assets'
        returns:-
            list of rows where each row has a single table_name and multiple test_names associated with it(in form of an array)
    '''
    DATALAKE_MGMT = GCP_PROJECTS["DATALAKE_MGMT"]
    bigquery_configurations_table_name = "{}.snapshot_service.pre_snapshot_dhp_configuration".format(
        DATALAKE_MGMT)  # define how this table would be stored
    query_string = f"""
        SELECT * from `{bigquery_configurations_table_name}` where job_name = '{job_name}' and is_active = TRUE
    """
    logging.info("executing query {}".format(query_string))
    results = list(run_query(query_string))
    logging.info(
        "results fetched from pre_snapshot_dhp_configuration for job_name {}".format(job_name))
    logging.info(results)
    return results


def create_sql_for_dhp_parameters(parameters):
    '''
    Description:-
        Takes parameters as input and creates the sql to fetch the results from DHP
    Input:-
        paramerter: dictionary containing the parameters for which would then be used to create the sql w.r.t. the parameters that need to be fetched
    Returns:-
        sql: sql query to fetch the results from DHP
    '''
    sql = """   SELECT * FROM 
                {}  
    """.format(DHP_INTERNAL_HUB_HEALTH)
    and_where_flag = False
    for key, value in parameters.items():
        if not and_where_flag:
            sql += f""" WHERE  {key} = "{value}"\n"""
            and_where_flag = True
        else:
            sql += f""" AND {key} = "{value}"\n"""
    return sql


def validate_dhp_test(parameters):
    sql_for_the_test = create_sql_for_dhp_parameters(parameters)
    logging.info("sql_for_the_test is {}".format(sql_for_the_test))
    dhp_test_result = list(run_query(sql_for_the_test))
    logging.info(
        "results fetched from DHP for parameters {}".format(parameters))
    logging.info(dhp_test_result)
    test_succeeded, message = check_results(dhp_test_result)
    return test_succeeded, message, dhp_test_result


def check_results(results):
    '''
    Description:-
        This function is solely used to check the results fetched from DHP and validate if the tests have passed or failed
    params:-
        results: results fetched from DHP for a particular test
    returns:-
        test_succeeded: boolean value indicating if the test has passed or failed
        message: message adding description about the status of test results
    '''
    test_succeeded = False
    if results == []:
        return test_succeeded, "No results found"
    elif results[0]["is_healthy"] != "1":
        return test_succeeded, "Test has a unhealthy status, is_healthy is not set to 1"
    else:
        test_succeeded = True
        return test_succeeded, "Test has a successful status"


def find_past_date(current_date: str, number_of_days_to_subtract: int):
    """
    This function takes in current_date in str format, the number of days as input and returns the date in the format YYYY-MM-DD by subtracting the date with number_of_days_to_update
    Parameters:-
        current_date: str format date, e.g. '2025-01-19'
        number_of_days_to_subtract: int, number of days to subtract from the current_date
    Returns:-
        Date in the format YYYY-MM-DD, e.g. '2025-01-18' after subtracting number_of_days_to_subtract days from '2025-01-19'
    """
    current_date = date.fromisoformat(current_date)
    yesterday_date = current_date - timedelta(days=number_of_days_to_subtract)
    yesterday_date = yesterday_date.strftime('%Y-%m-%d')
    return yesterday_date


def find_table_function_parameters(job_name: str, process_date) -> dict:
    '''
    Description:-
        This function retrieves the details from DHP based on the job_name and process_date
    params:-
        job_name: name of the job for which the details are to be fetched
        process_date: process_date for which the details are to be fetched
    returns:-
        dhp_parameters: dictionary containing the details fetched from DHP
    '''
    table_function_parameters = {}
    test_configurations_for_job = fetch_list_of_tests_for_job(
        job_name)
    if test_configurations_for_job == []:
        logging.info(
            "No tests found for job_name {}, if you feel there should be DHP validations for this job, please update the configurations ".format(job_name))
        return {}
    # We are only considering the first table configured in the job and thus the paremeters from there would be used
    row = test_configurations_for_job[0]

    parameters = {}
    table_name = row["table_name"]
    project_id, dataset_id, table_id = table_name.split(".")

    parameters["process_date"] = process_date
    parameters["project_name"] = project_id
    parameters["dataset_name"] = dataset_id
    parameters["table_name"] = table_id
    parameters["test_name"] = row["max_id_test_name"]
    test_succeeded, message, dhp_test_result = validate_dhp_test(
        parameters)
    if not test_succeeded:
        return {}
    test_description_current_string = dhp_test_result[0]["test_description"]

    max_id_current, column_to_check, _ = fetch_data_from_test_description(
        test_description_current_string)
    yesterday_date = find_past_date(process_date, 1)
    parameters["process_date"] = yesterday_date
    test_succeeded, message, dhp_test_result_yesterday = validate_dhp_test(
        parameters)
    if not test_succeeded:
        return {}
    test_description_yesterday_string = dhp_test_result_yesterday[0]["test_description"]
    max_id_yesterday, _, _ = fetch_data_from_test_description(
        test_description_yesterday_string)
    table_function_parameters["max_id"] = max_id_current
    table_function_parameters["max_id_column"] = column_to_check
    table_function_parameters["min_id"] = max_id_yesterday
    return table_function_parameters


'''
functions related to DHP fetch for generalized table functions call
'''

# Needs to be fetched from globals
ACTIVE_SNAPSHOT_JOBS_TABLE = "apex-internal-hub-dev-00.common.active_snapshot_jobs"

# Needs to be fetched from globals
DHP_INTERNAL_HUB_HEALTH = "apex-internal-hub-dev-00.datalake_status.internal_hub_health"

# Needs to be fetched from globals
DHP_PRE_SNAPSHOT_CONFIGURATION = "apex-datalake-mgmt-dev-00.snapshot_service.pre_snapshot_dhp_configuration"


def check_if_entry_for_internal_hub_view_exists(snapshot_job_name: str, process_date: str) -> list[bool, str]:
    """
    Checks if an entry for the internal hub view exists for the given snapshot name.
    :param snapshot_job_name: Name of the snapshot.
    :return: True, test_description if entry exists, else False, empty string.''
    """

    test_name_to_use_in_dhp = "Snapshot_source_validation"
    query_to_fetch_details_from_snapshot_jobs = """
        SELECT source_table.project_var as project_var, source_table.schema_name as schema_name, source_table.table_name as table_name
        FROM {ACTIVE_SNAPSHOT_JOBS_TABLE}
        WHERE job_name = '{snapshot_job_name}'
    """.format(ACTIVE_SNAPSHOT_JOBS_TABLE=ACTIVE_SNAPSHOT_JOBS_TABLE, snapshot_job_name=snapshot_job_name)
    logging.info(
        f"Running query to fetch details from active_snapshot_jobs for snapshot: {snapshot_job_name}, query: {query_to_fetch_details_from_snapshot_jobs}")
    result = list(run_query(query_to_fetch_details_from_snapshot_jobs))
    project_name, dataset_name, table_name = result[0][
        'project_var'], result[0]['schema_name'], snapshot_job_name

    logging.info(
        f"Source view name fetched from active_snapshot_jobs: {project_name}, {dataset_name}, {table_name}")

    query = f"""
        SELECT *
        FROM `{DHP_INTERNAL_HUB_HEALTH}`
        WHERE project_name = '{project_name}'
        AND dataset_name = '{dataset_name}'
        AND table_name = '{table_name}'
        AND process_date = '{process_date}'
        AND test_name = '{test_name_to_use_in_dhp}'
    """.format(
        project_name=project_name, dataset_name=dataset_name, table_name=table_name, process_date=process_date)
    logging.info(
        f"Running query to check if entry exists for snapshot: {snapshot_job_name}, query: {query}")
    result = list(run_query(query))
    logging.info(
        f"Result of query to check if entry exists for snapshot: {snapshot_job_name}, len(result): {len(result)}")
    if len(result) == 0:
        return False, ''
    else:
        return True, result[0]['test_description']


def check_if_entry_for_row_count_validation_exists(snapshot_job_name: str, process_date: str) -> list[bool, str]:
    """
    Checks if an entry for the internal hub view exists for the given snapshot name.
    :param snapshot_job_name: Name of the snapshot.
    :param process_date: Process date for which the entry is to be checked.
    :return: True, test_description if entry exists, else False, empty string.''
    """

    test_name_to_use_in_dhp = "ODS_replication_validation"
    query_to_fetch_details_from_dhp_configurations = """
        SELECT table_name
        FROM {DHP_PRE_SNAPSHOT_CONFIGURATION}
        WHERE job_name = '{snapshot_job_name}'
    """.format(DHP_PRE_SNAPSHOT_CONFIGURATION=DHP_PRE_SNAPSHOT_CONFIGURATION, snapshot_job_name=snapshot_job_name)
    logging.info(
        f"Running query to fetch details from DHP_PRE_SNAPSHOT_CONFIGURATION for snapshot: {snapshot_job_name}, query: {query_to_fetch_details_from_dhp_configurations}")
    # need to add logic to fail the dag if we get more than one rows as part of the result
    result = list(run_query(query_to_fetch_details_from_dhp_configurations))
    project_name, dataset_name, table_name = result[0][
        'table_name'].split(".")

    logging.info(
        f"Source view name fetched from DHP_PRE_SNAPSHOT_CONFIGURATION: {project_name}, {dataset_name}, {table_name}")

    query = f"""
        SELECT *
        FROM `{DHP_INTERNAL_HUB_HEALTH}`
        WHERE project_name = '{project_name}'
        AND dataset_name = '{dataset_name}'
        AND table_name = '{table_name}'
        AND process_date = '{process_date}'
        AND test_name = '{test_name_to_use_in_dhp}'
    """.format(
        project_name=project_name, dataset_name=dataset_name, table_name=table_name, process_date=process_date)
    logging.info(
        f"Running query to check if entry exists for snapshot: {snapshot_job_name}, query: {query}")
    result = list(run_query(query))
    logging.info(
        f"Result of query to check if entry exists for snapshot: {snapshot_job_name}, len(result): {len(result)}")
    if len(result) == 0:
        return False, ''
    else:
        return True, result[0]['test_description']


def fetch_table_function_parameters_for_snapshot(snapshot_job_name: str, process_date: str, table_function_arguments: list) -> [bool, str, dict]:
    """
    Fetches the parameters for a snapshot job from DHP based on the snapshot name and process date.
    :param snapshot_job_name: Name of the snapshot.
    :param process_date: Process date for which the parameters are to be fetched.
    :return: Dictionary containing the parameters if found, else an empty dictionary.
    """
    return_message = "Table Function arguments found for IHV message"
    internal_hub_entry_exists, test_description = check_if_entry_for_internal_hub_view_exists(
        snapshot_job_name, process_date)

    if not internal_hub_entry_exists:
        logging.warning(
            f"No internal hub entry found for snapshot {snapshot_job_name} with process_date {process_date}")
        entry_for_row_count_validation_exists, test_description = check_if_entry_for_row_count_validation_exists(
            snapshot_job_name, process_date)
        return_message = "table_function_parameters found from row count validation test details"
        if not entry_for_row_count_validation_exists:
            return False, "No entry found for the snapshot job in DHP", {}
    test_description = test_description.replace("'", "\"")
    test_description_json = json.loads(
        test_description)
    list_of_arguments_not_found_in_dhp = []
    asset_params = {}
    for argument in table_function_arguments:
        if argument not in test_description_json:
            list_of_arguments_not_found_in_dhp.append(argument)
        else:
            asset_params[argument] = f"{ test_description_json[argument] }"
    if list_of_arguments_not_found_in_dhp:
        return False, f"Arguments {list_of_arguments_not_found_in_dhp} not found in DHP for snapshot {snapshot_job_name} with process_date {process_date}", {}
    return True, return_message, asset_params
