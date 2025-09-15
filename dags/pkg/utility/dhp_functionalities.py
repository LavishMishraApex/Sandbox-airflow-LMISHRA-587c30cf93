import requests
from dags.pkg.utility.auth_utils import get_ascend_jwt  # needs path changed
import base64
import logging
import json

from dags.pkg.utility.bigquery_functionalities import *  # needs path changed


from datetime import date, timedelta

GCP_PROJECTS = {
    "INTERNAL_HUB": "apex-internal-hub-dev-00",
    "DATALAKE_MGMT": "apex-datalake-mgmt-dev-00"
}

DHP_INTERNAL_HUB_HEALTH = "apex-internal-hub-dev-00.datalake_status.internal_hub_health"
DHP_PRE_SNAPSHOT_CONFIGURATION = "apex-datalake-mgmt-dev-00.snapshot_service.pre_snapshot_dhp_configuration"


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
# Needs to be fetched from globals
DHP_INTERNAL_HUB_HEALTH_v2 = "apex-internal-hub-dev-00.datalake_status.datalake_test_v2"


def create_sql_for_dhpv2_parameters(parameters):
    '''
    Description:-
        Takes parameters as input and creates the sql to fetch the results from DHP v2
    Input:-
        parameter: dictionary containing the parameters for which would then be used
        to create the sql w.r.t. the parameters that need to be fetched
    Returns:-
        sql: sql query to fetch the results from DHP v2
    '''
    sql = f"""SELECT test_description, is_healthy
    FROM (
    SELECT
        s.publish_time,
        s.project_id AS project_name,
        s.dataset_name,
        s.table_name,
        s.process_date,
        s.publisher,
        s.report_name AS test_name,
        test_description,
        is_healthy,
        ROW_NUMBER() OVER (
            PARTITION BY
            s.project_id,
            s.dataset_name,
            s.table_name,
            s.process_date,
            s.publisher,
            s.report_name
            ORDER BY s.publish_time DESC
        ) latest_record_identifier
    FROM
        {DHP_INTERNAL_HUB_HEALTH_v2} AS s
    Qualify latest_record_identifier = 1
    ) h
        """
    and_where_flag = False
    for key, value in parameters.items():
        if not and_where_flag:
            sql += f""" WHERE  {key} = "{value}"\n"""
            and_where_flag = True
        else:
            sql += f""" AND {key} = "{value}"\n"""
    return sql


def fetch_from_dhp_v2(parameters: dict) -> [bool, bool, dict]:
    '''
    Run Validation test to see if the view/table function is ready to be snapshotted
    '''
    sql_for_the_test = create_sql_for_dhpv2_parameters(parameters)
    logging.info(
        "Running query to check if entry exists , query: %s", sql_for_the_test)
    dhp_test_result = list(run_query(sql_for_the_test))
    logging.info("Result of query to check if entry exists len(result): %s", len(
        dhp_test_result))
    if not dhp_test_result:
        return False, False, {}
    test_description = dhp_test_result[0]['test_description'].replace(
        "'", "\"")
    test_description_json = json.loads(test_description)
    return True, dhp_test_result[0]['is_healthy'], test_description_json


def fetch_from_dhp_v1(parameters: dict) -> [bool, bool, dict]:

    query = create_sql_for_dhp_parameters(parameters)
    logging.info(
        f"Running query to check if entry exists , query: {query}")
    result = list(run_query(query))
    logging.info(
        f"Result of query to check if entry exists len(result): {len(result)}")
    if len(result) == 0:
        return False, False, {}
    else:
        test_description = result[0]['test_description'].replace("'", "\"")
    test_description_json = json.loads(
        test_description)
    return True, result[0]['is_healthy'], test_description_json


def fetch_from_dhp(parameters: dict) -> [bool, bool, dict]:
    has_data, is_healthy, test_description_json = fetch_from_dhp_v2(
        parameters)
    if has_data:
        return has_data, is_healthy, test_description_json
    else:
        return fetch_from_dhp_v1(parameters)


def check_if_entry_for_row_count_validation_exists(snapshot_job_name: str, process_date: str) -> list[bool, bool, dict]:
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
        AND use_row_for_snapshot = TRUE
    """.format(DHP_PRE_SNAPSHOT_CONFIGURATION=DHP_PRE_SNAPSHOT_CONFIGURATION, snapshot_job_name=snapshot_job_name)
    logging.info(
        f"Running query to fetch details from DHP_PRE_SNAPSHOT_CONFIGURATION for snapshot: {snapshot_job_name}, query: {query_to_fetch_details_from_dhp_configurations}")
    # need to add logic to fail the dag if we get more than one rows as part of the result
    result = list(
        run_query(query_to_fetch_details_from_dhp_configurations))
    if len(result) != 1:
        return False, f"Expected exactly one row to have use_row_for_snapshot = TRUE for snapshot {snapshot_job_name} in {DHP_PRE_SNAPSHOT_CONFIGURATION}, but got {len(result)} rows", {}
    project_name, dataset_name, table_name = result[0][
        'table_name'].split(".")
    logging.info(
        f"Table name fetched from DHP_PRE_SNAPSHOT_CONFIGURATION: {project_name}, {dataset_name}, {table_name}")
    dhp_parameters = {}
    dhp_parameters["project_name"] = project_name
    dhp_parameters["dataset_name"] = dataset_name
    dhp_parameters["table_name"] = table_name
    dhp_parameters["process_date"] = process_date
    dhp_parameters["test_name"] = test_name_to_use_in_dhp
    return True, "DHP parameters found", dhp_parameters


def fetch_dhp_test_description_for_snapshot(snapshot_job_name: str, process_date: str) -> [bool, str, dict]:
    """
    Fetches the parameters for a snapshot job from DHP based on the snapshot name and process date.
    :param snapshot_job_name: Name of the snapshot.
    :param process_date: Process date for which the parameters are to be fetched.
    :return: Dictionary containing the parameters if found, else an empty dictionary.
    """
    entry_for_row_count_validation_exists, return_message, dhp_parameters = check_if_entry_for_row_count_validation_exists(
        snapshot_job_name, process_date)
    if not entry_for_row_count_validation_exists:
        return False, return_message, {}
    return_message = "table_function_parameters found from row count validation test details"

    entry_exists, is_healthy, test_description_json = fetch_from_dhp(
        dhp_parameters)
    if not entry_exists:
        return False, "No entry found for the snapshot job in DHP", {}
    if not is_healthy:
        return False, "Entry found in DHP but is_healthy is not set to 1", {}

    return True, return_message, test_description_json


'''
changes for publish to v2
'''


DATALAKE_ASCEND_DHP_SA = "eyJrZXlJZCI6IjAxSzBXV0Q0TTBTM1BBMkc3TkQwUkpNV0RIIiwicHJpdmF0ZUtleSI6Ii0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZnSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2d3Z2dTa0FnRUFBb0lCQVFDUGxERW9VZVFyUldzUFxuUTBSODlja0FJZ0pKVlQ0L2FvbjBTL05iajN3Y0FuNUw1YUpwMm93NC84QlpyNC9jU3YvcUhiVDJqV0lkVDVjRFxuc1hPU0pzVDhzelVmT09yeEREZzVQNzJvcFFhaDJOc2VLZ0plbWZDYW9WOHo4eGJ5RDV3ME9MMXBiUTlOSklQNlxuMWEybTNPOURuQndsMXJqeHQ2NVVBQjlyOTRBTFpxN0JOUTRYS0pPMEV2KzZSblBvY0JtdUVFZU5zd0tjdTIyWlxuTG1aZFFZNVdEYTF3RU04WjdDYjI1SjEyeHBwYzYrUVNiTmN2cjJNZUZQMTBBUWFuTHgwMU8rODRraHh4N0Z1N1xuZHFodE1WMUVRNTNxNGpRU0VlV054eDdwVTlIcGVXRmo2VTkxM2pveGhLZkF1d3pYa0FnYXpvOURlZVFwUGpVQlxuZnl6ZndlNTdBZ01CQUFFQ2dnRUFXb1dkQklXMDdFOGs4NGQrbTZZK3ByWEthVCswTWpsU1p0S255TmRLOFVIbFxuTEtiSDRpTW0reHpMd2YrOUhLK3diNE54UDJ6ZUtncXU5R1locmtpQk02MHMxZFdGMHBuWXJNZHlKT3grcFBYYlxuR0VaMkhmekNSRXR2Z1lwR3NqQ0RWQzFkeGlVN1czQ2xRVFVNK2NJYm02M0YxVmx1V3Y5cWlvMVZRalhWNWRsUVxuWXNVODMyMWlYelNnenp4ME1OVWxzWXFJVHBtNlVMYTFvQ0xUcVNGbGhYUVBORkVWaFM1dURvc2o4QjRCa3B0aVxuN3pGNGFuSmQ1R1pPWnlveG9uWnk1L0VKdzhwcmNvaWJYaE9ENTEvS2thM09xUWpGejk3RThRSG1DQ3ZDQXFqWFxueEFhSkdKRWJBVVJRY1FPSjJ5eEQ1bHNOMC9vdFQwc3J4VW8rVzBRUjRRS0JnUURibll6Y3FVUFJqWTFxWWQ2S1xuSHFjNDNTUlVzQkUxRmx6UGx1YnNMZUd6YStwMWpQMTZ4MTJDb3o5NWlmenphS09NYTIvWFRIMlJleWF2QnFDZ1xuaWhuUDZsUDFzeldDK3ExZEJUTklJU1Q0L0NnbXBXNjFTdWhsZ0w0NkVUd1Zja0dlcy9TN0wwcWJvK01Cbzl6NFxuZ2UzWWdONDh5ZnNmS2l5Y2diSnYxSnFyQ3dLQmdRQ25YYnpnWkVIM3U4QlpkdnJrSDZMb1h0NncyeUQydmZ0bFxuc09QRmJRVnhpRzJHZ2hGVVJYS0phQmJ6eWZzS1BJbDhubEZuN1JjcHRCcW9DRjdnUUdMQ0hDd2ZlNmg2Q1FyNVxuL0pLYld1bnM2QURsaXhUMzZ4L2hJNDBIZ0ozeVBlUFRJMEZraC9DalUrdzQyeGFCSTVDN2NsMzhILzM3NWxPS1xuMUNLdjloZHdVUUtCZ1FDSFNZTGczQlMvSG1nalJLOEdmdU9jai80MWZWRGNWeTVOWXpSV0FkMnIzYXJOUjFGUFxuTlVsUmxLY2hnL09qTHE2eGJlMnp2NWNLNjhaa3c3eG5xU3RGZmFEREZ1YThEUmlHMlJGQ09jakE2UFVDK1o0OVxuYUN2SmU0bXowN0lqdEFMZ2RSTXB6SFExZEx2KzRxYlpINUVaY2lsMVlTZWxoeUY4T0JsbjhweGxDUUtCZ1FDaVxuQ1ZVR0Fzc0RhQmtRQk90ZTFXcEpneUFqSmVSQ1B5a1lDU3hjUmZMUk9uNmZqV250cHRiL1JYR0RVZmZrcnp1RlxuRlZwSFBmb0EvRWdhaXhBZ0dQWUViSFlqZlB0ZU8wY1BSSU5FT2I3bENMRmxpMFFmeXRvd2hOVFRnS2hxa1pUelxuSTl6NTBjc2V0ZStzRkNFem9oVk1CYXdNbjRTc3p3L3ZCdmNXV1RIVUlRS0JnR1BJVGN4SENQeXV4MjV3VEh0WVxuSEJvczN0WEdWYkpRT0JOV2JMS3I3Vkc3enowQVI2ZFdLeTgzampCMVBaSHRrSkVSbEUrbVVKTmFlVnVzR1BhYVxub21vOExPaXdldDA1V2xiUVlFa0lySHlLdm1HZzhpRWxVUC9uNldiaDBBNDFJK3EwVnNZaVIyVzJqTnVzOHlrYlxuWUhjbTl4a2kza1RTM00zajZkdEd5Y0xpXG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tIiwibmFtZSI6InN2Yy1kaHAtZGV2LWRhdGFsYWtlLW1nbXQiLCJvcmdhbml6YXRpb24iOiJmaXJtcy9BRlMiLCJ0eXBlIjoic2VydmljZUFjY291bnQifQ=="
dhpv2_url = Variable.get(
    "dhp_health_report_v2_url", default_var=f"https://data-health-report.{ENVIRONMENT}.gcp.apexclearing.com")


def publish_report_to_dhp_v2(publish_dict):
    jwt_token = get_ascend_jwt(json.loads(
        base64.b64decode(DATALAKE_ASCEND_DHP_SA)))

    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {jwt_token}"}

    logging.info("publishing following dict to DHP v2 {}".format(publish_dict))
    response = requests.post(
        dhpv2_url, headers=headers, data=json.dumps(publish_dict), timeout=300)

    logging.info(response.status_code)
    logging.info(response.text)

    if response.status_code == 200:
        logging.info("Health report published successfully.")
        return True, response
    else:
        logging.info("Failed to publish health report.")
        logging.info(f"Error: {response.status_code} - {response.text}")
        return False, response


def certify_asset(parameters):
    '''
    This function takes in a dict to certify an asset in DHP.
    This parameters should have following keys set
    1. all the minimum keys that are required to certify health of an asset, check the DHP documentation for details
    2. a separate dict with key "additional_report_details" if you want to set extra keys in the report details section of your request
    Example input

    {
        "project_id": "apex-internal-hub-dev-00",
        "report_name": "data_asset_health",
        "description": "Reports the health of a data asset",
        "publisher": "datalake@apexclearing.com",
        "dataset_name": "snapshot",
        "table_name": "daily_accounts_v2",
        "process_date": "2025-01-15",
        "is_healthy": true,

        "additional_report_details": {
            "some_key": "some_value"
        }

    }
    or
    {
        "full_table_name": "apex-internal-hub-dev-00.snapshot.daily_accounts_v2",
        "report_name": "data_asset_health",
        "description": "Reports the health of a data asset",
        "publisher": "datalake@apexclearing.com",
        "process_date": "2025-01-15",
        "is_healthy": true,

        "additional_report_details": {
            "some_key": "some_value"
        }

    }
    '''

    dhp_publish_dict = {}
    if "full_table_name" in parameters:
        project_id, dataset_name, table_name = parameters["full_table_name"].split(
            ".")
    else:
        project_id = parameters["project_id"]
        dataset_name = parameters["dataset_name"]
        table_name = parameters["table_name"]
    dhp_publish_dict["project_id"] = project_id
    dhp_publish_dict["report_name"] = "data_asset_health"
    dhp_publish_dict["description"] = parameters["description"]
    dhp_publish_dict["publisher"] = parameters["publisher"]
    dhp_publish_dict["report_details"] = {}
    dhp_publish_dict["report_details"]["dataset_name"] = dataset_name
    dhp_publish_dict["report_details"]["table_name"] = table_name
    dhp_publish_dict["report_details"]["process_date"] = parameters["process_date"]
    dhp_publish_dict["report_details"]["is_healthy"] = parameters["is_healthy"]
    if "additional_report_details" in parameters:
        for key, value in parameters["additional_report_details"].items():
            dhp_publish_dict["report_details"][key] = value
    return publish_report_to_dhp_v2(dhp_publish_dict)
