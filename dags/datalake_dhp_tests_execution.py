import json
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context

# from ascend.datalake.config.globals import CLOUDRUN_URL, ENVIRONMENT
from airflow.exceptions import AirflowException
from pkg.utility import get_id_token
from pkg.utility.bigquery_functionalities import *

from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

import datetime
import requests
import logging
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator

# need to fetch Datalake MGMT project and internal_hub health project

CLOUDRUN_URL = "https://dbt-apex-datalake-h3n6ulr52q-uc.a.run.app"
ENVIRONMENT = "dev"


def fetch_list_of_tests_for_job(job_name: str):
    '''
        Description:-
            Takes job_name as input and fetches the list of dbt tests associated with the job_name w.r.t the tables its dependent on
        params 
            job_name: job_name for which the tests need to be fetched, example: 'latest_assets'
        returns:-
            list of rows where each row has a single table_name and multiple test_names associated with it(in form of an array)
    '''
    DATALAKE_MGMT = "apex-datalake-mgmt-dev-00"
    bigquery_configurations_table_name = "{}.snapshot_service.pre_snapshot_dhp_configuration".format(
        DATALAKE_MGMT)  # define how this table would be stored
    query_string = f"""
        SELECT * from `{bigquery_configurations_table_name}` where job_name = '{job_name}'
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
                `apex-internal-hub-dev-00.datalake_status.internal_hub_health`  
    """
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
    return check_results(dhp_test_result)


def validate_dhp_tests_for_job(job_name: str, process_date: str, results: list):
    '''
    Description:-
        Takes job_name, process_date and results as input , creates the sql for each test and validates the results fetched from DHP
        This function will process first run all the tests mentioned in the configuration and raise an AirflowException if any one of the tests fail
    params:-
        job_name: job_name for which the tests need to be run, example: 'latest_assets'
        process_date: process_date for which we are checking DHP statuse.g. '2021-01-01'
        results:- list of rows where each row has a single table_name and multiple test_names associated with it(in form of an array) as fetched from Bigquery Configurations of DHP tests to be evaluated
    returns:- None
        This function raises an AirflowException if any of the tests fail
    '''
    results_for_job = {}
    all_tests_succeeded = True
    for row in results:
        max_id_test_name = row["max_id_test_name"]
        dhp_test_names_array = json.loads(row["dhp_test_names_array"])
        logging.info("max_id_test_name is {} and dhp_test_names_array is {}".format(
            max_id_test_name, dhp_test_names_array))
        table_name = row["table_name"]
        # split the table name into project_id, dataset_id, table_id
        project_id, dataset_id, table_id = table_name.split(".")
        parameters = {}
        parameters["project_name"] = project_id
        parameters["dataset_name"] = dataset_id
        parameters["table_name"] = table_id
        parameters["process_date"] = process_date
        results_for_table = {}
        for test_name in dhp_test_names_array:
            parameters["test_name"] = test_name
            test_succeeded, message = validate_dhp_test(parameters)
            all_tests_succeeded = all_tests_succeeded and test_succeeded
            results_for_table[test_name] = {
                "test_succeeded": test_succeeded, "message": message}
            if test_name == max_id_test_name:
                pass  # need to update this to fetch and store max_id details for today's process_date and yesterday's process_date
        results_for_job[table_name] = results_for_table
    logging.info("results_for_job are {}".format(results_for_job))
    logging.info("all_tests_succeeded is {}".format(all_tests_succeeded))
    if not all_tests_succeeded:
        raise AirflowException(
            "One or more tests failed for job_name {}".format(job_name))


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


def check_dhp_status(**kwargs):
    """
       Takes job_name and process_date as arguments, finds the list of tests associated with the job_name by callilng fetch_list_of_tests_for_job function and calls validate_dhp_tests_for_job function to validate all the tests

       :param job_name: job_name for which the tests need to be run, example: 'latest_assets'
       :param process_date: e.g. '2021-01-01'
       :return: None, raises AirflowException if any of the tests fail from within validate_dhp_tests_for_job function
       """
    # parameters = kwargs.get("parameters")
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
        jobs_list = ["latest_prices", "latest_assets", "daily_accounts"]
        for job_name in jobs_list:
            datalake_dhp_test = PythonOperator(
                task_id="datalake_dhp_test_{}".format(job_name),
                python_callable=check_dhp_status,
                op_kwargs={"job_name": job_name,
                           "process_date": "2025-02-04"
                           },
                dag=dag,
                provide_context=True,
            )

        return dag


dag_id = "datalake_dhp_tests_execution"
schedule = None
dag = create_dag(dag_id, schedule)


'''
To trigger the Dag, currently we are using the parameters
    {"project_name": "apex-assets-dev-00",
                                      "dataset_name": "feeder",
                                      "table_name": "apexinternal_assets_v1_asset_apexinternal_assets_v1_asset",
                                      "process_date": "2025-01-28"
                                      }
To test null results, we can update the parameter process_date to be some date in future, maybe something in 2026
To test unhealthy results, we need to post a fake entry to DHP which has unhealthy status

curl command used to make tests pass
curl -m 70 -X POST "https://health-report-ddnbjbky7q-uc.a.run.app" \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $(gcloud auth print-identity-token)" \
-d '{
     "project_name": "apex-assets-dev-00",
    "dataset_name": "feeder",
    "table_name": "apexinternal_assets_v1_asset_apexinternal_assets_v1_asset",
	"process_date": "2025-02-04",	
    "tester_email_address": "lmishra@apexfintechsolutions.com",    
	"test_name": "assets_assets_row_count_validation",
    "test_description": "{\"max_id\":\"10761\",\"column_name\":\"id\"}",   	
	"is_healthy": 1,    
    "report_timestamp": "2025-02-04 16:00:00.000"    
}'


curl -m 70 -X POST "https://health-report-ddnbjbky7q-uc.a.run.app" \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $(gcloud auth print-identity-token)" \
-d '{
     "project_name": "apex-assets-dev-00",
    "dataset_name": "feeder",
    "table_name": "apexinternal_assets_v1_asset_apexinternal_assets_v1_asset",
	"process_date": "2025-02-04",	
    "tester_email_address": "lmishra@apexfintechsolutions.com",    
	"test_name": "assets_assets_row_count_validation_v2",
    "test_description": "{\"max_id\":\"10761\",\"column_name\":\"id\"}",   	
	"is_healthy": 0,    
    "report_timestamp": "2025-02-04 16:27:00.000"    
}'
curl -m 70 -X POST "https://health-report-ddnbjbky7q-uc.a.run.app" \
-H "Content-Type: application/json" \
-H "Authorization: Bearer $(gcloud auth print-identity-token)" \
-d '{
     "project_name": "apex-assets-dev-00",
    "dataset_name": "feeder",
    "table_name": "apexinternal_assets_v1_price_apexinternal_assets_v1_price",
	"process_date": "2025-02-04",	
    "tester_email_address": "lmishra@apexfintechsolutions.com",    
	"test_name": "assets_prices_row_count_validation",
    "test_description": "{\"max_id\":\"10761\",\"column_name\":\"id\"}",   	
	"is_healthy": 1,    
    "report_timestamp": "2025-02-04 16:00:00.000"    
}'

{
    "project_name": "apex-assets-dev-00",
    "dataset_name": "feeder",
    "table_name": "apexinternal_assets_v1_price_apexinternal_assets_v1_price",
    "process_date": "2025-01-15",
    "tester_email_address": "assets-svc-validator@apexclearing.com",
    "test_name": "ODS_replication_validation",
    "test_description": "{'max_id':'10761','column_name':'sequence_id'}",    
	"is_healthy": "1",
    "report_timestamp": "2025-01-15 20:30:00.000"    
}
'''
