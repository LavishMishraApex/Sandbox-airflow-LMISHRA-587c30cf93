from airflow.models import Variable
from . import get_impersonated_creds
from google.cloud import bigquery
import google
import json
import io
from google.cloud.exceptions import NotFound
'''
This module has a Bigquery Client object defined at the end with the name bq_client, which is used to interact with BigQuery.
This module can be directly ingested in any DAG file and the variable bq_client can be used to maintain a single bq_client object during the lifecycle of the DAG to use the BigQuery functionalities defined in the module.
Feel free to import the package as 
from ascend.datalake.pkg.utility.bigquery_functionalities import *

'''

scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]
ENVIRONMENT = "dev"
AIRFLOW_SA = f"airflow@apex-airflow-{ENVIRONMENT}-00.iam.gserviceaccount.com"


def make_bigquery_client():
    MGMT_PROJECT = Variable.get("DATALAKE_MGMT")
    creds = get_impersonated_creds(AIRFLOW_SA, target_scopes=scopes)
    bq_client = bigquery.Client(credentials=creds, project=MGMT_PROJECT)
    return bq_client


def run_query(query_string: str) -> google.cloud.bigquery.job.query.QueryJob:
    """
    Generic function to run a query in BigQuery
    Parameters:-
        query_string:- query that needs to be executed against bq_client object
    Returns:-
        google.cloud.bigquery.job.query.QueryJob object which has the results of the query as an iterator
    This function takes in a query string as an argument and returns the result of execution of the query against bq_client.
    """
    result = (
        bq_client.query(query_string).result()
    )

    return result


def list_full_dataset_ids(project_id):
    '''
    Generic function to list all the dataset ids in a project
    Parameters:-
        project_id:- project_id for which the dataset ids need to be listed
    Returns:-
        full_dataset_ids_list:- list of all the dataset ids in the project, where each dataset id is in the format project_id.dataset_id
    '''
    full_dataset_ids_list = [
        project_id + "." + x.dataset_id for x in list(bq_client.list_datasets(project_id))]
    return full_dataset_ids_list


def list_all_tables(full_dataset_id):
    '''
    Generic function to list all the tables in a dataset
    Parameters:-
        full_dataset_id:- full_dataset_id for which the tables need to be listed
    Returns:-
        table_ids_list:- list of all the table ids in the dataset, where each table id is in the format table_id only
    '''
    tables = bq_client.list_tables(full_dataset_id)
    return [table.table_id for table in tables]


def create_dict_from_list(schema_list: list, prefix: str, schema_dict: dict):
    '''
    Parameters:
        schema_list : schema of a table in the format of a list of dictionary, where each dictionary represent details for a column
        prefix : any prefix that needs to be added to the names of the columns, this would be utilized when we have a record column to call function recursively with the parent column name as prefix
        schema_dict : final dict that would hold the key value as the
    Returns :
        returns a dictionary of the schema of the table, where key is the full_name of the column and value is a dictonary which holds all other attributes as is
    This function convert the list to a dictionary of key value pair
    '''
    for column_array in schema_list:
        if column_array['type'] == 'RECORD':
            create_dict_from_list(
                column_array['fields'], prefix+column_array["name"]+".", schema_dict)
            del column_array['fields']
        schema_dict[prefix+column_array['name']] = column_array


def fetch_table_schema(bq_table: google.cloud.bigquery.table.Table) -> json:
    """

    Given a table, calls the BigQuery API to get its schema in an array.
    Refer to Google documentation for details.
    Parameters:
        bq_table: BigQuery table object
    Returns:

    :param table: table to get schema for
    :return: list of column dictionaries
    """
    schema_string = io.StringIO("")
    bq_client.schema_to_json(bq_table.schema, schema_string)
    schema_list = json.loads(schema_string.getvalue())
    return schema_list


def fetch_table_schema_with_full_qualified_column_names_and_table_type(full_table_id: str) -> dict:
    '''
    Parameters:
        full_table_id : full table name in the format project_id.dataset_id.table_id
    Returns:
        schema_dict : dictionary of the schema of the table, where key is the full_name of the column and value is a dictonary which holds all other attributes as is
    This function fetches the schema of the table and converts it to a dictionary of values, The value this function provides would be to hold the schema as
    '''
    try:
        bq_table = bq_client.get_table(
            full_table_id)  # This object can be checked for type of the table
        schema_arr = fetch_table_schema(bq_table)
        schema_dict = {}
        create_dict_from_list(schema_arr, "", schema_dict)
        return schema_dict, bq_table.table_type
    except NotFound as e:
        return {}, ""


def are_dict_exactly_same(dict1, dict2):
    keys1 = dict1.keys()
    keys2 = dict2.keys()
    if keys1 != keys2:
        return False
        # print(keys1,keys2,"differ")
    for key in keys1:
        if dict1[key] != dict2[key]:
            # print("dicts differ on key", key)
            return False
    # print("dicts exactly same")
    return True


def update_data_for_table(bq_table, rows, query_to_select_current_data, additional_columns):
    result = list(run_query(query_to_select_current_data))
    result = [dict(row['json_output']) for row in result]
    # rint(result)
    result_dict = {}
    for row in result:
        result_dict[row['alias']] = row
    to_be_updated_rows = [r for r in rows if (
        r['alias'] not in result_dict) or not are_dict_exactly_same(r, result_dict[r['alias']])]
    for row in to_be_updated_rows:
        for key, value in additional_columns.items():
            row[key] = value
    if len(to_be_updated_rows) > 0:
        table_name = bq_table.project + "." + \
            bq_table.dataset_id + "." + bq_table.table_id
        # print(f"inserting into the table {table_name} rows", rows)
        # might need to handle the errors
        errors = bq_client.insert_rows(bq_table, to_be_updated_rows)
        print(f"""called BQ API to insert data {
              to_be_updated_rows} into table {table_name}""")
        return errors
    return []


def insert_into_table(bq_table: google.cloud.bigquery.table.Table, rows: list) -> None:
    '''
    This function is used to insert rows into a table in BigQuery
    Parameters:
        bq_table : BigQuery table object
        rows : list of rows that needs to be inserted into the table
    Returns:
        Empty list if table is updated successfully, else list of errors
    '''
    table_name = bq_table.project + "." + \
        bq_table.dataset_id + "." + bq_table.table_id
    # print(f"inserting into the table {table_name} rows", rows)
    # might need to handle the errors
    errors = bq_client.insert_rows(bq_table, rows)
    # print(f"errors are {errors}")
    # print("inserted all rows")
    return errors


bq_client = make_bigquery_client()
