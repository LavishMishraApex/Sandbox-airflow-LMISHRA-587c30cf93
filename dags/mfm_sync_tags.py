from  pkg.utility import get_id_token
from  pkg.utility import get_impersonated_creds
from airflow import DAG
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
from  config.globals import AIRFLOW_SA
import json
import google.auth.transport.requests
import google.oauth2.id_token
import logging
import requests
import pandas as pd
import time
from requests.packages.urllib3.util.retry import Retry

CREATE_OR_REPLACE_TABLE_CF_URL = Variable.get("cf_url_create_or_replace_table")
MFM_PROJECTS = Variable.get("mfm_projects_sync_tags", deserialize_json=True)
MGMT_PROJECT = Variable.get("DATALAKE_MGMT")
gsheet_to_native = {
    f"{MGMT_PROJECT}.data_catalog.mfm_field_properties": f"{MGMT_PROJECT}.data_catalog.mfm_field_properties_native",
    f"{MGMT_PROJECT}.data_catalog.mfm_alias_field_map": f"{MGMT_PROJECT}.data_catalog.mfm_alias_field_map_native",
    f"{MGMT_PROJECT}.data_catalog.mfm_overrides": f"{MGMT_PROJECT}.data_catalog.mfm_overrides_native",
}

native_to_snapshot = {
    f"{MGMT_PROJECT}.data_catalog.mfm_masterlist": f"{MGMT_PROJECT}.data_catalog.mfm_masterlist_snapshot",
    f"{MGMT_PROJECT}.data_catalog.mfm_overrides_native": f"{MGMT_PROJECT}.data_catalog.mfm_overrides_snapshot",
}
scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]
'''
creds, dummy = google.auth.default(
	scopes=scopes
)
'''
creds = get_impersonated_creds(AIRFLOW_SA, target_scopes=scopes)
time_dict = {}


def trigger_cloud_function(cf_url, cloud_function_input={}):
    headers = {
        "Authorization": f"Bearer {get_id_token(cf_url)}",
        "Content-Type": "application/json",
    }
    http_response = requests.post(cf_url, json=cloud_function_input, headers=headers)
    # logging.info("response is ")
    # logging.info(http_response)
    return http_response


def fetch_policy_tags_method(**kwargs):
    start_time = time.time()
    cloud_function_input = {}
    CF_URL_mfm_list_policy_tags = Variable.get("cf_url_mfm_list_policy_tags")
    logging.info(f" Posting REQUEST TO {CF_URL_mfm_list_policy_tags}...")
    response = trigger_cloud_function(CF_URL_mfm_list_policy_tags, cloud_function_input=cloud_function_input)
    logging.info("response received is ", response.status_code, response.text)
    policy_tags = response.json()
    logging.info("fetched policy tags datatype is ", type(policy_tags))
    logging.info(f"policytags fetched are {policy_tags}")
    kwargs['ti'].xcom_push(key='policy_tags', value=json.dumps(policy_tags))
    end_time = time.time()
    logging.info("time taken to fetch policy tags = ", end_time - start_time)
    time_dict["time taken to fetch policy tags"] = end_time - start_time


def run_query(query_string: str) -> pd.core.frame.DataFrame:
    """
    Generic function to run a query in BigQuery
    :param query_string: query to execute
    :return: pandas dataframe containing results
    """

    bq_client = bigquery.Client(credentials=creds, project=MGMT_PROJECT)
    df = (
        bq_client.query(query_string).result().to_dataframe()
    )

    return df


def trigger_datacatalog_sync_method(**kwargs) -> list:
    """
    	Helper function to get set of datasets in a gcp_project_name
    	:param gcp_project_name: gcp_project_name to examine
    	:return: set of dataset names
    """
    total_start_time = time.time()
    policy_tags = kwargs['ti'].xcom_pull(task_ids='fetch_policy_tags', key='policy_tags')
    start_time = time.time()
    gcp_project_name = kwargs['gcp_project_name']
    mfm_check_diff_request_syncs_metadata = Variable.get("mfm_check_diff_request_syncs_metadata", deserialize_json=True)
    mgmt_project = mfm_check_diff_request_syncs_metadata["MGMT_PROJECT"]
    cf_url = mfm_check_diff_request_syncs_metadata["SYNC_TAGS_CF_URL"]
    df = run_query(f"SELECT DISTINCT schema_name FROM `{gcp_project_name}.INFORMATION_SCHEMA.SCHEMATA`")
    datasets = list(df["schema_name"])
    dataset_list = []
    for d in datasets:
        if d == "feeder" or d == "feeder_temp" or d == 'looker_pdt' or (len(d) >= 8 and d[:8] == 'dataset_'):
            continue
        dataset_list.append(d)
    end_time = time.time()
    logging.info("time taken to find dataset_list = ", end_time - start_time)
    time_dict["time taken to find dataset_list"] = end_time - start_time
    failure_occured_project = False
    project_failed_tables = {}
    for dataset in dataset_list:
        failure_occured_dataset = False
        dataset_failed_tables = {}
        start_time = time.time()
        query = (
            f"Select * except(cumulative_override_value, overrides_value)  from ( \n"
            f"    Select *,ROW_NUMBER()  OVER (PARTITION BY alias,dataset,table order by overrides_value desc) as cumulative_override_value FROM ( \n"
            f"        with mfm_dev_overrides as   (\n"
            f"              Select alias, description, policyTag, dataset, table from `{mgmt_project}.data_catalog.mfm_overrides_native` \n"
            f"              WHERE dataset = '{dataset}' \n"
            f"         ) \n"
            f"        ,viewlist as  (\n"
            f"              SELECT table_name, table_type FROM `{gcp_project_name}.{dataset}.INFORMATION_SCHEMA.TABLES`  \n"
            f"              WHERE table_type = 'VIEW' OR table_type = 'EXTERNAL' \n"
            f"        ) \n"
            f"        SELECT map.alias, properties.description, properties.policyTag, dataset_details.table_schema as dataset, dataset_details.table_name as table, 0 as overrides_value FROM   `{mgmt_project}.data_catalog.mfm_field_properties_native` properties \n"
            f"              join   `{mgmt_project}.data_catalog.mfm_alias_field_map_native` map on properties.field = map.field \n"
            f"              join   `{gcp_project_name}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` dataset_details on map.alias = dataset_details.field_path \n"
            f"              where dataset_details.table_name not like '%__x' and dataset_details.table_name not like '%__b' and dataset_details.table_name not like '%__bx' \n"
            f"        UNION ALL \n"
            f"        Select mfm_dev_overrides.alias, mfm_dev_overrides.description, mfm_dev_overrides.policyTag, mfm_dev_overrides.dataset, viewlist.table_name as table, 1 as overrides_value FROM viewlist \n"
            f"              , mfm_dev_overrides \n"
            f"              where REGEXP_CONTAINS(viewlist.table_name,CONCAT(mfm_dev_overrides.table,'_v[0-9]+')) IS TRUE \n"
            f"        UNION ALL  \n"
            f"        (Select *, 2 as overrides_value from mfm_dev_overrides) \n"
            f"  ) \n"
            f")  \n"
            f"where cumulative_override_value = 1 -- had to use another nesting cause if you check the inner query, cumulative_override_value is formed with a window function and where clause is directly not allowed on window functions \n"
        )
        logging.info("query for dataset " + dataset + " is " + query)
        dataset_df = run_query(query)
        dataset_df.columns = dataset_df.columns.str.strip()
        list_of_tables = list(dataset_df['table'].unique())
        list_of_tables = [i for i in list_of_tables if i != None]
        logging.info("list of tables is")
        logging.info(list_of_tables)
        for table in list_of_tables:
            table_df = dataset_df.loc[dataset_df['table'] == table][["alias", "description", "policyTag"]]
            full_table_name = gcp_project_name + "." + dataset + "." + table
            input_to_cloud_function = {"table_to_sync": full_table_name,
                                       "columns_to_be_changed_data": table_df.to_json(orient='records'),
                                       "policy_tags": policy_tags}
            try:
                trigger_cloud_function(cf_url, input_to_cloud_function)
            except Exception as e:
                logging.info(full_table_name + " failed with the exception " + str(e))
                dataset_failed_tables[full_table_name] = str(e)
                failure_occured_project = True
                failure_occured_dataset = True
            logging.info("input_to_cloud_function is ")
            logging.info(input_to_cloud_function)
        if failure_occured_dataset:
            logging.info("failed tables for the dataset " + dataset)
            logging.info(dataset_failed_tables)
            project_failed_tables[dataset] = dataset_failed_tables

        end_time = time.time()
        time_dict["time taken to process dataset " + dataset] = end_time - start_time
        logging.info("time taken to process dataset " + dataset, end_time - start_time)
        logging.info("dataframe for the " + dataset + " is")
        logging.info(dataset_df)
    if failure_occured_project:
        logging.info("following is the list of tables per dataset that failed")
        logging.info(project_failed_tables)
    # logging.info(list(df["schema_name"]))
    total_end_time = time.time()
    logging.info("total of ", len(dataset_list), "datasets were process in ", total_end_time - total_start_time,
                 "seconds")
    time_dict["total time to process " + str(len(dataset_list)) + " datasets "] = total_end_time - total_start_time
    logging.info("time computations are as follows", time_dict)
    return list(df["schema_name"])


def create_dag(dag_id, schedule, default_args):
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              catchup=False,
              max_active_runs=1)

    with dag:

        start = DummyOperator(
            task_id="start",
            dag=dag
        )

        source_data_ready = DummyOperator(
            task_id="native_tables_ready",
            dag=dag
        )
        fetch_policy_tags = PythonOperator(task_id="fetch_policy_tags",
                                           python_callable=fetch_policy_tags_method,
                                           trigger_rule=TriggerRule.ALL_SUCCESS,
                                           provide_context=True)
        syncs_completed = DummyOperator(
            task_id="syncs_completed",
            dag=dag
        )

        end = DummyOperator(
            task_id="end",
            dag=dag
        )

        for source_table in gsheet_to_native:
            '''
            if Variable.get('environment')=='dev':

                table_name = source_table.split(".")[-1]

                d_json = json.dumps(
                    {
                        "source": source_table,
                        "target": gsheet_to_native[source_table]
                    }
                )

                refresh_native_table = SimpleHttpOperator(
                    task_id=f"refresh_{table_name}_native",
                    http_conn_id="cf_create_or_replace_table",
                    method="POST",
                    endpoint="",
                    data=d_json,
                    headers={"Authorization": f"Bearer {get_id_token(CREATE_OR_REPLACE_TABLE_CF_URL)}",
                             "Content-Type": "application/json"},
                    dag=dag,
                )

                start >> refresh_native_table >> source_data_ready
            else:
                start >> source_data_ready
            '''
            start >> source_data_ready
        source_data_ready >> fetch_policy_tags
        for p in MFM_PROJECTS:
            trigger_datacatalog_sync = PythonOperator(task_id=f"resync_tables_project_{p}",
                                                      python_callable=trigger_datacatalog_sync_method,
                                                      trigger_rule=TriggerRule.ALL_SUCCESS,
                                                      op_kwargs={'gcp_project_name': p},
                                                      provide_context=True)

            fetch_policy_tags >> trigger_datacatalog_sync >> syncs_completed
        for native_table in native_to_snapshot:
            '''
            #This has been commented now to let the sync process run for whole MFM_DEV sheet. These comments should be removed once mfm_auto_request_resyncs starts working
            table_name = native_table.split(".")[-1]

            d_json = json.dumps(
                {
                    "source": native_table,
                    "target": native_to_snapshot[native_table]
                }
            )

            refresh_snapshot_table = SimpleHttpOperator(
                task_id=f"refresh_{table_name}_snapshot",
                http_conn_id="cf_create_or_replace_table",
                method="POST",
                endpoint="",
                data=d_json,
                headers={"Authorization": f"Bearer {get_id_token(CREATE_OR_REPLACE_TABLE_CF_URL)}",
                         "Content-Type": "application/json"},
                dag=dag,
            )
            '''

            # syncs_completed >> refresh_snapshot_table >> end
        syncs_completed >> end

        return dag


# Setup DAG
dag_id = f"mfm_sync_tags"
schedule = None

default_args = {"owner": "apex",
                "start_date": days_ago(0)
                }

dag = create_dag(dag_id, schedule, default_args)
