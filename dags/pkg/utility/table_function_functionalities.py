'''

SELECT * FROM 
                apex-internal-hub-dev-00.datalake_status.internal_hub_health  
     WHERE  project_name = "apex-assets-dev-00"
 AND dataset_name = "feeder"
 AND table_name = "apexinternal_assets_v1_price_apexinternal_assets_v1_price"
 AND process_date = "2025-07-31"
 AND test_name = "ODS_replication_validation"


 

 SELECT * FROM 
                apex-internal-hub-dev-00.datalake_status.internal_hub_health  
     WHERE  project_name = "apex-assets-dev-00"
 AND dataset_name = "feeder"
 AND table_name = "apexinternal_assets_v1_price_apexinternal_assets_v1_price"
 AND process_date = "2025-07-31"
 AND test_name = "ODS_replication_validation"


 SELECT * FROM 
                apex-internal-hub-dev-00.datalake_status.internal_hub_health  
     WHERE  project_name = "apex-assets-dev-00"
 AND dataset_name = "feeder"
 AND table_name = "apexinternal_assets_v1_price_apexinternal_assets_v1_price"
 AND process_date = "2025-07-31"
 AND test_name = "ODS_replication_validation"
 
 
SELECT * FROM 
                apex-internal-hub-dev-00.datalake_status.internal_hub_health  
     WHERE  project_name = "apex-internal-hub-dev-00"
 AND dataset_name = "snapshots/assets"
 AND table_name = "latest_prices"
 AND process_date = "2025-07-31"
 AND test_name = "Snapshot_source_validation"
 '''
from dags.pkg.utility.bigquery_functionalities import *  # needs path changed
import logging


def fetch_list_of_arguments_of_a_table_function(routine_id):
    client = make_bigquery_client()

    try:
        routine = client.get_routine(routine_id)
        arguments_list = []
        for argument in routine.arguments:
            print("\t\tName: '{}'".format(argument.name))
            print("\t\tType: '{}'".format(argument.data_type))
            arguments_list.append(argument.name)
        return arguments_list
    except Exception as e:
        print(f"Error fetching routine: {e}")


def fetch_table_function_for_snapshot(snapshot_name: str) -> [bool, str]:
    """
    Checks if a Snapshot is configured to used Table Functions, 
    if yes, returns full qualified name of the table function
    :param snapshot_name: Name of the snapshot.
    :return: True, Table function name if configured, else False.
    """
    configs_table = "apex-internal-hub-dev-00.common.active_snapshot_jobs"  # Needs to be imported from variables
    QUERY = f"""
        SELECT asset_type, source_table
        FROM `{configs_table}`
        WHERE job_name = '{snapshot_name}'
    """
    logging.info(
        f"Running query to fetch table function for snapshot: {snapshot_name}, query: {QUERY}")
    results = list(run_query(QUERY))[0]
    if results:
        asset_type, source_table = results
        if asset_type == "TABLE_FUNCTION":
            logging.info(
                f"Table function found for snapshot: {snapshot_name}, function: {source_table}")
            return True, source_table["project_var"] + "." + source_table["schema_name"] + "." + source_table["table_name"]
        else:
            logging.info(
                f"No table function found for snapshot: {snapshot_name}, asset type: {asset_type}")
            '''
            Adding this for testing, please remove later
            '''
            return True, "apex-internal-hub-dev-00.assets.snapshot_latest_prices"  # Example table function, remove in production
            return False, "Table Function is not configured"
    return [True, 'random_return']


def fetch_table_function_parameters_from_dhp():
