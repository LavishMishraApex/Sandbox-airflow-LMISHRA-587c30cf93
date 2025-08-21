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
# Needs to be fetched from configurations
INTERNAL_HUB = "apex-internal-hub-dev-00"


def fetch_list_of_arguments_of_a_table_function(routine_id):
    client = make_bigquery_client()

    try:
        routine = client.get_routine(routine_id)
        arguments_list = []
        for argument in routine.arguments:
            arguments_list.append(argument.name)
        return arguments_list
    except Exception as e:
        logging.info(f"Error fetching routine: {e}")


def fetch_table_function_for_snapshot(snapshot_job_name: str) -> [bool, str]:
    """
    Checks if a Snapshot is configured to used Table Functions, 
    if yes, returns full qualified name of the table function
    :param snapshot_job_name: Name of the snapshot.
    :return: True, Table function name if configured, else False.
    """
    configs_table = "apex-internal-hub-dev-00.common.active_snapshot_jobs"  # Needs to be imported from variables
    QUERY = f"""
        SELECT asset_type, source_table
        FROM `{configs_table}`
        WHERE job_name = '{snapshot_job_name}'
    """
    logging.info(
        f"Running query to fetch table function for snapshot: {snapshot_job_name}, query: {QUERY}")
    results = list(run_query(QUERY))[0]
    if results:
        asset_type, source_table = results
        if asset_type == "TABLE_FUNCTION":
            logging.info(
                f"Table function found for snapshot: {snapshot_job_name}, function: {source_table}")
            source_table["project_var"] = source_table["project_var"].replace(
                "INTERNAL_HUB", INTERNAL_HUB)
            return True, source_table["project_var"] + "." + source_table["schema_name"] + "." + source_table["table_name"]
        else:
            logging.info(
                f"No table function found for snapshot: {snapshot_job_name}, asset type: {asset_type}")
            '''
            Adding this for testing, please remove later
            '''
            return True, "apex-internal-hub-dev-00.assets.snapshot_latest_prices"  # Example table function, remove in production
            return False, "Table Function is not configured"


def get_table_function_invocation_phrase(table_function_name: str, parameters: dict) -> str:
    """
    Returns the invocation phrase for a table function
    :param table_function_name: Full qualified name of the table function.
    :param parameters: Dictionary of parameters to be passed to the table function."""
    table_function_invocation_phrase = f"`{table_function_name}`("
    table_function_invocation_phrase += ", ".join(
        [f"{k} => {v}" for k, v in parameters.items()])
    table_function_invocation_phrase += ")"
    return table_function_invocation_phrase
