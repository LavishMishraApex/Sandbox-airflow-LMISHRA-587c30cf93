from dags.pkg.utility.table_function_functionalities import *  # needs path changed
from dags.pkg.utility.dhp_functionalities import *  # needs path changed
import logging
import sys
from airflow.exceptions import AirflowFailException

logging.getLogger().setLevel(logging.INFO)
sys.tracebacklimit = 0  # Remove traceback from logs for more readable error messages


def fetch_windowing_parameters_for_snapshot_generation(snapshot_name: str, process_date: str) -> [bool, dict]:
    """
    Fetches windowing parameters for snapshot generation from DHP.
    :param snapshot_name: Name of the snapshot.
    :param process_date: Process date for which the parameters are to be fetched.
    :return: Dictionary containing the parameters if found, else an empty dictionary.
    """
    is_table_function, table_function_name = fetch_table_function_for_snapshot(
        snapshot_name)
    logging.info(
        f"for snapshot {snapshot_name} with is_table_function={is_table_function} and table_function_name is {table_function_name} ")
    if is_table_function:
        table_function_arguments = fetch_list_of_arguments_of_a_table_function(
            table_function_name)
        logging.info(
            f"Table function arguments for {table_function_name} are {table_function_arguments}")
        table_function_parameters_exists_in_dhp, return_message, test_description_json = fetch_dhp_test_description_for_snapshot(
            snapshot_name, process_date)
        if table_function_parameters_exists_in_dhp:
            list_of_arguments_not_found_in_dhp = []
            asset_params = {}
            for argument in table_function_arguments:
                if argument not in test_description_json:
                    list_of_arguments_not_found_in_dhp.append(argument)
                else:
                    asset_params[argument] = f"{ test_description_json[argument] }"
            if list_of_arguments_not_found_in_dhp:
                raise AirflowFailException(
                    f"Arguments {list_of_arguments_not_found_in_dhp} not found in DHP for snapshot {snapshot_name} with process_date {process_date}, table_function_arguments required for table function {table_function_name} are {table_function_arguments}  but details found in DHP are {test_description_json}")
            return True, asset_params
        else:
            raise AirflowFailException(
                return_message + f" for snapshot {snapshot_name} with process_date {process_date}")
    else:
        logging.info(
            "Source of this Snapshot is not a table Function. Proceeding with Standard Snapshot Run")
        return False, {}
