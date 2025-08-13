import json
import logging
import pytz
import sys
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException, AirflowException
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime
from google.cloud import bigquery

from dags.pkg.utility.table_function_functionalities import *  # needs path changed
from dags.pkg.utility.dhp_functionalities import *  # needs path changed

logging.getLogger().setLevel(logging.INFO)
sys.tracebacklimit = 0  # Remove traceback from logs for more readable error messages


DAG_DESC = "Run snapshot service v2 for a given snapshot and parameters."
DEFAULT_TESTS = ["row_count", "accessibility", "clear_text"]


# this will create v2 snapshot service DAG but datalake_eod_orchestration must be updated
# to run the v2 version. Should we create a central location for a list of v2 snapshots
# that each DAG can pull from?
# For now, we will set DB_TESTS to False for all snapshots in the list. DBT tests are not rerunnable from the snapshot service.
SNAPSHOTS = {
    "latest_prices": {
        "DBT_TESTS": False,
        "parameters": {"process_date": [], "data_window": {}}
    }
}


def handle_response(resp):
    res = json.loads(resp.text)
    if resp.status_code == 200:
        logging.info(
            f"Successful status returned from server.\n STATUS: {resp.status_code} \n DETAILS:\n{json.dumps(res, indent=2)}")
        return True
    else:
        logging.error(
            f"Unsuccessful status returned from server.\n STATUS: {resp.status_code} \n DETAILS:\n{json.dumps(res, indent=2)}")
        return False


def is_done(resp):
    context = get_current_context()
    ti = context["ti"]

    response = json.loads(resp.text)

    if response["done"]:
        logging.info(
            f"Snapshot finished with state: {response['metadata']['state']}")
        ti.xcom_push(key="response", value=response)
        return True
    else:
        logging.info(
            f"Snapshot is in progress with state: {response['metadata']['state']}")
        return False


@task
def build_request_params(snapshot_name: str) -> dict:
    context = get_current_context()
    parameters = context["params"].get("parameters")

    # check if process_date is an empty list. If empty pull process_date Airflow variable and convert to YYYY-MM-DD
    if not parameters.get("process_date"):
        process_date = Variable.get("process_date", default_var=None)
        if process_date:
            parameters["process_date"] = [
                str(datetime.strptime(process_date, "%m-%d-%Y").date())]

    # Check if individual tests are specified, if not check for DBT_TESTS
    if "tests" not in parameters.keys() and snapshot_name in SNAPSHOTS.keys() and SNAPSHOTS[snapshot_name].get("DBT_TESTS"):
        parameters["tests"] = DEFAULT_TESTS

    is_table_function, table_function_name = fetch_table_function_for_snapshot(
        snapshot_name)
    logging.info(
        f"for snapshot {snapshot_name} with is_table_function={is_table_function} and table_function_name is {table_function_name} ")
    if is_table_function:
        table_function_arguments = fetch_list_of_arguments_of_a_table_function(
            table_function_name)
        logging.info(
            f"Table function arguments for {table_function_name} are {table_function_arguments}")
        table_function_parameters_exists_in_dhp, return_message, source_asset_params = fetch_table_function_parameters_for_snapshot(
            snapshot_name, parameters["process_date"][0], table_function_arguments)  # need to add in the documentations that with windowing, we can only support one process_date and that would be the first one supplied within process_date list
        logging.info(
            f"table function parameters exists for snapshot {snapshot_name} with process_date {parameters['process_date'][0]}: {table_function_parameters_exists_in_dhp}, return_message: {return_message} are {source_asset_params}")
        if table_function_parameters_exists_in_dhp:
            parameters["data_window"] = source_asset_params
    else:
        logging.info(
            "Source of this Snapshot is not a table Function. Proceeding with Standard Snapshot Run")
    request_params = {"snapshot": snapshot_name}

    request_params.update(parameters)
    logging.info(
        f"Request parameters built: {json.dumps(request_params, indent=2)}")
    return request_params


@task(multiple_outputs=True)
def run_snapshot_service_v2(request_params: dict):
    context = get_current_context()

    logging.info(
        f"Calling snapshot service with parameters: {json.dumps(request_params, indent=2)}")
    http_run = HttpOperator(
        task_id="run_snapshot",
        http_conn_id="http_snapshot_service_v2",
        method="POST",
        endpoint="/v1beta/snapshots:run",
        data=json.dumps(request_params),
        response_check=lambda res: handle_response(res),
        response_filter=lambda res: json.loads(res.text),
        extra_options={"timeout": 3600}  # 1 hour
    )
    response = http_run.execute(context)
    return response


@task
def check_snapshot_is_done(response):
    context = get_current_context()

    snapshot_id = response.get("name")
    logging.info(f"Checking snapshot status for id: {snapshot_id}")

    http_run = HttpSensor(
        task_id="check_is_done",
        http_conn_id="http_snapshot_service_v2",
        method="GET",
        endpoint=f"/v1beta/snapshots/operations/{snapshot_id}",
        response_check=lambda res: is_done(res),
        mode="poke",
        poke_interval=30,
    )

    http_run.execute(context)


@task
def get_snapshot_result():
    context = get_current_context()
    ti = context["ti"]

    res = ti.xcom_pull(
        task_ids="check_snapshot_is_done",
        key="response"
    )

    if res["metadata"]["state"] == "COMPLETED":
        logging.info(
            f"Snapshot completed successfully with state: {res['metadata']['state']}")
        return True
    else:
        err = {"detail": res["response"]["detail"],
               "errors": res["response"]["errors"][0]}
        logging.error(f"Snapshot failed:\n{json.dumps(err, indent=2)}")
        raise AirflowFailException("Snapshot failed.")


def create_snapshot_dag(dag_id: str, snapshot_name: str, parameters: dict) -> DAG:
    default_args = {
        "owner": "team:datalake",
        "retries": 0,
        "params": {
            "parameters": {**parameters},
        },
    }
    dag = DAG(
        dag_id=dag_id,
        description=DAG_DESC,
        schedule_interval=None,
        catchup=False,
        max_active_runs=10,
        start_date=datetime(2025, 3, 25, 12),
        default_args=default_args,
        doc_md=f"""
        ### DAG for Snapshot: {snapshot_name}
        - Runs snapshot-service v2 for the snapshot `{snapshot_name}`.
        """
    )

    with dag:
        params = build_request_params(snapshot_name)
        run_snapshot = run_snapshot_service_v2(params)
        snapshot_state_check = check_snapshot_is_done(run_snapshot)
        snapshot_result = get_snapshot_result()

        params >> run_snapshot >> snapshot_state_check >> snapshot_result

    return dag


# Dynamically create DAGs for each snapshot
for snapshot_name, snapshot_config in SNAPSHOTS.items():
    dag_id = f"snapshot_service_v2__{snapshot_name}"
    parameters = snapshot_config.get("parameters", {})
    globals()[dag_id] = create_snapshot_dag(dag_id, snapshot_name, parameters)
