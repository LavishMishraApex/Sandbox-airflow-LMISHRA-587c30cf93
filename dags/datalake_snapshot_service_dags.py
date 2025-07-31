import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
from airflow.operators.python import get_current_context

current_date = str(datetime.now(pytz.timezone("UTC")).date())

jobs = {
    "latest_currencies": {
        "process_date": current_date,
    },
    "latest_prices": {
        "process_date": current_date,
    }
}


def create_snapshot_dag(dag_id: str, job_name: str, parameters: dict) -> DAG:
    dag = DAG(
        dag_id=dag_id,
        schedule_interval=None,
        default_args={
            "owner": "team:datalake",
            "start_date": days_ago(0),
            "retries": 0,
            "params": parameters
        },
        tags=["snapshot_service"]
    )
    table_function_parameters = {}
    with dag:
        def _trigger_snapshot_service_cf(job_name: str, parameters: dict):
            # if the parameter is a list, then convert the runtime input which will be a list wrapped in quotes
            logging.info("need table_function_parameters here somehow")
            context = get_current_context()
            logging.info(context)
            logging.info(context['ti'].task_id)
            logging.info(
                f"dagrun configutaions received are {context['dag_run'].conf}")
        # generate jinja templates for the PythonOperator call
        runtime_parameters = {}
        for key in parameters:
            run_value = "{{ params['" + key + "'] }}"
            runtime_parameters[key] = run_value
        run_task = PythonOperator(
            task_id=f"snapshot__{job_name}",
            python_callable=_trigger_snapshot_service_cf,
            execution_timeout=timedelta(minutes=60),
            op_kwargs={
                "job_name": job_name,
                "parameters": runtime_parameters,
            }
        )


for job_name in jobs:
    dag_id = f"snapshot_service__{job_name}"
    locals()[dag_id + "_dag"] = create_snapshot_dag(dag_id=dag_id,
                                                    job_name=job_name, parameters=jobs[job_name])
