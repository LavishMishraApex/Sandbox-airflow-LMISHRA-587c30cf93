from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator
from airflow import DAG

from typing import Dict
from datetime import datetime, date, timedelta
from airflow.utils.dates import days_ago


def check_dhp_status(**kwargs) -> None:
    context = get_current_context()
    table_function_parameters = {
        "min_id": 0, "max_id": 1, "max_id_column": "id"}
    context['ti'].xcom_push(key='table_function_parameters',
                            value=table_function_parameters)


def dhp_validations(job_name: str, process_date: str) -> PythonOperator:
    dhp_validations_task = PythonOperator(
        task_id="dhp_validations_"+job_name,
        python_callable=check_dhp_status,
        op_kwargs={
            "job_name": job_name,
            "process_date": process_date
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    return dhp_validations_task


def create_snapshot_call_arguments(context, dagrun_order) -> Dict:
    logging.info(f"context for the dag is {context}")
    logging.info(f"current dagrun order is {dagrun_order}")
    task_id = context['ti'].task_id
    logging.info(f"task_id is {task_id}")
    dagrun_order.payload["table_function_parameters"] = {
        "random_key": "random_value"
    }
    return dagrun_order


def create_snapshot_pipelines(process_date: str) -> TaskGroup:
    jobs = [

        "latest_currencies",
        "latest_prices",
    ]
    tg_dict: Dict[str, TaskGroup] = {}
    for job_name in jobs:
        with TaskGroup(group_id=f"datalake_eod_{job_name}") as job_tg:
            dhp_validations_task = dhp_validations(job_name, process_date)
            snapshot = TriggerDagRunOperator(
                task_id=f"snapshot_service__{job_name}",
                trigger_dag_id=f"snapshot_service__{job_name}",
                conf={
                    "process_date": process_date,
                },
                wait_for_completion=True,
                python_callable=create_snapshot_call_arguments,
                retries=0,
            )

        dhp_validations_task >> snapshot

        tg_dict[f"datalake_eod_{job_name}"] = job_tg
    return tg_dict


def create_dag():
    DAG_ID = "xcom_push_pull_recursion_check"
    PROCESS_DATE = "2025-02-25"

    dag = DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        catchup=False,
        tags=["team:datalake", "testing"],
        doc_md="""This dag is for testing integration only, it only exists in dev.""",
        default_args={
            "owner": "team:datalake",
            "depends_on_past": False,
            "email": [""],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "start_date": days_ago(1),
        },
    )

    with dag:
        create_snapshot_pipelines(process_date=PROCESS_DATE,)


create_dag()
