
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

from dags.pkg.tasks import dhp_validations
from dags.pkg.utility.bigquery_functionalities import *
from dags.pkg.tasks import dhp_validations

job_name = "snapshot_windowing_test"
process_date = "2025-09-04"


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

        dhp_validations_task = dhp_validations(job_name, process_date)

        return dag


dag_id = "dhp_validation_dag"
schedule = None  # needs to be changed to 2 am CST
dag = create_dag(dag_id, schedule)
