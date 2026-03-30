import json
import logging
import requests
import datetime
import pytz


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context

from pkg.tasks.sync_report_checks import *

job_name = "daily_activities"
process_date = "2026-02-20"  # needs to be fetched from x


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

        sync_report_checks_task = sync_report_checks(
            job_name, process_date)
        return dag


dag_id = "datalake_sync_report_checks"
schedule = None  # needs to be changed to 2 am CST
dag = create_dag(dag_id, schedule)
