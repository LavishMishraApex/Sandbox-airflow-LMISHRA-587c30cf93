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


def get_dag_params(**context):
    """
    Retrieves job_name and process_date from DAG run configuration or params.
    
    Returns:
        dict: Dictionary containing job_name and process_date
        
    Raises:
        AirflowFailException: If process_date is not provided
    """
    from airflow.exceptions import AirflowFailException
    
    dag_run = context.get('dag_run')
    params = context.get('params', {})
    
    # Try to get from dag_run conf first, then from params
    if dag_run and dag_run.conf:
        job_name = dag_run.conf.get('job_name', params.get('job_name', 'daily_activities'))
        process_date = dag_run.conf.get('process_date', params.get('process_date', None))
    else:
        job_name = params.get('job_name', 'daily_activities')
        process_date = params.get('process_date', None)
    
    # Fail if process_date is not provided
    if not process_date:
        raise AirflowFailException(
            "process_date parameter is required. Please provide it via DAG run configuration or params."
        )
    
    return {'job_name': job_name, 'process_date': process_date}


def run_sync_report_checks(**context):
    """
    Wrapper function to get parameters and run sync report checks.
    """
    params = get_dag_params(**context)
    logging.info(f"Running sync report checks with job_name: {params['job_name']}, process_date: {params['process_date']}")
    
    # Call the imported execute_tests function directly
    execute_tests(params['job_name'], params['process_date'])


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
        params={
            "job_name": "daily_activities",
            "process_date": None,  # Must be provided - DAG will fail if not specified
        },
    )

    with dag:
        # Task to run sync report checks with dynamic parameters
        sync_report_checks_task = PythonOperator(
            task_id="sync_report_checks",
            python_callable=run_sync_report_checks,
            provide_context=True,
        )
        return dag


dag_id = "datalake_sync_report_checks"
schedule = None  # needs to be changed to 2 am CST
dag = create_dag(dag_id, schedule)
