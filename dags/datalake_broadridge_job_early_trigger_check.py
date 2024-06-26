# 2024-06-24 lmishra initial deployment

import datetime
import json
import logging
import os
import pytz

import google.auth.transport.requests
import google.oauth2.id_token
from google.auth import impersonated_credentials
from google.cloud import bigquery

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

#from pkg.utility import get_impersonated_creds  

job_configurations = {   
    "fbb_bqraw_rf" :    {
        
        "task_id": "000 Refresh Channel - fbb_bqraw_rf",
        "process_name": "Tidal",
        "status": "Started" 
    },
    "fbi_bqraw_rf" :    {
        "task_id": "000 Refresh Channel - fbi_bqraw_rf",
        "process_name": "Tidal",
        "status": "Started" 
    }
}
# get process date from dag_run.conf or use yesterday
def get_process_date(**kwargs) -> str:
        
    process_date = kwargs.get("dag_run").conf.get("process_date")
    if process_date:
        assert isinstance(process_date, str)
        try:
            datetime.datetime.strptime(process_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Incorrect date format, should be YYYY-MM-DD")
        logging.info(f"Using date from config: {process_date}")
        return process_date
    else:
        date = datetime.datetime.now(pytz.timezone('US/Central'))
        date = date - datetime.timedelta(days=1)
        logging.info(f"Using current date: {date}")
        return date.strftime("%Y-%m-%d")
    



# main function to check if the replication has started
def check_early_start(**kwargs):
    job_name = kwargs['job_name']
    logging.info(f"job name received is {job_name}")
    
    process_date = get_process_date(**kwargs)
    logging.info(f"process_date: {process_date}")

def failure_notification():
    env = Variable.get("environment")
    channel = "datalake-health-check-alerts-dev" #channel = "datalake-collaboration" if env == "prd" else f"datalake-health-check-alerts-{env}"
    slack_connection_id = "slack_notifier"
    slack_channel = channel
    msg = f":red_circle: Broadridge jobs have started early.Please check on the jobs 'fbi_bqraw_rf' and 'fbb_bqraw_rf' <!subteam^S04PA2MNXSB> "

    return send_slack_notification(
        slack_conn_id=slack_connection_id,
        text=msg,
        channel=slack_channel,
    )
    

def create_dag(dag_id, schedule):

    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=datetime.datetime(2024, 6, 24, tzinfo=pytz.timezone('US/Central')),
        max_active_runs=1,
        catchup=False,
        tags=["team:datalake"],
        default_args={
        "owner": "datalake",
        "retries": 3, 
        "on_failure_callback": failure_notification(),
        },
    )

    with dag:
            # dummy operator to start the dag
            start = EmptyOperator(
                task_id="Start",
                dag=dag,
            )

            for job_name in list(job_configurations.keys()):
                early_start_check = PythonOperator(
                task_id=f"early_start_check_{job_name}",
                python_callable=check_early_start,
                op_kwargs={'job_name': job_name},
                dag=dag,
                provide_context=True,
                )

            # empty operator to end the dag
            end = EmptyOperator(
                task_id="End",
                dag=dag,
            )

            start >> early_start_check >> end

    return dag


dag_id = "datalake_broadridge_job_early_trigger_check"
schedule = None #needs to be changed to 2 am CST

dag = create_dag(dag_id, schedule)

