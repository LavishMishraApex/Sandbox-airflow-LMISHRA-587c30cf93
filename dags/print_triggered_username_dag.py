from airflow.decorators import dag, task
from airflow.utils.db import create_session
from airflow.models.log import Log
from datetime import datetime


@task
def print_triggered_username(dag_id: str):
    """
    Retrieve and print the username of the user who triggered the DAG execution.
    """
    with create_session() as session:
        # Query the logs to find the username of the user who triggered the DAG
        result = session.query(Log.owner).filter(
            # .first()
            Log.dag_id == dag_id, Log.event == 'trigger').order_by(Log.dttm.desc())
        for res in result:
            print(f"Owner found: {res}")

        username = result[0] if result else "Unknown"
        print(f"The DAG '{dag_id}' was triggered by user: {username}")


@dag(
    dag_id='print_triggered_username_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 1},
    tags=['example', 'username'],
    description='A DAG that prints the username of the user who triggered it.'
)
def print_triggered_username_dag():
    """
    DAG to print the username of the user who triggered the DAG execution.
    """
    print_triggered_username(dag_id='datalake_update_unhealthy_dhp_status')


print_triggered_username_dag()
