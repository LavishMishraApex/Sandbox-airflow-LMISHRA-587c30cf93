from airflow.decorators import dag, task
from airflow.utils.db import create_session
from airflow.models.log import Log
from datetime import datetime

import logging
import sys
logging.getLogger().setLevel(logging.INFO)
sys.tracebacklimit = 0  # Remove traceback from logs for more readable error messages


def return_latest_trigger_username(dag_id: str) -> str:
    """
    Retrieve and return latest the username of the Latest trigger of a DAG.
    Args:
        dag_id (str): The ID of the DAG to check.
    Returns:
        str: The username of the user who triggered the DAG, or "Unknown" if not
    """
    with create_session() as session:
        # Query the logs to find the username of the user who triggered the DAG
        result = session.query(Log.owner).filter(
            Log.dag_id == dag_id, Log.event == 'trigger').order_by(Log.dttm.desc()).first()

        username = result[0] if result else "Unknown"
        logging.info(f"The DAG '{dag_id}' was triggered by user: {username}")
        return username
