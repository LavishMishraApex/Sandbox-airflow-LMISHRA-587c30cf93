from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.http.operators.http import SimpleHttpOperator
from  config.globals import CF_URL_EOD_VALIDATE_RAW_ROW_COUNT, ENVIRONMENT
from  pkg.utility import get_id_token
from datetime import datetime, timedelta
import json
DAG_TO_BE_TRIGGERED = "datalake_freshness_check"


def validate_raw_legacy(
        product: str,
        process_date: str,
        is_recon: bool = False
) -> TaskGroup:
    """
    This function returns operators for trading day and row_count validations
    """
    product_src = {
        "positions": "position_history",
        "entries": "entry",
        "activities": "activity",
    }

    with TaskGroup(group_id='legacy_validations') as validations:
        if ENVIRONMENT == 'stg':
            # validations don't work in stg yet.
            dummy_task = DummyOperator(task_id="dummy_validation_task")
        else:
            trigger_freshness_check_dag = TriggerDagRunOperator(
                task_id='trigger_freshness_check_dag',
                trigger_dag_id=DAG_TO_BE_TRIGGERED,
                conf={"domain": product, "process_date": process_date},
                wait_for_completion=True
            )
            if product in product_src:
                request = json.dumps(
                    {
                        "table": product_src[product],
                        "process_date": str(datetime.strptime(process_date, "%Y-%m-%d").date()),
                        "is_recon": is_recon,
                    }
                )
                check_row_counts = SimpleHttpOperator(
                    task_id=f"eod_validate_row_count_{product}",
                    http_conn_id="cf_eod_validate_raw_row_count",
                    method="POST",
                    endpoint="",
                    data=request,
                    headers={
                        "Authorization": f"Bearer {get_id_token(CF_URL_EOD_VALIDATE_RAW_ROW_COUNT)}",
                        "Content-Type": "application/json",
                    },
                    retries=48,
                    retry_delay=timedelta(minutes=10),
                )

                trigger_freshness_check_dag >> check_row_counts
    return validations
