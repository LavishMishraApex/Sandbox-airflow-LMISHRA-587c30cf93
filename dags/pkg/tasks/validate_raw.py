from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from  config.globals import GCP_PROJECTS, ENVIRONMENT

def run_validation(routine: str, process_date: str):

    query = (
        f"""
        SELECT raw_row_count - sync_report_row_count
        FROM `{routine}`("{process_date}");
        """
    )

    hook = BigQueryHook(
        gcp_conn_id="datalake_gcp_mgmt_id",
        use_legacy_sql=False,
        impersonation_chain=f"airflow@apex-airflow-{ENVIRONMENT}-00.iam.gserviceaccount.com",
    )
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    ls_of_ls = cursor.fetchall()

    difference = ls_of_ls[0][0]
    if difference != 0:
        raise RuntimeError(f"Row difference is not zero for {routine}")

def validate_raw(table: str, process_date: str) -> TaskGroup:
    routine_dict = {
        "daily_positions": f"{GCP_PROJECTS['INTERNAL_HUB']}.ledger.row_count_validation_positions",
        "daily_entries": f"{GCP_PROJECTS['INTERNAL_HUB']}.ledger.row_count_validation_entries",
        "daily_activities": f"{GCP_PROJECTS['INTERNAL_HUB']}.ledger.row_count_validation_activities",
        "daily_overnight_balances": f"{GCP_PROJECTS['INTERNAL_HUB']}.margins.row_count_validation_overnight_buying_power",  #Note: This is the same as daily_overnight_buying_power
        "daily_overnight_buying_power": f"{GCP_PROJECTS['INTERNAL_HUB']}.margins.row_count_validation_overnight_buying_power",
        "daily_cash_trading_list": f"{GCP_PROJECTS['INTERNAL_HUB']}.margins.row_count_validation_cash_trading_list",
        "daily_margin_calls_alerts": f"{GCP_PROJECTS['INTERNAL_HUB']}.margins.row_count_validation_margin_calls_alerts",
        "daily_position_breakout_strategy_overnight": f"{GCP_PROJECTS['INTERNAL_HUB']}.margins.row_count_validation_position_breakout_strategy_overnight",
    }
    with TaskGroup(group_id="validations") as validation_tg:
        if table in routine_dict.keys():
            PythonOperator(
                task_id="check_row_counts",
                python_callable=run_validation,
                op_kwargs={
                    "routine": routine_dict[table],
                    "process_date": process_date
                },
                retries=48,
                retry_delay=timedelta(minutes=10),
            )
        else:
            DummyOperator(task_id="dummy_validation")
    return validation_tg
