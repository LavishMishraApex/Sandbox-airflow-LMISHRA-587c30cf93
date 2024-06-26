from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from  pkg.tasks import log_group, validate_raw, validate_raw_legacy
from  pkg.tasks.validate_snapshot import validate_row_count, validate_accessibility, validate_clear_text
from typing import Dict

def create_snapshot_pipelines(
        process_date: str,
        include_margins: bool = False
) -> Dict[str, TaskGroup]:
    jobs = [
        "daily_account_groups",
        "daily_accounts",
        "daily_accounts_v2",
        "daily_activities",
        "daily_clients",
        "daily_correspondent_address",
        "daily_correspondents",
        "daily_correspondents_contra_accounts",
        "daily_entries",
        "daily_positions",
        "latest_announcements",
        "books_and_records_order_memorandum",
        "latest_equities",
        "latest_currencies",
        "latest_prices",
        "latest_fdic_synthetics",
        "latest_exchanges",
        "latest_assets",
        "allocator_detail",
    ]

    margins_jobs = [
        "amount_available_for_withdrawal_detail",
        "daily_overnight_balances",
        "daily_overnight_buying_power",
        "daily_position_breakout_strategy_overnight",
        "daily_margin_calls_alerts",
        "daily_cash_trading_list",
    ]
    if include_margins:
        jobs += margins_jobs

    tg_dict: Dict[str, TaskGroup] = {}
    for job_name in jobs:
        with TaskGroup(group_id=f"datalake_eod_{job_name}") as job_tg:
            validate = validate_raw(job_name, process_date)
            snapshot = TriggerDagRunOperator(
                task_id=f"snapshot_service__{job_name}",
                trigger_dag_id=f"snapshot_service__{job_name}",
                conf={
                    "process_date": process_date
                },
                wait_for_completion=True,
                retries=0,
            )
            validate_rc = validate_row_count(job_name, process_date)
            validate_access = validate_accessibility(job_name, process_date)
            validate_cleartext = validate_clear_text(job_name, process_date)

            validate >> snapshot >> validate_rc >> validate_access >> validate_cleartext

            log_group(job_tg, process_date)
        tg_dict[f"datalake_eod_{job_name}"] = job_tg

    (
        tg_dict["datalake_eod_daily_accounts"]
        >> [
            tg_dict["datalake_eod_daily_accounts_v2"],
            tg_dict["datalake_eod_daily_positions"],
            tg_dict["datalake_eod_daily_entries"],
            tg_dict["datalake_eod_daily_activities"],
            tg_dict["datalake_eod_daily_clients"],
            tg_dict["datalake_eod_daily_correspondent_address"],
            tg_dict["datalake_eod_daily_correspondents"],
            tg_dict["datalake_eod_daily_account_groups"],
            tg_dict["datalake_eod_daily_correspondents_contra_accounts"],
            tg_dict["datalake_eod_latest_announcements"],
            tg_dict["datalake_eod_books_and_records_order_memorandum"],
            tg_dict["datalake_eod_latest_equities"],
            tg_dict["datalake_eod_latest_currencies"],
            tg_dict["datalake_eod_latest_prices"],
            tg_dict["datalake_eod_latest_fdic_synthetics"],
            tg_dict["datalake_eod_latest_exchanges"],
            tg_dict["datalake_eod_latest_assets"],
            tg_dict["datalake_eod_allocator_detail"],
        ]
    )

    if include_margins:
        (
            tg_dict["datalake_eod_daily_accounts"]
            >> [
                tg_dict["datalake_eod_daily_overnight_balances"],
                tg_dict["datalake_eod_daily_overnight_buying_power"],
                tg_dict["datalake_eod_daily_position_breakout_strategy_overnight"],
                tg_dict["datalake_eod_daily_margin_calls_alerts"],
                tg_dict["datalake_eod_daily_cash_trading_list"],
            ]
        )

        (
            tg_dict["datalake_eod_daily_overnight_buying_power"]
            >> tg_dict["datalake_eod_amount_available_for_withdrawal_detail"]
        )

    return tg_dict
