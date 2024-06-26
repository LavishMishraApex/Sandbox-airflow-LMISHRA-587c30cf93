from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from  pkg.objects import GCPBaseTable, GCPBucket
from  pkg.utility import build_column_string
from typing import List
from datetime import datetime
import pytz


def export_table(
    task_id: str,
    table: GCPBaseTable,
    bucket: GCPBucket,
    client_code: str,
    process_date: str,
    gcp_conn_id: str,
    impersonation_chain: str,
) -> PythonOperator:

    """
    SNAPSHOT A GCP TABLE TO A GCP BUCKET
    """
    def _export_table(
        table: GCPBaseTable,
        bucket: GCPBucket,
        client_code: str,
        process_date: str,
        gcp_conn_id: str,
        impersonation_chain: str,
    ):
        process_date = datetime.strptime(process_date, "%Y-%m-%d").date()
        run_timestamp = datetime.now(pytz.timezone("UTC"))

        # uri
        snapshot_folder = (
            table.name
            + "_"
            + run_timestamp.strftime("%Y%m%d")
            + "T"
            + run_timestamp.strftime("%H%M%S")
            + "Z"
        )
        uri = (
            bucket.uri
            + "/"
            + process_date.strftime("%Y%m%d")
            + "/"
            + snapshot_folder
            + "/shard_*.parquet"
        )

        # if a filter column is used, it should be exposed in the external table
        # if it isn't in the list of keys, still include it in exposed columns
        key_column_list: List[str] = table.keys
        for _filter in table.filters:
            if _filter.column not in key_column_list:
                key_column_list = key_column_list + [_filter.column]

        key_columns = build_column_string(key_column_list)
        if "processDate" in key_column_list or "process_date" in key_column_list:
            pass
        else:
            key_columns = key_columns + f" DATE '{process_date}' as processDate,"

        # clause
        clause = []
        filter_errors = ""
        for _filter in table.filters:
            if _filter.method == "process_date":
                clause = clause + [f"{_filter.column} = '{process_date}'"]
            elif _filter.method == "correspondent_code":
                if client_code != "NULL":
                    clause = clause + [f"UPPER({_filter.column}) = '{client_code}'"]
                else:
                    clause = clause + [f"{_filter.column} IS NULL"]
            else:
                filter_errors = (
                    filter_errors
                    + f"\nERROR:{_filter} IN ACTIVE TABLE {table.alias} SETTINGS DOES NOT CORRESPOND TO CODED FILTER METHODS.\n"
                )

        clause = " AND ".join(clause)
        if clause != "":
            clause = "WHERE " + clause

        # query
        # only the inner most select is currently critical.
        # nesting in the other select statements is to force this onto one GCP worker
        # if this is divided up among multiple workers in GCP, it can cause a shard name-space collision. Its a GCP bug
        # the nesting of statements forces it onto one worker by using a distinct.
        # the outer select statement is there to mask that a new column was added
        query = (
            f"EXPORT DATA OPTIONS("
            f"uri=('{uri}'), "
            f"format='PARQUET', "
            f"overwrite=FALSE"
            f") AS "
            f"SELECT * EXCEPT (dummyColumnForSnapshots__123456789) FROM ("
            f"SELECT DISTINCT * FROM ("
            f"SELECT ROW_NUMBER() OVER(ORDER BY 1) as dummyColumnForSnapshots__123456789,"
            f"TO_JSON_STRING(t) AS payload, "
            f"{key_columns} "
            f"CURRENT_TIMESTAMP() as snapshotTimestamp, "
            f"FROM `{table.id}` t "
            f"{clause}"
            f")"
            f")"
        )

        print(f"QUERY WAS: {query}")
        if filter_errors != "":
            raise ValueError(filter_errors)
        else:
            hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False, impersonation_chain=impersonation_chain)
            hook.insert_job(configuration={"query": {"query": query, "useLegacySql": False}})

    run_task = PythonOperator(
        task_id=task_id,
        python_callable=_export_table,
        op_kwargs={
            "table": table,
            "bucket": bucket,
            "client_code": client_code,
            "process_date": process_date,
            "gcp_conn_id": gcp_conn_id,
            "impersonation_chain": impersonation_chain,
        },
        retries=0,
    )

    return run_task
