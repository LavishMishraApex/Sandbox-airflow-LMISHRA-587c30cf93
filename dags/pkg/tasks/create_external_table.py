from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from  pkg.objects import GCPExternalTable


def create_external_table(
    task_id: str,
    external_table: GCPExternalTable,
    gcp_conn_id: str,
    impersonation_chain: str,
) -> BigQueryInsertJobOperator:
    """
    TURN A BUCKET INTO A GCP TABLE
    """

    query = (
        f"CREATE OR REPLACE EXTERNAL TABLE "
        "`"
        f"{external_table.id}"
        "` "
        f"OPTIONS ("
        f"format = 'PARQUET', "
        f"uris = {external_table.existing_uris}"
        f")"
    )

    run_task = BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": {"query": query, "useLegacySql": False}},
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
        retries=0,
    )
    return run_task
