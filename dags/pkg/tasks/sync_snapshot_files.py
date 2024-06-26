from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from  config.globals import CF_URL_SNAPSHOT_SERVICE, GCP_PROJECTS

def sync_snapshot_files(
    job_name: str,
    pii: bool,
) -> GCSSynchronizeBucketsOperator:
    """
    SYNC FILES BETWEEN TWO BUCKETS
    """

    if pii:
        pii_str = "pii"
    else:
        pii_str = "nonpii"

    run_task = GCSSynchronizeBucketsOperator(
            task_id=f"sync_{pii_str}_archive",
            source_bucket=f"{GCP_PROJECTS['INTERNAL_HUB']}-centralized-active-{pii_str}-us",
            source_object=f"snapshot_service/{job_name}/",
            destination_bucket=f"{GCP_PROJECTS['INTERNAL_HUB']}-centralized-archive-{pii_str}-us",
            destination_object=f"snapshot_service/{job_name}/",
            gcp_conn_id="datalake_gcp_mgmt_id",
            impersonation_chain=f"snapshot-{pii_str}@{GCP_PROJECTS['DATALAKE_MGMT']}.iam.gserviceaccount.com",
            retries=0,
        )

    return run_task
