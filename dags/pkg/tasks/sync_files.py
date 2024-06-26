from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from  pkg.objects import GCPBucket


def sync_files(
    task_id: str,
    source_bucket: GCPBucket,
    destination_bucket: GCPBucket,
    gcp_conn_id: str,
    impersonation_chain: str,
) -> GCSSynchronizeBucketsOperator:
    """
    SYNC FILES BETWEEN TWO BUCKETS
    """
    run_task = GCSSynchronizeBucketsOperator(
        task_id=task_id,
        source_bucket=source_bucket.bucket,
        source_object=source_bucket.gcs_object,
        destination_bucket=destination_bucket.bucket,
        destination_object=destination_bucket.gcs_object,
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
        retries=0,
    )
    return run_task
