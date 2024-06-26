from google.cloud import storage


def is_valid_uri(uri: str, project: str) -> bool:
    """
    uri looks like: 'gs://datalake-hub-sandbox-09d1b395-centralized-active-nonpii-nam4/apx1_published/daily_accounts/*'
    scans to see if any files match the uri
    """

    split = uri.find("/", 5)
    bucket_name = uri[5:split]
    prefix = uri[split + 1: -1]

    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    if blobs:
        results = True
    else:
        results = False
    return results
