from typing import List
from .is_valid_uri import is_valid_uri


def get_existing_uris(uri_list: List[str], project: str) -> str:
    """
    uri looks like: 'gs://datalake-hub-sandbox-09d1b395-centralized-active-nonpii-nam4/apx1_published/daily_accounts/*'
    only returns uri's that have files
    """
    valid_uri_list = []
    for uri in uri_list:
        if is_valid_uri(uri, project):
            valid_uri_list = valid_uri_list + [uri]
    return valid_uri_list
