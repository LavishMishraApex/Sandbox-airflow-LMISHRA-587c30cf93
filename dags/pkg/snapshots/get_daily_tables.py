from  pkg.objects import GCPExternalTable
from  pkg.utility import get_client_code_list
from  pkg.snapshots.get_snapshot_tables import get_snapshot_tables
from typing import List


def get_daily_tables() -> List[GCPExternalTable]:
    """
    returns the datalake_internal_hub daily_table for every active_table
    """
    client_codes = get_client_code_list()
    active_tables = get_snapshot_tables()

    daily_table_list: List[GCPExternalTable] = []
    for _table in active_tables:
        project_variable = _table.project_variable
        keys = _table.keys
        daily_name = f"daily_{_table.alias}"
        project = _table.project

        uri_list = []
        for _client_code in client_codes:
            # location where published data would be stored
            uri_internal = f"gs://{project}-centralized-active-nonpii-us/{_client_code.lower()}_published/{daily_name}/*"
            uri_published = f"gs://{project}-centralized-active-nonpii-us/{_client_code.lower()}_internal/{daily_name}/*"
            uri_list = uri_list + [uri_internal] + [uri_published]

        daily_table = GCPExternalTable(
            alias=_table.alias,
            project_variable=project_variable,
            schema="snapshots",
            name=daily_name,
            filters=_table.filters,
            keys=keys,
            uri_list=uri_list,
        )

        daily_table_list = daily_table_list + [daily_table]

    return daily_table_list
