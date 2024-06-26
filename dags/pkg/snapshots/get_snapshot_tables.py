from  config.snapshots import gcp_tables_config, snapshot_tables_config, client_specific_config
from  pkg.objects import GCPBaseTable, FilterKey
from typing import List


def define_snapshot_table(alias) -> GCPBaseTable:

    snapshot_config = snapshot_tables_config[alias]
    table_config = gcp_tables_config[alias]
    table = GCPBaseTable(
        alias=alias,
        project_variable=table_config["project_variable"],
        schema=table_config["schema"],
        name=table_config["name"],
        keys=snapshot_config["keys"],
        filters=list(map(lambda x: FilterKey(**x), snapshot_config["filters"]))
    )

    return table


def get_snapshot_tables(client_code: str = None) -> List[GCPBaseTable]:
    snapshot_tables = []
    if client_code:
        alias_list = []
        for alias in snapshot_tables_config:
            if snapshot_tables_config[alias]["for_all_clients"] == "True":
                alias_list = alias_list + [alias]

        client_alias_list = alias_list
        if client_code in client_specific_config:
            client_alias_list = client_alias_list + client_specific_config[client_code]
        client_alias_list = list(set(client_alias_list))

        client_tables = []
        for alias in client_alias_list:
            table = define_snapshot_table(alias)
            client_filter_found = False
            for _filter in table.filters:
                if _filter.method == "correspondent_code":
                    client_filter_found = True
            if client_filter_found or client_code == "NULL":
                client_tables = client_tables + [define_snapshot_table(alias)]
        snapshot_tables = client_tables
    else:
        for alias in snapshot_tables_config:
            table = define_snapshot_table(alias)
            snapshot_tables = snapshot_tables + [table]
    return snapshot_tables


