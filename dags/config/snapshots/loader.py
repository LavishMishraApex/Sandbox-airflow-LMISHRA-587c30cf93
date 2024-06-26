import yaml
import os


def load_yaml(file_name, list_name):
    dirname = os.path.dirname(__file__)
    filepath = os.path.join(dirname, file_name)
    with open(filepath, "r") as f:
        dict_of_list = yaml.safe_load(f)
    return dict_of_list[list_name]

gcp_tables_config = load_yaml("gcp_tables.yaml", "gcp_tables")
snapshot_tables_config = load_yaml("snapshot_tables.yaml", "snapshot_tables")
client_specific_config = load_yaml("client_specific.yaml", "client_specific")
