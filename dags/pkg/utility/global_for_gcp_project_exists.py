from  config.globals import GCP_PROJECTS


def global_for_gcp_project_exists(variable: str) -> bool:
    """
    check if an airflow variable exists
    """
    it_exists = False
    try:
        GCP_PROJECTS[variable]
        it_exists = True
    except KeyError:
        pass
    return it_exists

