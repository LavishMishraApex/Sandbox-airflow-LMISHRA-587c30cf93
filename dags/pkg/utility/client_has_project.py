from .global_for_gcp_project_exists import global_for_gcp_project_exists


def client_has_project(client_code: str):
    """
    check if client has an airflow var for their project
    """
    results = global_for_gcp_project_exists(f"{client_code.upper()}_HUB")
    return results
