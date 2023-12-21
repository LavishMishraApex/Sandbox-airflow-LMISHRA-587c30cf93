import google.auth.transport.requests
import google.oauth2.id_token


def get_id_token(service_url: str):
    auth_req = google.auth.transport.requests.Request()
    from dags.plugins import get_impersonated_creds
    from dags.plugins.globals import AIRFLOW_SA
    airflow_sa = AIRFLOW_SA
    token_creds = google.auth.impersonated_credentials.IDTokenCredentials(
        get_impersonated_creds(airflow_sa), target_audience=service_url, include_email=True)
    token_creds.refresh(auth_req)
    return token_creds.token



