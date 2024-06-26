import google.auth.transport.requests
import google.oauth2.id_token
import os

def get_id_token(service_url: str):
    auth_req = google.auth.transport.requests.Request()
    from  pkg.utility import get_impersonated_creds
    from  config.globals import ENVIRONMENT
    airflow_sa = f"airflow@apex-airflow-{ENVIRONMENT}-00.iam.gserviceaccount.com"
    token_creds = google.auth.impersonated_credentials.IDTokenCredentials(
        get_impersonated_creds(airflow_sa), target_audience=service_url, include_email=True)
    token_creds.refresh(auth_req)
    return token_creds.token



