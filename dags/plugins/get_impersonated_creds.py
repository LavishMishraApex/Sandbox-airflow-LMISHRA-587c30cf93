import google.auth.transport.requests
import google.oauth2.id_token
from google.auth import impersonated_credentials
def get_impersonated_creds(target_account, target_scopes=['https://www.googleapis.com/auth/cloud-platform']):
    source_creds, _ = google.auth.default()
    impersonated_creds = impersonated_credentials.Credentials(
        source_credentials=source_creds,
        target_principal=target_account,
        target_scopes=target_scopes,
        lifetime=3600  # 1 hour
    )
    return impersonated_creds
