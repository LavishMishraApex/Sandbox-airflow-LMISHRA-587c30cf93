from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
#from ascend.datalake.pkg.utility import get_id_token

from dags.plugins import get_id_token

DAG_ID="datalake_test_dag"

tablefrequencydict={}
'''
def call_cloud_function(message):
    import requests
    response = requests.get("http://api.open-notify.org/astros.json")
    print(response)
'''
CF_URL_testcloudfunction = """https://us-central1-apex-datalake-mgmt-dev-00.cloudfunctions.net/mfm-list-policy-tags"""
# "def get_id_token(service_url: str):
#     auth_req = google.auth.transport.requests.Request()
#     id_token = google.oauth2.id_token.fetch_id_token(auth_req, service_url)
#     return id_token"
dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    catchup=False,
    tags=["team:datalake", "testing"],
    doc_md="""this is a dummy testing dag, please ignore this!""",
    default_args={
        "owner": "team:datalake",
        "depends_on_past": False,
        "email": [""],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "start_date": days_ago(1),
    },
)


with dag:
    run_task = SimpleHttpOperator(
        task_id="cloudfunctionhit",
        http_conn_id="cf_mfm_sync_tags",
        method="POST",
        endpoint="",
        data='{}',
        headers={
            "Authorization": f"Bearer {get_id_token(CF_URL_testcloudfunction)}",
            "Content-Type": "application/json",
        }
    )