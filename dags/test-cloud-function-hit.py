from airflow import DAG
#from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging


'''
Following imports are from folders within the same repo, these need to be changed before we go for dev deployment
'''
from dags.plugins import get_id_token
from dags.plugins.trigger_cloud_function import trigger_cloud_function

DAG_ID="datalake_test_dag"

tablefrequencydict={}

CF_URL_testcloudfunction = """https://us-central1-apex-datalake-mgmt-dev-00.cloudfunctions.net/mfm-find-all-tag-locations-for-column"""

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    catchup=False,
    tags=["team:datalake", "testing"],
    doc_md="""This Dag can be used to trigger cloud function from airflow environment, fetch reponse from it. Response from the cloud function should be a json dict which has keys 'text' and 'status_code' from requests.Response() object
    Required parameters to hit the cloud function
    'cloud_function_input' :- Input that would be passed to the cloud function. 
    'cf_url' :- URL of the Cloud function you are trying to hit
    Please ensure that the Airflow service account has access to trigger the Cloud Function
    Example:-
    {"cf_url" : "https://us-central1-apex-datalake-mgmt-dev-00.cloudfunctions.net/mfm-list-policy-tags", "input_to_cloud_function" : {}}
    """,
    default_args={
        "owner": "team:datalake",
        "depends_on_past": False,
        "start_date": days_ago(1),
    },
)
def list_datasets(project: str) -> list:
    client = get_bigquery_client()

    datasets = list(client.list_datasets(project))  # Make an API request.

    if not datasets:
        logging.info("{} project does not contain any datasets.".format(project))
    return datasets
def check_cloud_function_trigger_method(**kwargs):
    cloud_function_input = kwargs['dag_run'].conf.get('cloud_function_input')
    CF_URL = kwargs['dag_run'].conf.get('cf_url')
    response = trigger_cloud_function(CF_URL, cloud_function_input)
    logging.info("returned response 'text': {}, 'status_code' : {} ".format(response.text, response.status_code))
with dag:
    check_cloud_function_trigger_task = PythonOperator(task_id="check_cloud_function_trigger",
                                       python_callable=check_cloud_function_trigger_method,
                                       provide_context=True)