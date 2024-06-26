import logging
from airflow.exceptions import AirflowFailException
from  pkg.utility import get_id_token
import requests

'''
This function can be used directly from Airflow Operators to trigger a cloud function and fetch a response
cf_url:- URL of the cloud function
cloud_function_input:- Json key value pairs that needs to be inputted to the cloud function
If an error occurs, it would be handled by the function and AirflowFailException would be thrown after logging the error details
If successful execution occurs, then the response return from cloud function is return back.

'''
def trigger_cloud_function(cf_url, cloud_function_input={}):
    headers = {
        "Authorization": f"Bearer {get_id_token(cf_url)}",
        "Content-Type": "application/json",
    }
    http_response = requests.post(cf_url, json=cloud_function_input, headers=headers).json()
    logging.info("response is ")
    logging.info(http_response)
    return http_response
