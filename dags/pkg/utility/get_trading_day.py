import logging
from airflow.exceptions import AirflowFailException
from  pkg.utility.trigger_cloud_function import trigger_cloud_function
'''
This function is used for fetching whether its a trading day on a particular process date.
variable expected. CF_URL:- The cloud function to hit for checking the trading day https://apexclearing.atlassian.net/wiki/spaces/NDP/pages/6491701301/Cloud+Function+datalake+check+trading+day
                    process_date:- date that needs to be checked for trading_day in 'mm-dd-yyyy' format, for ex:- '05-15-2023'
Output:- It returns whether True of False on whether its a trading day, also it logs the error and raises exception if Cloud function didn't return a correct response.
'''
def get_trading_day(cf_url, process_date):
    is_trading_day = False
    cloud_function_input = {}
    cloud_function_input['date'] = process_date
    http_response = trigger_cloud_function(cf_url, cloud_function_input)
    if http_response['status'] == 200:
        if http_response['trading_day']:
            logging.info("Continuing execution as a trading day")
            is_trading_day = True
    else:
        logging.info("Cloud Function return wrong http_response, please check logs", http_response)
        raise AirflowFailException
    return is_trading_day

