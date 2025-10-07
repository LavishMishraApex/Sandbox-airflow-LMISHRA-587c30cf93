

from ast import Param
import pytz
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
import logging
from airflow.exceptions import AirflowException
from pkg.utility.dhp_functionalities import *
from pkg.utility.bigquery_functionalities import *
# path needs to be changed
from pkg.utility.astro_log_interactions import return_latest_trigger_username

dag_params = {"publish_dict": {}, "sql": "",
              "is_healthy_value_to_publish": "", "jira_ticket": ""}


def check_if_block_update(**kwargs):
    if not ("publish_dict" in kwargs['dag_run'].conf and kwargs['dag_run'].conf["publish_dict"] != {}):
        ti = kwargs['ti']
        jira_ticket = ti.xcom_pull(
            task_ids='jira_ticket_validation', key='jira_ticket')
        sql = kwargs['dag_run'].conf.get("sql", "")
        result = list(run_query(sql))
        total_records = len(result)
        latest_triggered_user = ti.xcom_pull(
            task_ids='store_latest_triggered_user', key='latest_triggered_user')
        raise AirflowException("""This DAG is being used to update multiple records. this task is failed as a check User {} is trying to update {} records with Jira ticket {}, if you feel this is correct, feel free to mark this task as Success and all those records would be updated""".format(
            latest_triggered_user, total_records, jira_ticket))
    else:
        logging.info(
            "DAG is being used to update just one record, proceeding with publish")


def jira_ticket_validation(**kwargs):
    if "jira_ticket" not in kwargs['dag_run'].conf or kwargs['dag_run'].conf["jira_ticket"] == "":
        raise AirflowException(
            "jira_ticket must be provided as part of DAG arguments")
    else:
        logging.info(
            f"Jira ticket provided is {kwargs['dag_run'].conf['jira_ticket']}")
        jira_ticket = kwargs['dag_run'].conf['jira_ticket']
        if 'NDP' not in jira_ticket and 'TECHOPS' not in jira_ticket:
            raise AirflowException(
                "jira_ticket must be a valid NDP or TECHOPS ticket")
        ti = kwargs['ti']
        ti.xcom_push(key='jira_ticket', value=jira_ticket)


def store_latest_trigger_user_function(dag_id: str, **kwargs):
    username = return_latest_trigger_username(dag_id)
    logging.info(f"Latest triggered user is {username}")
    ti = kwargs['ti']
    ti.xcom_push(key='latest_triggered_user', value=username)
    return username


def publish_single_healthy_status_to_dhp(dhp_publish_dict: dict):
    if "description" not in dhp_publish_dict:
        dhp_publish_dict["description"] = "Update to make status healthy"
    if "publisher" not in dhp_publish_dict:
        dhp_publish_dict["publisher"] = "gcp-dataplatform@apexclearing.com"
    logging.info(f"DHP publish dict: {dhp_publish_dict}")
    is_dhp_publish_success, response_json = publish_report_to_dhp_v2(
        dhp_publish_dict)
    if not is_dhp_publish_success:
        logging.error("Failed to publish DHP report for table , status_code is {}, response is {}".format(
            response_json.status_code, response_json.text))
    else:
        logging.info("Successfully published healthy status to DHP for request {}".format(
            dhp_publish_dict))
    return is_dhp_publish_success, response_json


def publish_status_to_dhp(**kwargs):
    ti = kwargs['ti']
    latest_triggered_user = ti.xcom_pull(
        task_ids='store_latest_triggered_user', key='latest_triggered_user')
    logging.info(
        f"Latest triggered user pulled from XCom is {latest_triggered_user}")
    jira_ticket = ti.xcom_pull(
        task_ids='jira_ticket_validation', key='jira_ticket')
    if "publish_dict" in kwargs['dag_run'].conf and kwargs['dag_run'].conf["publish_dict"] != {}:
        publish_dict = kwargs['dag_run'].conf["publish_dict"]
        if publish_dict["report_name"] == "data_asset_heatlth":
            is_healthy_key = "is_healthy"
        else:
            is_healthy_key = "tests_passed"
        if publish_dict["report_details"][is_healthy_key].lower() == "false":
            publish_dict["report_details"][is_healthy_key] = False
        else:
            publish_dict["report_details"][is_healthy_key] = True
        publish_dict["description"] += "!!Adhoc request to update status by user {}!!".format(
            latest_triggered_user)
        publish_dict["description"] += " JIRA Ticket: {}".format(
            jira_ticket)
        is_publish_success, response_json = publish_single_healthy_status_to_dhp(
            publish_dict)
        if not is_publish_success:
            raise AirflowException("Failed to publish DHP report, status_code is {}, response is {}".format(
                response_json.status_code, response_json.text))
    elif "sql" in kwargs['dag_run'].conf and kwargs['dag_run'].conf["sql"] != "":
        logging.info(
            f"kwargs dag_run keys: {kwargs['dag_run'].conf.keys()}")
        is_healthy_value_to_publish = kwargs['dag_run'].conf.get(
            "is_healthy_value_to_publish")
        failed_to_publish_count = 0
        if is_healthy_value_to_publish and is_healthy_value_to_publish.lower() == "false":
            logging.info(
                f"Unhealthy status will be published for all request to DHP")
            is_healthy_value_to_publish = False
        else:
            logging.info(
                f"Healthy status will be published for all request to DHP")
            is_healthy_value_to_publish = True
        sql = kwargs['dag_run'].conf["sql"]
        result = list(run_query(sql))
        if not result:
            logging.info("No unhealthy DHP statuses found.")
            return
        logging.info(f"Found {len(result)} requests to publish.")
        for row in result:
            logging.info(f"Processing row: {row}")
            dhp_publish_dict = {
                "project_id": row["project_id"],
                "report_name": row["report_name"],
                "description": "!!Adhoc request to update status by user {}!!".format(
                    latest_triggered_user),
                "publisher": "gcp-dataplatform@apexclearing.com",
                "report_details":
                {"dataset_name": row["dataset_name"],
                    "table_name": row["table_name"],
                    "process_date": row["process_date"],
                    "is_healthy": is_healthy_value_to_publish}
            }
            logging.info(
                "publish_dict created is {}".format(dhp_publish_dict))
            dhp_publish_dict["description"] += " JIRA Ticket: {}".format(
                jira_ticket)
            is_publish_success, response_json = publish_single_healthy_status_to_dhp(
                dhp_publish_dict)
            if not is_publish_success:
                failed_to_publish_count += 1
        if failed_to_publish_count > 0:
            raise AirflowException(
                f"Failed to publish DHP report for {failed_to_publish_count} requests, please check the logs above for details")
    else:
        raise AirflowException(
            "Either publish_dict or sql must be provided in as part of DAG arguments")


def create_dag(dag_id, schedule):
    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=datetime(
            2024, 6, 24, tzinfo=pytz.timezone('US/Central')),
        max_active_runs=1,
        catchup=False,
        tags=["team:datalake"],
        default_args={
            "owner": "datalake",
            "retries": 3,
        },
        params=dag_params
    )
    with dag:
        store_latest_trigger_user = PythonOperator(
            task_id="store_latest_triggered_user",
            python_callable=store_latest_trigger_user_function,
            op_kwargs={'dag_id': dag_id},
            dag=dag,
            provide_context=True
        )
        jira_ticket_validation_task = PythonOperator(
            task_id="jira_ticket_validation",
            python_callable=jira_ticket_validation,
            dag=dag,
            provide_context=True,
            retries=0
        )
        block_update_check = PythonOperator(
            task_id="block_update_check",
            python_callable=check_if_block_update,
            dag=dag,
            provide_context=True,
            retries=0
        )
        update_all_unhealthy_dhp_status = PythonOperator(
            task_id="update_all_unhealthy_dhp_status",
            python_callable=publish_status_to_dhp,
            dag=dag,
            provide_context=True
        )
        store_latest_trigger_user >> jira_ticket_validation_task >> block_update_check >> update_all_unhealthy_dhp_status
        return dag


dag_id = "datalake_update_unhealthy_dhp_status"
schedule = None
dag = create_dag(dag_id, schedule)
