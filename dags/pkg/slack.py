from airflow.operators.bash import BashOperator
from  config.globals import SLACK_URL

# TO-DO need to replace this with secret manger value
DL_SLACK_URL = (
    "https://hooks.slack.com/services/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"
)


def task_fail_slack_alert(context):
    slack_msg = '"icon_emoji": ":airflow:", "text": "*Dag:* *{dag}* FAILED for task: *{task}*\n*Execution Date:* {exec_date}\n*URL:* {log_url}"'.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
    slack_command = (
        "curl -X POST --data-urlencode 'payload={" + slack_msg + "}' " + SLACK_URL
    )
    failed_alert = BashOperator(task_id="slack_failed", bash_command=slack_command)
    return failed_alert.execute(context=context)


def task_fail_slack_alert_red(context):
    slack_msg = '"icon_emoji": ":airflow:", "text": ":red_circle: *Dag:* *{dag}* FAILED for task: *{task}*\n*Execution Date:* {exec_date}\n*URL:* {log_url}"'.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
    slack_command = (
        "curl -X POST --data-urlencode 'payload={" + slack_msg + "}' " + DL_SLACK_URL
    )
    failed_alert = BashOperator(task_id="slack_failed", bash_command=slack_command)
    return failed_alert.execute(context=context)


def send_slack(context, message):
    slack_command = (
        'curl -X POST --data-urlencode \'payload={"icon_emoji": ":airflow:", "text": "'
        + message
        + "\"}' "
        + SLACK_URL
    )
    slack_task = BashOperator(task_id="send_slack", bash_command=slack_command)
    return slack_task.execute(context=context)
