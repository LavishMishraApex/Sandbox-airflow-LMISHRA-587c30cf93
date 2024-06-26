from airflow import DAG
from airflow.models import BaseOperator, TaskInstance
from airflow.models.dagrun import DagRun
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import get_current_context, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException

from typing import Union
from datetime import datetime

from  pkg.utility import parse_status_set
from  config.globals import GCP_PROJECTS, ENVIRONMENT

DATALAKE_MGMT = GCP_PROJECTS["DATALAKE_MGMT"]

def log_group(group: Union[TaskGroup, DAG], process_date: str):
    """
    ADDS A LOG TASK AS THE FIRST AND LAST TASK OF A `TaskGroup` OBJECT
    'log_start' and 'log_end' TASKS ARE ADDED INSIDE THE TASKGROUP
    """

    def update_log(
            process_name: str,
            task_id: str,
            status: str,
            description,
            process_date: str,
    ) -> None:
        """
        WRITES A LOG RECORD TO A BIGQUERY TABLE <DATALAKE_MGMT>.general.processes_status_log
        """
        query_template = "INSERT INTO `{}.general.processes_status_log` (process_name, task_id, status, description, process_date, recorded_at) SELECT '''{}''', '''{}''', '''{}''', '''{}''', CAST('{}' AS DATE), CURRENT_TIMESTAMP()"
        query = query_template.format(
            DATALAKE_MGMT, process_name, task_id, status, description, datetime.strptime(process_date, "%Y-%m-%d").date()
        )
        print(f"QUERY WAS: {query}")
        hook = BigQueryHook(
            gcp_conn_id="datalake_gcp_mgmt_id",
            use_legacy_sql=False,
            impersonation_chain=f"airflow@apex-airflow-{ENVIRONMENT}-00.iam.gserviceaccount.com"
        )
        hook.insert_job(configuration={"query": {"query": query, "useLegacySql": False}})

    def find_tasks_to_log(tg: TaskGroup):
        context = get_current_context()
        dag_run: DagRun = context["dag_run"]

        tasks_to_log = dict()  # {task_id: state}
        children = tg.children
        for child_id in children:
            child = tg.children[child_id]
            if child.label not in ["log_start", "log_end", "status_light"]:
                parse_tasks = set()
                if type(child) == TaskGroup:
                    for _leaf in child.leaves:
                        parse_tasks.add(dag_run.get_task_instance(_leaf.task_id))
                else:
                    parse_tasks.add(dag_run.get_task_instance(child_id))
                status_set = set()
                for _task in parse_tasks:
                    status_set.add(_task.state)
                tasks_to_log[child_id] = parse_status_set(status_set)
        return tasks_to_log

    def log_start_func(tg: TaskGroup, process_date: str):
        context = get_current_context()
        dag_run: DagRun = context["dag_run"]
        tasks_to_log = find_tasks_to_log(tg=tg)
        group_id = tg.group_id
        status = parse_status_set(set(tasks_to_log.values()))
        description = [dag_run.run_id, tasks_to_log]
        update_log(
            process_name=dag_run.dag_id,
            task_id=group_id,
            status=status,
            description=description,
            process_date=process_date,
        )

    def log_end_func(tg: TaskGroup, process_date: str):
        context = get_current_context()
        dag_run: DagRun = context["dag_run"]

        # skip if log_start was skipped
        log_start_task = tg.get_child_by_label("log_start")
        log_start_task_instance: TaskInstance = dag_run.get_task_instance(
            task_id=log_start_task.task_id
        )
        if log_start_task_instance.state == State.SKIPPED:
            raise AirflowSkipException

        tasks_to_log = find_tasks_to_log(tg=tg)
        group_id = tg.group_id
        status = parse_status_set(set(tasks_to_log.values()))
        description = [dag_run.run_id, tasks_to_log]
        update_log(
            process_name=dag_run.dag_id,
            task_id=group_id,
            status=status,
            description=description,
            process_date=process_date,
        )

        if status == State.FAILED:
            status_light_task: BaseOperator = tg.get_child_by_label("status_light")
            status_light_task_instance = dag_run.get_task_instance(
                task_id=status_light_task.task_id
            )
            status_light_task_instance.set_state(state=State.UPSTREAM_FAILED)

    log_start_task_id = "log_start"
    log_end_task_id = "log_end"

    if type(group) == DAG:
        tg = group.task_group
    else:
        tg = group
    op_kwargs = {"tg": tg, "process_date": process_date}

    root_tasks = tg.roots
    leaf_tasks = tg.leaves
    children = tg.children
    roots = []
    leaves = []
    for child_id in children:
        child = tg.children[child_id]
        if type(child) == TaskGroup:
            for _root in child.roots:
                if _root in root_tasks:
                    roots = roots + [child]
            for _leaf in child.leaves:
                if _leaf in leaf_tasks:
                    leaves = leaves + [child]
        else:
            if child in root_tasks:
                roots = roots + [child]
            if child in leaf_tasks:
                leaves = leaves + [child]

    log_start = PythonOperator(
        task_id=log_start_task_id,
        task_group=tg,
        python_callable=log_start_func,
        op_kwargs=op_kwargs,
        retries=3,
    )
    log_start >> roots

    log_end = PythonOperator(
        task_id=log_end_task_id,
        task_group=tg,
        python_callable=log_end_func,
        op_kwargs=op_kwargs,
        retries=3,
        trigger_rule="all_success",
    )

    leaves >> log_end
