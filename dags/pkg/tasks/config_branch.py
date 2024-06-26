from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.operators.python import (
    BranchPythonOperator,
    get_current_context,
)

from typing import List


def config_branch(conf_keyword: str, strict: bool = True) -> BranchPythonOperator:
    """
    Returns a branch operator
    param: config_keyword is the word in the dag config.params that it will use for desired branches
    param: strict determines whether or not it should fail if a search-for branch isn't found
    function: Searches for any task or task_group called ABC that is both immediately downstream and at the same taskgroup level as the operator
    notes:
    Assuming this operator is in a taskgroup called "family"
    If searching for "ABC"
        Triggers:
            family.ABC,
            family.ABC.Task1
            family.ABC.XYZ.Task_1 (if immediately downstream)
            family.ABC.*
        Doesn't Trigger:
            family.ABCX
            family.task_1.ABC
            XYZ.ABC
            ABC
    """

    def _config_branch(conf_keyword: str, strict: bool):
        context = get_current_context()
        this_task = context["task"]
        try:
            conf_input = context["dag_run"].conf[conf_keyword]
            print(f"\nBRANCH CONFIG WAS: {conf_keyword}: {conf_input}\n")
        except KeyError:
            # triggers when the config that called the dag didn't have the argument
            # primarily used so that if the dag is triggered by another, it doesn't have to pass a full config
            conf_input = ""

        if type(conf_input) == str:
            conf_input = [conf_input]

        # join the config labels to the current family_id (handles nesting of branch function in a task group)
        family_id = this_task.task_id[0: -(len(this_task.label))]
        selected_prefix_list: List[str] = []
        for label in conf_input:
            selected_prefix_list = selected_prefix_list + [family_id + label]

        # get all possible branches from within the task group
        available_tasks: List[BaseOperator] = []
        available_task_ids: List[str] = []
        for task in this_task.downstream_list:
            group_id = task.task_id[0: -(len(task.label))]  # strip label
            if (
                group_id.find(family_id) == 0
            ):  # checks that it is within the same task group
                available_task_ids = available_task_ids + [task.task_id]
                available_tasks = available_tasks + [task]

        # match the family + config value (=prefix) to tasks.
        selected_task_ids: List[str] = []
        missing_ids: List[str] = []
        for prefix in selected_prefix_list:
            prefix_found = False
            for task in available_tasks:
                if prefix == task.task_id:
                    prefix_found = True
                    selected_task_ids = selected_task_ids + [task.task_id]
                else:
                    group_id = task.task_id[0: -(len(task.label))]  # strip label
                    if (
                        group_id.find(prefix) == 0
                    ):  # if the prefix searched for is a prefix to the task
                        prefix_found = True
                        selected_task_ids = selected_task_ids + [task.task_id]
            if not prefix_found:  # if the prefix is not found mark it
                missing_ids = missing_ids + [prefix]

        print(f"\nAVAILABLE TASK BRANCHES: {available_task_ids}\n")
        if conf_input == [""]:
            branch_ids = available_task_ids
            print(f"\nSELECTED TASK BRANCHES FROM DEFAULT: {branch_ids}\n")
        else:
            branch_ids = selected_task_ids
            print(f"\nSELECTED TASK BRANCHES FROM CONFIG: {branch_ids}\n")

        # if any values weren't found, fail
        if missing_ids:
            msg = f"\nSELECTED TASK BRANCHES FROM CONFIG: {branch_ids}\n"
            if strict:
                raise AirflowFailException(msg)
            else:
                print(msg)
        return branch_ids

    run_task = BranchPythonOperator(
        task_id=f"branch__{conf_keyword}",
        python_callable=_config_branch,
        op_kwargs={"conf_keyword": conf_keyword, "strict": strict},
        retries=0,
    )
    return run_task
