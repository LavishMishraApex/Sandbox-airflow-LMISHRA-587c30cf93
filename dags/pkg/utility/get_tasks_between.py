from airflow.models import BaseOperator
from typing import Set


def get_tasks_between(
    upstream_task: BaseOperator, downstream_task: BaseOperator
) -> Set[BaseOperator]:
    """
    returns a list of tasks upstream and downstream, within the same level
    inclusive of the tasks provided
    """

    parent_tasks = set(downstream_task.get_flat_relatives(upstream=True))
    child_tasks = set(upstream_task.get_flat_relatives(upstream=False))
    tasks_between = parent_tasks.intersection(child_tasks)

    return tasks_between
