from airflow.utils.state import State
from typing import Set


def parse_status_set(status_set: Set[str]) -> str:
    """
    TURNS A SET OF STATUSES INTO A SINGLE STATUS
    """
    if bool(status_set & State.failed_states):  # if any failed
        status = State.FAILED
    elif status_set.issubset(State.success_states):  # if all success states
        status = State.SUCCESS
    elif status_set.issubset(State.SKIPPED):
        status = State.SKIPPED
    else:
        # shouldn't happen
        status = "unknown"
    return status
