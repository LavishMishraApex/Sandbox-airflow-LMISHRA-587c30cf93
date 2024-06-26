from airflow.models import Variable


def variable_exists(variable: str) -> bool:
    """
    check if an airflow variable exists
    """
    it_exists = False
    try:
        Variable.get(variable)
        it_exists = True
    except KeyError:
        pass
    return it_exists
