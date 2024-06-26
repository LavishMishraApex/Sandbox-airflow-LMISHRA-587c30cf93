from typing import List
from airflow.models import Variable


def get_client_code_list() -> List[str]:
    def get_correspondent_code_list() -> List[str]:
        _list: List[str] = Variable.get("correspondent_code_list", deserialize_json=True)
        return _list

    correspondent_code_list = get_correspondent_code_list()
    client_code_list = [x.upper() for x in correspondent_code_list]
    _list = list(set(client_code_list))
    return _list