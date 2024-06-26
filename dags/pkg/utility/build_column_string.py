from typing import List


def build_column_string(column_list: List[str]) -> str:
    if column_list:
        columns = [f"{key} AS {key}" for key in column_list]
        columns = ", ".join(columns) + ","
    else:
        columns = ""
    return columns
