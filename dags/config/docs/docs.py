from string import Template
from textwrap import dedent

TEMPLATE = Template(
    dedent(
        """### $name

$description

$add_info

#### Re-Runnable if failed?  **$rerun**

#### Can skip? **$skip**

Owned by **$owner**. 

Contact **[$slack_name](https://apexclearing.slack.com/archives/$slack_link)** for support.
"""
    )
)

SLACK_CHANNELS = {
    "books-and-records": {
        "name": "Ledger-Engineering",
        "link": "C02AVTUG8NT",  # private ledger channel
        "owner": "books-and-records",
    },
    "datalake": {
        "name": "Datalake",
        "link": "C03BEMYQPC3",  # public datalake channel
        "owner": "datalake",
    },
}


def build_docstring(**kwargs):
    if "owner" in kwargs:
        owner = kwargs["owner"]
        if owner in SLACK_CHANNELS:
            kwargs["slack_name"] = SLACK_CHANNELS[owner]["name"]
            kwargs["slack_link"] = SLACK_CHANNELS[owner]["link"]
    return TEMPLATE.safe_substitute(kwargs)
