import ast

ENVIRONMENT = "{{ data.env }}"
GCP_PROJECTS = ast.literal_eval("{{ data.gcp_projects }}")
CF_URL_EOD_VALIDATE_RAW_ROW_COUNT = "{{ data.cf_url_eod_validate_raw_row_count }}"
CF_URL_EOD_CHECK_FRESHNESS="{{data.cf_url_eod_check_freshness}}"
SLACK_URL = "{{ data.slack }}"
CLOUDRUN_URL = "{{ data.cloudrun_url }}"
CF_URL_SNAPSHOT_SERVICE = "{{ data.cf_url_snapshot_service }}"
CF_URL_CHECK_TRADING_DAY="{{data.cf_url_check_trading_day}}"
AIRFLOW_SA = f"airflow@apex-airflow-{ENVIRONMENT}-00.iam.gserviceaccount.com"
