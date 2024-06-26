from typing import List
from  pkg.utility import get_existing_uris
from  config.globals import GCP_PROJECTS

class FilterKey(object):
    def __init__(self, method, column):
        self.method = method
        self.column = column


class GCPBaseTable(object):
    def __init__(
        self,
        alias: str,
        project_variable: str,
        schema: str,
        name: str,
        keys: List[str],
        filters: List[FilterKey],
    ):
        self.alias = alias
        self.project_variable = project_variable
        self.schema = schema
        self.name = name
        self.keys = keys
        self.filters = filters

        # computed
        self.project = GCP_PROJECTS[self.project_variable]
        self.id = self.project + "." + self.schema + "." + self.name
        self.variable_id = self.project_variable + "." + self.schema + "." + self.name


class GCPExternalTable(GCPBaseTable):
    def __init__(
        self,
        alias: str,
        project_variable: str,
        schema: str,
        name: str,
        uri_list: List[str],
        keys: List[str],
        filters: List[FilterKey],
    ):
        super().__init__(
            alias=alias,
            project_variable=project_variable,
            schema=schema,
            name=name,
            keys=keys,
            filters=filters,
        )
        self.uri_list = uri_list
        self.existing_uris = get_existing_uris(self.uri_list, self.project)


class GCPBucket(object):
    def __init__(
        self,
        alias: str,
        project_variable: str,
        bucket_suffix: str,
        gcs_object: str,
        external_tables: List[GCPExternalTable],
    ):
        # raw
        self.alias = alias
        self.project_variable = project_variable
        self.bucket_suffix = bucket_suffix
        self.gcs_object = gcs_object
        self.external_tables = external_tables

        # computed
        self.project = GCP_PROJECTS[self.project_variable]
        self.bucket = self.project + self.bucket_suffix
        self.id = self.bucket + "/" + self.gcs_object
        self.uri = "gs://" + self.id
