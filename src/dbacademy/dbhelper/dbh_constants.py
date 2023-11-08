__all__ = ["WORKSPACE_HELPER", "CLUSTERS_HELPER", "DBACADEMY_HELPER", "WAREHOUSE_HELPER"]


class WorkspaceHelperConstants:

    def __init__(self):
        # TODO convert to properties
        self.WORKSPACE_SETUP_JOB_NAME = "DBAcademy Workspace-Setup"
        self.BOOTSTRAP_JOB_NAME = "DBAcademy Workspace-Setup (Bootstrap)"

        self.PARAM_EVENT_ID = "event_id"
        self.PARAM_EVENT_DESCRIPTION = "event_description"
        self.PARAM_POOLS_NODE_TYPE_ID = "pools_node_type_id"
        self.PARAM_DEFAULT_SPARK_VERSION = "default_spark_version"
        self.PARAM_DATASETS = "datasets"
        self.PARAM_COURSES = "courses"
        self.PARAM_SOURCE = "source"
        self.PARAM_ORG_ID = "org_id"
        self.PARAM_WORKSPACE_NAME = "workspace_name"


class ClustersHelperConstants:
    def __init__(self):
        # TODO convert to properties
        self.POLICY_ALL_PURPOSE = "DBAcademy"
        self.POLICY_JOBS_ONLY = "DBAcademy Jobs"
        self.POLICY_DLT_ONLY = "DBAcademy DLT"
        self.POLICIES = [self.POLICY_ALL_PURPOSE, self.POLICY_JOBS_ONLY, self.POLICY_DLT_ONLY]

        self.POOL_DEFAULT_NAME = "DBAcademy"
        self.POOLS = [self.POOL_DEFAULT_NAME]


class DBAcademyHelperConstants:

    def __init__(self):
        # TODO convert to properties
        self.SCHEMA_DEFAULT = "default"
        self.SCHEMA_INFORMATION = "information_schema"

        self.SPARK_CONF_SMOKE_TEST = "dbacademy.smoke-test"
        self.SPARK_CONF_PATHS_DATASETS = "dbacademy.paths.datasets"
        self.SPARK_CONF_PATHS_USERS = "dbacademy.paths.users"
        self.SPARK_CONF_DATA_SOURCE_URI = "dbacademy.data-source-uri"
        self.SPARK_CONF_PROTECTED_EXECUTION = "dbacademy.protected-execution"
        self.SPARK_CONF_CLUSTER_TAG_SPARK_VERSION = "spark.databricks.clusterUsageTags.sparkVersion"

        self.CATALOG_SPARK_DEFAULT = "spark_catalog"
        self.CATALOG_UC_DEFAULT = "hive_metastore"

        self.TROUBLESHOOT_ERROR_TEMPLATE = """{error} Please see the "Troubleshooting | {section}" section of the "Version Info" notebook for more information."""


class WarehouseHelperConstants:

    def __init__(self):
        # TODO convert to properties
        self.WAREHOUSES_DEFAULT_NAME = "DBAcademy Warehouse"


WORKSPACE_HELPER = WorkspaceHelperConstants()
CLUSTERS_HELPER = ClustersHelperConstants()
DBACADEMY_HELPER = DBAcademyHelperConstants()
WAREHOUSE_HELPER = WarehouseHelperConstants()
