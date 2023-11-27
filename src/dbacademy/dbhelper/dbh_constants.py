__all__ = ["WORKSPACE_HELPER", "CLUSTERS_HELPER", "DBACADEMY_HELPER", "WAREHOUSE_HELPER"]

from typing import List


# noinspection PyPep8Naming
class WorkspaceHelperConstants:

    @property
    def WORKSPACE_SETUP(self) -> str:
        return "Workspace-Setup"

    @property
    def UNIVERSAL_WORKSPACE_SETUP(self) -> str:
        return "Universal-Workspace-Setup"

    @property
    def WORKSPACE_SETUP_JOB_NAME(self) -> str:
        # return f"DBAcademy's {self.UNIVERSAL_WORKSPACE_SETUP}"
        return "DBAcademy Workspace-Setup"

    @property
    def PARAM_EVENT_ID(self) -> str:
        return "event_id"

    @property
    def PARAM_EVENT_DESCRIPTION(self) -> str:
        return "event_description"

    @property
    def PARAM_POOLS_NODE_TYPE_ID(self) -> str:
        return "pools_node_type_id"

    @property
    def PARAM_DEFAULT_SPARK_VERSION(self) -> str:
        return "default_spark_version"

    @property
    def PARAM_DATASETS(self) -> str:
        return "datasets"

    @property
    def PARAM_COURSES(self) -> str:
        return "courses"

    @property
    def PARAM_SOURCE(self) -> str:
        return "source"

    @property
    def PARAM_ORG_ID(self) -> str:
        return "org_id"

    @property
    def PARAM_WORKSPACE_NAME(self) -> str:
        return "workspace_name"


# noinspection PyPep8Naming
class ClustersHelperConstants:

    @property
    def POLICY_ALL_PURPOSE(self) -> str:
        return "DBAcademy"

    @property
    def POLICY_JOBS_ONLY(self) -> str:
        return "DBAcademy Jobs"

    @property
    def POLICY_DLT_ONLY(self) -> str:
        return "DBAcademy DLT"

    @property
    def POLICIES(self) -> List[str]:
        return [self.POLICY_ALL_PURPOSE,
                self.POLICY_JOBS_ONLY,
                self.POLICY_DLT_ONLY]

    @property
    def POOL_DEFAULT_NAME(self) -> str:
        return "DBAcademy"

    @property
    def POOLS(self) -> List[str]:
        return [self.POOL_DEFAULT_NAME]


# noinspection PyPep8Naming
class DBAcademyHelperConstants:

    @property
    def SCHEMA_DEFAULT(self) -> str:
        return "default"

    @property
    def SCHEMA_INFORMATION(self) -> str:
        return "information_schema"

    @property
    def SPARK_CONF_SMOKE_TEST(self) -> str:
        return "dbacademy.smoke-test"

    @property
    def SPARK_CONF_PATHS_DATASETS(self) -> str:
        return "dbacademy.paths.datasets"

    @property
    def SPARK_CONF_PATHS_USERS(self) -> str:
        return "dbacademy.paths.users"

    @property
    def SPARK_CONF_DATA_SOURCE_URI(self) -> str:
        return "dbacademy.data-source-uri"

    @property
    def SPARK_CONF_PROTECTED_EXECUTION(self) -> str:
        return "dbacademy.protected-execution"

    @property
    def SPARK_CONF_CLUSTER_TAG_SPARK_VERSION(self) -> str:
        return "spark.databricks.clusterUsageTags.sparkVersion"

    @property
    def CATALOG_SPARK_DEFAULT(self) -> str:
        return "spark_catalog"

    @property
    def CATALOG_UC_DEFAULT(self) -> str:
        return "hive_metastore"

    @property
    def TROUBLESHOOT_ERROR_TEMPLATE(self) -> str:
        return """{error} Please see the "Troubleshooting | {section}" section of the "Version Info" notebook for more information."""


# noinspection PyPep8Naming
class WarehouseHelperConstants:

    @property
    def WAREHOUSES_DEFAULT_NAME(self) -> str:
        return "DBAcademy Warehouse"


WORKSPACE_HELPER: WorkspaceHelperConstants = WorkspaceHelperConstants()
CLUSTERS_HELPER: ClustersHelperConstants = ClustersHelperConstants()
DBACADEMY_HELPER: DBAcademyHelperConstants = DBAcademyHelperConstants()
WAREHOUSE_HELPER: WarehouseHelperConstants = WarehouseHelperConstants()
