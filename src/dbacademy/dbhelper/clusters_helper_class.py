from typing import TypeVar, Union


class ClustersHelper:
    from .dbacademy_helper_class import DBAcademyHelper
    from .workspace_helper_class import WorkspaceHelper

    T = TypeVar("T")

    POLICY_ALL_PURPOSE = "DBAcademy"
    POLICY_JOBS_ONLY = "DBAcademy Jobs-Only"
    POLICY_DLT_ONLY = "DBAcademy DLT-Only"

    POOL_DEFAULT_NAME = "DBAcademy"

    def __init__(self, workspace: WorkspaceHelper, da: DBAcademyHelper):
        self.da = da
        self.client = da.client
        self.workspace = workspace

    def create_instance_pool(self, min_idle_instances: int = 0, idle_instance_autotermination_minutes: int = 15):
        return ClustersHelper.create_named_instance_pool(name=ClustersHelper.POOL_DEFAULT_NAME,
                                                         client=self.client,
                                                         min_idle_instances=min_idle_instances,
                                                         idle_instance_autotermination_minutes=idle_instance_autotermination_minutes,
                                                         lab_id=self.workspace.lab_id,
                                                         workspace_description=self.workspace.description,
                                                         workspace_name=self.workspace.workspace_name,
                                                         org_id=self.workspace.org_id,
                                                         course_name=self.da.course_config.course_name)

    @staticmethod
    def create_named_instance_pool(*, client, name, min_idle_instances: int, idle_instance_autotermination_minutes: int, lab_id: str, workspace_description: str, workspace_name: str, org_id: str, course_name: str):
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
        from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper

        course_name = course_name or "unknown"

        tags = [
            (f"dbacademy.{WorkspaceHelper.PARAM_LAB_ID}", dbgems.clean_string(lab_id)),
            (f"dbacademy.{WorkspaceHelper.PARAM_DESCRIPTION}", dbgems.clean_string(workspace_description)),
            (f"dbacademy.workspace", dbgems.clean_string(workspace_name)),
            (f"dbacademy.org_id", dbgems.clean_string(org_id)),
            (f"dbacademy.course", dbgems.clean_string(course_name)),
            (f"dbacademy.source", dbgems.clean_string("Smoke-Test" if DBAcademyHelper.is_smoke_test else course_name))
        ]

        pool = client.instance_pools.create_or_update(instance_pool_name=name,
                                                      idle_instance_autotermination_minutes=idle_instance_autotermination_minutes,
                                                      min_idle_instances=min_idle_instances,
                                                      tags=tags)
        instance_pool_id = pool.get("instance_pool_id")

        # With the pool created, make sure that all users can attach to it.
        client.permissions.pools.update_group(instance_pool_id, "users", "CAN_ATTACH_TO")

        print(f"Created the pool \"{name}\" ({instance_pool_id})")
        return instance_pool_id

    def __create_cluster_policy(self, instance_pool_id: Union[None, str], name: str, definition: dict) -> str:
        if instance_pool_id is not None:
            definition["instance_pool_id"] = {
                "type": "fixed",
                "value": instance_pool_id,
                "hidden": False
            }

        if "spark_conf.spark.databricks.cluster.profile" in definition:
            definition["spark_conf.spark.databricks.cluster.profile"] = {
                "type": "fixed",
                "value": "singleNode",
                "hidden": False
            }

        policy = self.client.cluster_policies.create_or_update(name, definition)

        policy_id = policy.get("policy_id")

        # With the pool created, make sure that all users can attach to it.
        self.client.permissions.cluster_policies.update_group(policy_id, "users", "CAN_USE")

        print(f"Created policy \"{name}\" ({policy_id})")
        return policy_id

    def create_all_purpose_policy(self, instance_pool_id: str) -> str:
        return self.__create_cluster_policy(instance_pool_id, ClustersHelper.POLICY_ALL_PURPOSE, {
            "cluster_type": {
                "type": "fixed",
                "value": "all-purpose"
            },
            "autotermination_minutes": {
                "type": "range",
                "minValue": 1,
                "maxValue": 120,
                "defaultValue": 120,
                "hidden": False
            },
        })

    def create_jobs_policy(self, instance_pool_id: str) -> str:
        return self.__create_cluster_policy(instance_pool_id, ClustersHelper.POLICY_JOBS_ONLY, {
            "cluster_type": {
                "type": "fixed",
                "value": "job"
            },
        })

    def create_dlt_policy(self) -> str:
        from dbacademy import dbgems
        from .workspace_helper_class import WorkspaceHelper

        return self.__create_cluster_policy(None, ClustersHelper.POLICY_DLT_ONLY, {
            "cluster_type": {
                "type": "fixed",
                "value": "dlt"
            },
            f"custom_tags.dbacademy.{WorkspaceHelper.PARAM_LAB_ID}": {
                "type": "fixed",
                "value": dbgems.clean_string(self.workspace.lab_id),
                "hidden": False
            },
            f"custom_tags.dbacademy.{WorkspaceHelper.PARAM_DESCRIPTION}": {
                "type": "fixed",
                "value": dbgems.clean_string(self.workspace.description),
                "hidden": False
            },
            "custom_tags.dbacademy.workspace": {
                "type": "fixed",
                "value": dbgems.clean_string(self.workspace.workspace_name),
                "hidden": False
            },
            "custom_tags.dbacademy.org_id": {
                "type": "fixed",
                "value": dbgems.clean_string(self.workspace.org_id),
                "hidden": False
            },
            "custom_tags.dbacademy.course": {
                "type": "fixed",
                "value": dbgems.clean_string(self.da.course_config.course_name),
                "hidden": False
            },
        })
