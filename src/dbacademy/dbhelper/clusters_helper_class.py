from typing import TypeVar, Union


class ClustersHelper:
    from dbacademy.dbrest import DBAcademyRestClient
    from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
    from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper

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
                                                         org_id=self.workspace.org_id)

    @staticmethod
    def create_named_instance_pool(*, client: DBAcademyRestClient, name, min_idle_instances: int, idle_instance_autotermination_minutes: int, lab_id: str = None, workspace_description: str = None, workspace_name: str = None, org_id: str = None):
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
        from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper

        lab_id = lab_id or WorkspaceHelper.get_lab_id()
        workspace_description = workspace_description or WorkspaceHelper.get_workspace_description()
        workspace_name = workspace_name or WorkspaceHelper.get_workspace_name()
        org_id = org_id or dbgems.get_org_id()

        tags = [
            (f"dbacademy.{WorkspaceHelper.PARAM_LAB_ID}", dbgems.clean_string(lab_id)),
            (f"dbacademy.{WorkspaceHelper.PARAM_DESCRIPTION}", dbgems.clean_string(workspace_description)),
            (f"dbacademy.workspace", dbgems.clean_string(workspace_name)),
            (f"dbacademy.org_id", dbgems.clean_string(org_id)),
            (f"dbacademy.source", dbgems.clean_string("Smoke-Test" if DBAcademyHelper.is_smoke_test() else dbgems.clean_string(lab_id)))
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

    @staticmethod
    def __create_cluster_policy(*, client: DBAcademyRestClient, instance_pool_id: Union[None, str], name: str, definition: dict) -> str:
        from dbacademy import dbgems
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

        policy = client.cluster_policies.create_or_update(name, definition)

        policy_id = policy.get("policy_id")

        # With the pool created, make sure that all users can attach to it.
        client.permissions.cluster_policies.update_group(policy_id, "users", "CAN_USE")

        print(f"Created policy \"{name}\" ({policy_id})")
        dbgems.display_html(f"""
        <html><body><div>
            See <a href="/#setting/clusters/instance-pools/view/{policy_id}" target="_blank">{name} ({policy_id})</a>
        </div></body></html>
        """)

        return policy_id

    @staticmethod
    def create_all_purpose_policy(*, client: DBAcademyRestClient, instance_pool_id: str) -> None:
        ClustersHelper.__create_cluster_policy(client=client, instance_pool_id=instance_pool_id, name=ClustersHelper.POLICY_ALL_PURPOSE, definition={
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

    @staticmethod
    def create_jobs_policy(*, client: DBAcademyRestClient, instance_pool_id: str) -> None:
        ClustersHelper.__create_cluster_policy(client=client, instance_pool_id=instance_pool_id, name=ClustersHelper.POLICY_JOBS_ONLY, definition={
            "cluster_type": {
                "type": "fixed",
                "value": "job"
            },
        })

    @staticmethod
    def create_dlt_policy(*, client: DBAcademyRestClient, instance_pool_id: str, lab_id: str = None, workspace_description: str = None, workspace_name: str = None, org_id: str = None) -> None:
        from dbacademy import dbgems
        from .workspace_helper_class import WorkspaceHelper

        lab_id = lab_id or WorkspaceHelper.get_lab_id()
        workspace_description = workspace_description or WorkspaceHelper.get_workspace_description()
        workspace_name = workspace_name or WorkspaceHelper.get_workspace_name()
        org_id = org_id or dbgems.get_org_id()

        ClustersHelper.__create_cluster_policy(client=client, instance_pool_id=instance_pool_id, name=ClustersHelper.POLICY_DLT_ONLY, definition={
            "cluster_type": {
                "type": "fixed",
                "value": "dlt"
            },
            f"custom_tags.dbacademy.{WorkspaceHelper.PARAM_LAB_ID}": {
                "type": "fixed",
                "value": dbgems.clean_string(lab_id),  # self.workspace.lab_id),
                "hidden": False
            },
            f"custom_tags.dbacademy.{WorkspaceHelper.PARAM_DESCRIPTION}": {
                "type": "fixed",
                "value": dbgems.clean_string(workspace_description),  # self.workspace.description),
                "hidden": False
            },
            "custom_tags.dbacademy.workspace": {
                "type": "fixed",
                "value": dbgems.clean_string(workspace_name),  # self.workspace.workspace_name),
                "hidden": False
            },
            "custom_tags.dbacademy.org_id": {
                "type": "fixed",
                "value": dbgems.clean_string(org_id),  # self.workspace.org_id),
                "hidden": False
            },
        })
