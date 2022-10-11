from dbacademy_helper import DBAcademyHelper
from dbacademy_helper.workspace_helper import WorkspaceHelper
from typing import TypeVar
T = TypeVar("T")


class ClustersHelper:
    def __init__(self, workspace: WorkspaceHelper, da: DBAcademyHelper):
        self.da = da
        self.client = da.client
        self.workspace = workspace

    def create_instance_pools(self, min_idle_instances: int = 0, idle_instance_autotermination_minutes: int = 15):
        tags = [
            ("dbacademy.event_name", self.da.clean_string(self.workspace.event_name)),
            ("dbacademy.students_count", self.da.clean_string(self.workspace.student_count)),
            ("dbacademy.workspace", self.da.clean_string(self.workspace.workspace_name)),
            ("dbacademy.org_id", self.da.clean_string(self.workspace.org_id)),
            ("dbacademy.course", self.da.clean_string(self.da.course_config.course_name)),
            ("dbacademy.source", self.da.clean_string("Smoke-Test" if self.da.is_smoke_test() else self.da.course_config.course_name))
        ]

        name = "DBAcademy Pool"
        pool = self.client.instance_pools.create_or_update(instance_pool_name=name,
                                                           idle_instance_autotermination_minutes=idle_instance_autotermination_minutes,
                                                           min_idle_instances=min_idle_instances,
                                                           tags=tags)
        instance_pool_id = pool.get("instance_pool_id")

        # With the pool created, make sure that all users can attach to it.
        self.client.permissions.pools.update_group(instance_pool_id, "users", "CAN_ATTACH_TO")

        print(f"Created the pool \"{name}\" ({instance_pool_id})")
        return instance_pool_id

    def _create_cluster_policy(self, instance_pool_id: str, name: str, definition):
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

    def create_all_purpose_policy(self, instance_pool_id: str):
        return self._create_cluster_policy(instance_pool_id, "DBAcademy All-Purpose Policy", {
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

    def create_jobs_policy(self, instance_pool_id: str):
        return self._create_cluster_policy(instance_pool_id, "DBAcademy Jobs-Only Policy", {
            "cluster_type": {
                "type": "fixed",
                "value": "job"
            },
        })

    def create_dlt_policy(self, instance_pool_id: str):
        return self._create_cluster_policy(instance_pool_id, "DBAcademy DLT-Only Policy", {
            "cluster_type": {
                "type": "fixed",
                "value": "dlt"
            },
            "custom_tags.dbacademy.event_name": {
                "type": "fixed",
                "value": self.da.clean_string(self.workspace.event_name),
                "hidden": False
            },
            "custom_tags.dbacademy.students_count": {
                "type": "fixed",
                "value": self.da.clean_string(self.workspace.student_count),
                "hidden": False
            },
            "custom_tags.dbacademy.workspace": {
                "type": "fixed",
                "value": self.da.clean_string(self.workspace.workspace_name),
                "hidden": False
            },
            "custom_tags.dbacademy.org_id": {
                "type": "fixed",
                "value": self.da.clean_string(self.workspace.org_id),
                "hidden": False
            },
            "custom_tags.dbacademy.course": {
                "type": "fixed",
                "value": self.da.clean_string(self.da.course_config.course_name),
                "hidden": False
            },
            "custom_tags.dbacademy.source": {
                "type": "fixed",
                "value": self.da.clean_string("Smoke-Test" if self.da.is_smoke_test() else self.da.course_config.course_name),
                "hidden": False
            },
        })
