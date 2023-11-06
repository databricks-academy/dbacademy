__all__ = ["ClustersHelper"]

from typing import Union, Dict, Any
from dbacademy.dbhelper import dbh_constants
from dbacademy.clients.databricks import DBAcademyRestClient


class ClustersHelper:

    def __init__(self, db_academy_rest_client: DBAcademyRestClient):
        from dbacademy.common import validate
        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper

        self.__client = validate.any_value(db_academy_rest_client=db_academy_rest_client, parameter_type=DBAcademyRestClient, required=True)
        self.__workspace = WorkspaceHelper(self.__client)

    def create_instance_pool(self, *,
                             preloaded_spark_version: str,
                             min_idle_instances: int = 0,
                             idle_instance_autotermination_minutes: int = 15,
                             node_type_id: str = None,
                             org_id: str,
                             lab_id: str,
                             workspace_name: str,
                             workspace_description: str) -> str:

        return self.create_named_instance_pool(name=dbh_constants.CLUSTERS_HELPER.POOL_DEFAULT_NAME,
                                               min_idle_instances=min_idle_instances,
                                               idle_instance_autotermination_minutes=idle_instance_autotermination_minutes,
                                               node_type_id=node_type_id,
                                               preloaded_spark_version=preloaded_spark_version,
                                               lab_id=lab_id or self.__workspace.lab_id,
                                               workspace_description=workspace_description or self.__workspace.description,
                                               workspace_name=workspace_name or self.__workspace.workspace_name,
                                               org_id=org_id or self.__workspace.org_id)

    def create_named_instance_pool(self, *,
                                   name: str,
                                   min_idle_instances: int,
                                   idle_instance_autotermination_minutes: int,
                                   lab_id: str,
                                   workspace_description: str,
                                   workspace_name: str,
                                   org_id: str,
                                   node_type_id: str,
                                   preloaded_spark_version: str) -> str:

        from dbacademy import dbgems
        from dbacademy import common
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper

        lab_id = lab_id or WorkspaceHelper.get_lab_id()
        workspace_description = workspace_description or WorkspaceHelper.get_workspace_description()
        workspace_name = workspace_name or WorkspaceHelper.get_workspace_name()
        org_id = org_id or dbgems.get_org_id()

        tags = [
            (f"dbacademy.pool.{dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_ID}", common.clean_string(lab_id)),
            (f"dbacademy.pool.{dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_DESCRIPTION}", common.clean_string(workspace_description)),
            (f"dbacademy.pool.{dbh_constants.WORKSPACE_HELPER.PARAM_WORKSPACE_NAME}", common.clean_string(workspace_name)),
            (f"dbacademy.pool.{dbh_constants.WORKSPACE_HELPER.PARAM_ORG_ID}", common.clean_string(org_id)),
            (f"dbacademy.pool.{dbh_constants.WORKSPACE_HELPER.PARAM_SOURCE}", common.clean_string("Smoke-Test" if DBAcademyHelper.is_smoke_test() else lab_id))
        ]

        # We cannot update some pool attributes once they are created.
        # To address this, we need to delete it then create it.
        self.__client.instance_pools.delete_by_name(name)

        pool = self.__client.instance_pools.create_or_update(instance_pool_name=name,
                                                             idle_instance_autotermination_minutes=idle_instance_autotermination_minutes,
                                                             min_idle_instances=min_idle_instances,
                                                             node_type_id=node_type_id,
                                                             preloaded_spark_version=preloaded_spark_version,
                                                             tags=tags)
        instance_pool_id = pool.get("instance_pool_id")

        # With the pool created, make sure that all users can attach to it.
        self.__client.permissions.pools.update_group(id_value=instance_pool_id,
                                                     group_name="users",
                                                     permission_level="CAN_ATTACH_TO")

        dbgems.display_html(f"""
        <html style="margin:0"><body style="margin:0"><div style="margin:0">
            See <a href="/#setting/clusters/instance-pools/view/{instance_pool_id}" target="_blank">{name} ({instance_pool_id})</a>
        </div></body></html>
        """)

        return instance_pool_id

    def __create_cluster_policy(self, *,
                                instance_pool_id: Union[None, str],
                                name: str,
                                definition: Dict[str, Any]) -> str:

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

        # Some attributes of a policy cannot be updated once created.
        # To get around this, we first delete the policy.
        policy = self.__client.cluster_policies.get_by_name(name)
        if policy:
            self.__client.cluster_policies.delete_by_id(policy["policy_id"])

        policy = self.__client.cluster_policies.create_or_update(name, definition)

        policy_id = policy.get("policy_id")

        # With the pool created, make sure that all users can attach to it.
        self.__client.permissions.cluster_policies.update_group(id_value=policy_id,
                                                                group_name="users",
                                                                permission_level="CAN_USE")

        dbgems.display_html(f"""
        <html style="margin:0"><body style="margin:0"><div style="margin:0">
            See <a href="/#setting/clusters/cluster-policies/view/{policy_id}" target="_blank">{name} ({policy_id})</a>
        </div></body></html>
        """)

        return policy_id

    def create_all_purpose_policy(self, *,
                                  instance_pool_id: str,
                                  spark_version: str,
                                  autotermination_minutes_max: int,
                                  autotermination_minutes_default: int) -> str:

        if type(autotermination_minutes_max) != int or autotermination_minutes_max == 0:
            autotermination_minutes_max = 180

        if type(autotermination_minutes_default) != int or autotermination_minutes_default == 0:
            autotermination_minutes_default = 120

        definition = {
            "cluster_type": {
                "type": "fixed",
                "value": "all-purpose"
            },
            "autotermination_minutes": {
                "type": "range",
                "minValue": 1,
                "maxValue": autotermination_minutes_max,
                "defaultValue": autotermination_minutes_default,
                "hidden": False
            },
            "spark_conf.spark.databricks.cluster.profile": {
                "type": "fixed",
                "value": "singleNode",
                "hidden": False,
            },
            "num_workers": {
                "type": "fixed",
                "value": 0,
                "hidden": False
            },
            "data_security_mode": {
                "type": "unlimited",
                "defaultValue": "SINGLE_USER"
            },
            "runtime_engine": {
                "type": "unlimited",
                "defaultValue": "STANDARD"
            },
        }
        self.add_default_policy(definition, "spark_version", spark_version)

        return self.__create_cluster_policy(instance_pool_id=instance_pool_id,
                                            name=dbh_constants.CLUSTERS_HELPER.POLICY_ALL_PURPOSE,
                                            definition=definition)

    def create_jobs_policy(self, *,
                           instance_pool_id: str,
                           spark_version: str) -> str:

        definition = {
            "cluster_type": {
                "type": "fixed",
                "value": "job"
            },
            "spark_conf.spark.databricks.cluster.profile": {
                "type": "fixed",
                "value": "singleNode",
                "hidden": False,
            },
            "num_workers": {
                "type": "fixed",
                "value": 0,
                "hidden": False
            },
            "data_security_mode": {
                "type": "unlimited",
                "defaultValue": "SINGLE_USER"
            },
            "runtime_engine": {
                "type": "unlimited",
                "defaultValue": "STANDARD"
            },
        }
        ClustersHelper.add_default_policy(definition, "spark_version", spark_version)

        return self.__create_cluster_policy(instance_pool_id=instance_pool_id,
                                            name=dbh_constants.CLUSTERS_HELPER.POLICY_JOBS_ONLY,
                                            definition=definition)

    def create_dlt_policy(self, *,
                          lab_id: str,
                          workspace_description: str,
                          workspace_name: str,
                          org_id: str) -> str:

        from dbacademy import common, dbgems
        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        org_id = org_id or dbgems.get_org_id()
        lab_id = lab_id or WorkspaceHelper.get_lab_id()
        workspace_name = workspace_name or WorkspaceHelper.get_workspace_name()
        workspace_description = workspace_description or WorkspaceHelper.get_workspace_description()

        definition = {
            "cluster_type": {
                "type": "fixed",
                "value": "dlt"
            },
            "spark_conf.spark.databricks.cluster.profile": {
                "type": "fixed",
                "value": "singleNode",
                "hidden": False,
            },
            "num_workers": {
                "type": "fixed",
                "value": 0,
                "hidden": False,
            },
        }

        self.add_custom_tag(definition, dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_ID, lab_id)
        self.add_custom_tag(definition, dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_DESCRIPTION, workspace_description)
        self.add_custom_tag(definition, dbh_constants.WORKSPACE_HELPER.PARAM_SOURCE, "Smoke-Test" if DBAcademyHelper.is_smoke_test() else common.clean_string(lab_id))
        self.add_custom_tag(definition, dbh_constants.WORKSPACE_HELPER.PARAM_ORG_ID, org_id)
        self.add_custom_tag(definition, dbh_constants.WORKSPACE_HELPER.PARAM_WORKSPACE_NAME, workspace_name)

        return self.__create_cluster_policy(instance_pool_id=None, name=dbh_constants.CLUSTERS_HELPER.POLICY_DLT_ONLY, definition=definition)

    @staticmethod
    def add_default_policy(definition: Dict[str, Any], name: str, value: str) -> None:

        if value is None or value.strip() == "":
            from dbacademy import common
            common.print_warning("Invalid Cluster Policy Parameter", f"""The cluster policy parameter "{name}" was not specified; consequently it will be excluded from the cluster policy.""")
            return

        definition[name] = {
            "type": "unlimited",
            "defaultValue": value,
            "isOptional": True
        }

    @classmethod
    def add_custom_tag(cls, definition: Dict[str, Any], name: str, value: str, default_value: str = "UNKNOWN") -> None:
        from dbacademy import common

        key = f"custom_tags.dbacademy.{name}"

        if value is None or value.strip() == "":
            common.print_warning("Invalid Cluster Policy Parameter", f"""The cluster policy parameter "{key}" was not specified; consequently the default value "{default_value}" will be used instead.""")

            value = default_value

        str_value = common.clean_string(value, replacement="_")

        definition[key] = {
            "type": "fixed",
            "value": str_value,
            "hidden": False
        }
