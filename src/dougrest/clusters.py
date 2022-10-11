from typing import Dict

from dbacademy.dougrest.cluster_policies import ClustersPolicyClient
from dbacademy.rest.common import ApiContainer


class Clusters(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks
        self.policies = ClustersPolicyClient(databricks)

    def get(self, id):
        return self.databricks.api("GET", "2.0/clusters/get", data={"cluster_id": id})

    def list(self):
        response = self.databricks.api("GET", "2.0/clusters/list")
        return response.get("clusters", [])

    def list_by_name(self):
        response = self.databricks.api("GET", "2.0/clusters/list")
        return {c["cluster_name"]: c for c in response.get("clusters", ())}

    def create(self, cluster_name, node_type_id=None, driver_node_type_id=None,
               timeout_minutes=120, num_workers=0, num_cores="*", instance_pool_id=None, spark_version=None,
               start=True, **cluster_spec):
        data = {
            "cluster_name": cluster_name,
            "spark_version": spark_version or self.databricks.default_spark_version,
            "autotermination_minutes": timeout_minutes,
            "num_workers": num_workers,
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
        }
        if self.databricks.cloud == "AWS":
            data["aws_attributes"] = {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
            }
        elif self.databricks.cloud == "Azure":
            data["azure_attributes"] = {
                "first_on_demand": 1,
                "availability": "ON_DEMAND_AZURE",
                "spot_bid_max_price": -1,
            }
        if instance_pool_id:
            data["instance_pool_id"] = instance_pool_id
        else:
            node_type_id = node_type_id or self.databricks.default_machine_type
            driver_node_type_id = driver_node_type_id or node_type_id
            data["node_type_id"] = node_type_id
            data["driver_node_type_id"] = driver_node_type_id
            data["enable_elastic_disk"] = "true"
        if num_workers == 0:
            data["spark_conf"] = {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": f"local[{num_cores}]",
            }
            data["custom_tags"] = {"ResourceClass": "SingleNode"}
        data.update(cluster_spec)
        response = self.databricks.api("POST", "2.0/clusters/create", data)
        if not start:
            self.terminate(response["cluster_id"])
        data["cluster_id"] = response["cluster_id"]
        return data

    def update(self, cluster):
        response = self.databricks.api("POST", "2.0/clusters/edit", cluster)
        return response

    def edit(self, cluster_id, cluster_name=None, *, machine_type=None, driver_machine_type=None,
             timeout_minutes=120, num_workers=0, num_cores="*", instance_pool_id=None,
             spark_version=None, **cluster_spec):
        data = {
            "cluster_id": cluster_id,
            "spark_version": spark_version or self.databricks.default_spark_version,
            "autotermination_minutes": timeout_minutes,
            "num_workers": num_workers,
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
        }
        if cluster_name:
            data["cluster_name"] = cluster_name
        if self.databricks.cloud == "AWS":
            data["aws_attributes"] = {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
            }
        elif self.databricks.cloud == "Azure":
            data["azure_attributes"] = {
                "first_on_demand": 1,
                "availability": "ON_DEMAND_AZURE",
                "spot_bid_max_price": -1,
            }
        if instance_pool_id:
            data["instance_pool_id"] = instance_pool_id
        else:
            machine_type = machine_type or self.databricks.default_machine_type
            driver_machine_type = driver_machine_type or machine_type
            data["node_type_id"] = machine_type
            data["driver_node_type_id"] = driver_machine_type
            data["enable_elastic_disk"] = "true"
        if num_workers == 0:
            data["spark_conf"] = {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": f"local[{num_cores}]",
            }
            data["custom_tags"] = {"ResourceClass": "SingleNode"}
        data.update(cluster_spec)
        response = self.databricks.api("POST", "2.0/clusters/edit", data)
        return cluster_id

    def start(self, id):
        data = {"cluster_id": id}
        response = self.databricks.api("POST", "2.0/clusters/start", data)
        return response

    def restart(self, id):
        data = {"cluster_id": id}
        response = self.databricks.api("POST", "2.0/clusters/restart", data)
        return response

    def terminate(self, id):
        data = {"cluster_id": id}
        response = self.databricks.api("POST", "2.0/clusters/delete", data)
        return response

    def delete(self, id):
        data = {"cluster_id": id}
        response = self.databricks.api("POST", "2.0/clusters/permanent-delete", data)
        return response

    def create_or_start(self, name, machine_type=None, driver_machine_type=None,
                        timeout_minutes=120, num_workers=2, num_cores="*", instance_pool_id=None,
                        existing_clusters=None, cluster_spec=None):
        if existing_clusters is None:
            existing_clusters = self.databricks.clusters.list()
        cluster = next((c for c in existing_clusters if c["cluster_name"] == name), None)
        if not cluster:
            return self.create(name, machine_type, driver_machine_type, timeout_minutes,
                               num_workers, num_cores, instance_pool_id, cluster_spec)
        elif cluster["state"] == "TERMINATED":
            id = cluster["cluster_id"]
            self.edit(cluster_id=id,
                      cluster_name=name,
                      machine_type=machine_type,
                      driver_machine_type=driver_machine_type,
                      timeout_minutes=timeout_minutes,
                      num_workers=num_workers,
                      num_cores=num_cores,
                      instance_pool_id=instance_pool_id)
            self.start(id)
            return id
        else:
            return cluster["cluster_id"]

    def set_acl(self, cluster_id, user_permissions: Dict[str,str] = {}, group_permissions: Dict[str,str] ={}):
        data = {
            "access_control_list": [
                                       {
                                           "user_name": name,
                                           "permission_level": permission,
                                       } for name, permission in user_permissions.items()
                                   ] + [
                                       {
                                           "group_name": name,
                                           "permission_level": permission,
                                       } for name, permission in group_permissions.items()
                                   ]
        }
        return self.databricks.api("PUT", f"2.0/preview/permissions/clusters/{cluster_id}", data)

    def add_to_acl(self, cluster_id, user_permissions: Dict[str,str] = {}, group_permissions: Dict[str,str] = {}):
        data = {
            "access_control_list": [
                                       {
                                           "user_name": user_name,
                                           "permission_level": permission,
                                       } for user_name, permission in user_permissions.items()
                                   ] + [
                                       {
                                           "group_name": group_name,
                                           "permission_level": permission,
                                       } for group_name, permission in group_permissions.items()
                                   ]
        }
        return self.databricks.api("PATCH", f"2.0/preview/permissions/clusters/{cluster_id}", data)
