from typing import Optional, Dict, Any, List
from enum import Enum
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer
from dbacademy import common


class Availability(Enum):
    SPOT = "SPOT"
    FALL_BACK = "FALL_BACK"
    ON_DEMAND = "ON_DEMAND"


class ClusterConfig:

    def __init__(self, *,
                 cluster_name: Optional[str],
                 spark_version: str,
                 node_type_id: Optional[str],
                 driver_node_type_id: str = None,
                 instance_profile_id: str = None,
                 num_workers: int,
                 autotermination_minutes: Optional[int],
                 single_user_name: str = None,
                 availability: Availability = None,
                 spark_conf: Optional[Dict[str, Any]] = None,
                 **kwargs):

        self.__params = {
            "cluster_name": cluster_name,
            "spark_version": spark_version,
            "num_workers": num_workers,
            "node_type_id": node_type_id,
            "instance_profile_id": instance_profile_id,
            "autotermination_minutes": autotermination_minutes,
        }

        spark_conf = spark_conf or dict()

        extra_params = kwargs or dict()
        if "custom_tags" not in extra_params:
            extra_params["custom_tags"] = dict()

        if "aws_attributes" not in extra_params:
            extra_params["aws_attributes"] = dict()

        if single_user_name is not None:
            extra_params["single_user_name"] = single_user_name
            extra_params["data_security_mode"] = "SINGLE_USER"
            extra_params["spark.databricks.passthrough.enabled"] = "true"

        if driver_node_type_id is not None:
            extra_params["driver_node_type_id"] = driver_node_type_id

        if num_workers == 0:
            # Don't use "local[*, 4] because the node type might have more cores
            spark_conf["spark.master"] = "local[*]"
            extra_params.get("custom_tags")["ResourceClass"] = "SingleNode"

            spark_conf["spark.databricks.cluster.profile"] = "singleNode"

        assert extra_params.get("aws_attributes", dict()).get("availability") is None, f"The parameter \"aws_attributes.availability\" should not be specified directly, use \"availability\" instead."

        if instance_profile_id is None and availability is None:
            # Default to on-demand if the instance profile was not defined
            availability = Availability.ON_DEMAND

        if availability is not None:
            assert instance_profile_id is None, f"The parameter \"availability\" cannot be specified when \"instance_profile_id\" is specified."
            extra_params.get("aws_attributes")["availability"] = availability.value

        if len(spark_conf) > 0:
            self.__params["spark_conf"] = spark_conf

        for key, value in extra_params.items():
            self.__params[key] = value

    @property
    def params(self) -> Dict[str, Any]:
        return self.__params


class ClustersClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/clusters"

    @common.deprecated(f"Use ClusterClient.create_from_config() instead")
    def create(self, *,
               cluster_name: str,
               spark_version: str,
               node_type_id: str,
               driver_node_type_id: str = None,
               num_workers: int,
               autotermination_minutes: int,
               single_user_name: str = None,
               on_demand: bool = True,
               spark_conf: Optional[Dict[str, Any]] = None,
               **kwargs) -> str:

        config = ClusterConfig(cluster_name=cluster_name,
                               spark_version=spark_version,
                               node_type_id=node_type_id,
                               driver_node_type_id=driver_node_type_id,
                               num_workers=num_workers,
                               autotermination_minutes=autotermination_minutes,
                               single_user_name=single_user_name,
                               availability=Availability.ON_DEMAND if on_demand else Availability.FALL_BACK,
                               spark_conf=spark_conf,
                               **kwargs)

        return self.create_from_config(config)

    def create_from_config(self, config: ClusterConfig) -> str:
        return self.create_from_dict(config.params)

    def create_from_dict(self, params: Dict[str, Any]) -> str:
        cluster = self.client.api("POST", f"{self.base_uri}/create", _data=params)
        return cluster.get("cluster_id")

    # I'm not 100% sure this isn't called outside of this library -JDP
    @common.deprecated(reason="Use ClustersClient.list_clusters() instead")
    def list(self):
        return self.list_clusters()

    def list_clusters(self) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_uri}/list")
        return response.get("clusters", list())

    def list_node_types(self):
        response = self.client.api("GET", f"{self.base_uri}/list-node-types")
        return response.get("node_types", list())

    # I'm not 100% sure this isn't called outside of this library -JDP
    @common.deprecated("Use ClustersClient.get_by_id() or ClustersClient.get_by_name() instead")
    def get(self, cluster_id):
        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        return self.client.api("GET", f"{self.base_uri}/get?cluster_id={cluster_id}")

    def get_by_id(self, cluster_id) -> Optional[Dict[str, Any]]:
        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        return self.client.api("GET", f"{self.base_uri}/get?cluster_id={cluster_id}", _expected=[200, 400])

    def get_by_name(self, cluster_name) -> Optional[Dict[str, Any]]:
        for cluster in self.list_clusters():
            if cluster_name == cluster.get("cluster_name"):
                return cluster

        return None

    def terminate_by_id(self, cluster_id) -> None:
        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        return self.client.api("POST", f"{self.base_uri}/delete", cluster_id=cluster_id)

    def terminate_by_name(self, cluster_name) -> None:
        from dbacademy.rest.common import DatabricksApiException

        cluster_name = common.validate_type(cluster_name, "cluster_name", str)
        cluster = self.get_by_name(cluster_name)
        if cluster is not None:
            cluster_id = cluster.get("cluster_id")
            return self.terminate_by_id(cluster_id)

        # 400 is the same returned by get_by_id and delete_by_id when it doesn't exist.
        raise DatabricksApiException(f"The cluster {cluster_name} was not found", http_code=400)

    def destroy_by_id(self, cluster_id) -> None:
        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        self.client.api("POST", f"{self.base_uri}/permanent-delete", cluster_id=cluster_id, _expected=[200, 400])
        return None

    def destroy_by_name(self, cluster_name) -> None:
        cluster_name = common.validate_type(cluster_name, "cluster_name", str)
        cluster = self.get_by_name(cluster_name)
        if cluster is not None:
            cluster_id = cluster.get("cluster_id")
            self.destroy_by_id(cluster_id)

        return None

    def get_current_spark_version(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's spark version
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The spark version
        """
        from dbacademy import dbgems
        cluster_id = cluster_id or dbgems.get_tags().get("clusterId")
        return self.get_by_id(cluster_id).get("spark_version", None)

    def get_current_instance_pool_id(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's instance_pool_id
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The instance pool id
        """
        from dbacademy import dbgems
        cluster_id = cluster_id or dbgems.get_tags()["clusterId"]
        return self.get_by_id(cluster_id).get("instance_pool_id", None)

    def get_current_node_type_id(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's node_type_id
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The instance pool id
        """
        from dbacademy import dbgems
        cluster_id = cluster_id or dbgems.get_tags().get("clusterId")
        return self.get_by_id(cluster_id).get("node_type_id", None)
