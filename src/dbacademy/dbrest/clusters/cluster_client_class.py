from typing import Optional, Dict, Any, List
from dbacademy.clients.rest.common import ApiContainer


class ClustersClient(ApiContainer):
    from dbacademy.dbrest.clusters import ClusterConfig
    from dbacademy.dbrest import DBAcademyRestClient
    from dbacademy import common

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
               libraries: List[Dict[str, Any]] = None,
               **kwargs) -> str:
        from dbacademy.dbrest.clusters import ClusterConfig, Availability

        config = ClusterConfig(cluster_name=cluster_name,
                               spark_version=spark_version,
                               node_type_id=node_type_id,
                               driver_node_type_id=driver_node_type_id,
                               num_workers=num_workers,
                               autotermination_minutes=autotermination_minutes,
                               single_user_name=single_user_name,
                               availability=Availability.ON_DEMAND if on_demand else Availability.SPOT_WITH_FALLBACK,
                               spark_conf=spark_conf,
                               libraries=libraries,
                               **kwargs)

        return self.create_from_config(config)

    def create_from_config(self, config: ClusterConfig) -> str:
        return self.create_from_dict(config.params)

    def create_from_dict(self, params: Dict[str, Any]) -> str:
        import json
        print("-"*80)
        print(json.dumps(params, indent=4))
        print("-"*80)
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
        from dbacademy import common

        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        return self.client.api("GET", f"{self.base_uri}/get?cluster_id={cluster_id}")

    def get_by_id(self, cluster_id) -> Optional[Dict[str, Any]]:
        from dbacademy import common

        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        return self.client.api("GET", f"{self.base_uri}/get?cluster_id={cluster_id}", _expected=[200, 400])

    def get_by_name(self, cluster_name) -> Optional[Dict[str, Any]]:
        for cluster in self.list_clusters():
            if cluster_name == cluster.get("cluster_name"):
                return self.get_by_id(cluster.get("cluster_id"))

        return None

    def terminate_by_id(self, cluster_id) -> None:
        from dbacademy import common

        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        return self.client.api("POST", f"{self.base_uri}/delete", cluster_id=cluster_id)

    def terminate_by_name(self, cluster_name) -> None:
        from dbacademy import common
        from dbacademy.clients.rest.common import DatabricksApiException

        cluster_name = common.validate_type(cluster_name, "cluster_name", str)
        cluster = self.get_by_name(cluster_name)
        if cluster is not None:
            cluster_id = cluster.get("cluster_id")
            return self.terminate_by_id(cluster_id)

        # 400 is the same returned by get_by_id and delete_by_id when it doesn't exist.
        raise DatabricksApiException(f"The cluster {cluster_name} was not found", http_code=400)

    def destroy_by_id(self, cluster_id) -> None:
        from dbacademy import common

        cluster_id = common.validate_type(cluster_id, "cluster_id", str)
        self.client.api("POST", f"{self.base_uri}/permanent-delete", cluster_id=cluster_id, _expected=[200, 400])
        return None

    def destroy_by_name(self, cluster_name) -> None:
        from dbacademy import common

        cluster_name = common.validate_type(cluster_name, "cluster_name", str)
        cluster = self.get_by_name(cluster_name)
        if cluster is not None:
            cluster_id = cluster.get("cluster_id")
            self.destroy_by_id(cluster_id)

        return None

    def get_current(self, cluster_id: str = None) -> Dict[str, Any]:
        """
        Retrieves the current cluster
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The spark version
        """
        from dbacademy import dbgems
        cluster_id = cluster_id or dbgems.get_tags().get("clusterId")
        return self.get_by_id(cluster_id)

    def get_current_spark_version(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's spark version
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The spark version
        """
        return self.get_current(cluster_id).get("spark_version", None)

    def get_current_instance_pool_id(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's instance_pool_id
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The instance pool id
        """
        return self.get_current(cluster_id).get("instance_pool_id", None)

    def get_current_data_security_mode(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's data_security_mode
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The data security mode
        """
        return self.get_current(cluster_id).get("data_security_mode", None)

    def get_current_single_user_name(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's single_user_name
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The data security mode
        """
        return self.get_current(cluster_id).get("single_user_name", None)

    def get_current_node_type_id(self, cluster_id: str = None) -> Optional[str]:
        """
        Retrieves the corresponding cluster's node_type_id
        :param cluster_id: The specified cluster_id or the cluster_id returned by `dbgems.get_tags().get("clusterId")`
        :return: The instance pool id
        """
        return self.get_current(cluster_id).get("node_type_id", None)
