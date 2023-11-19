__all__ = ["ClustersClient"]

from dbacademy.common import validator
from typing import Optional, Dict, Any, List
from dbacademy.clients.rest.common import ApiContainer, ApiClient
from dbacademy.clients.databricks.clusters.cluster_config_class import ClusterConfig


class ClustersClient(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/clusters"

    def create_from_config(self, config: ClusterConfig) -> str:
        return self.create_from_dict(config.params)

    def create_from_dict(self, params: Dict[str, Any]) -> str:
        import json
        print("-"*80)
        print(json.dumps(params, indent=4))
        print("-"*80)
        cluster = self.client.api("POST", f"{self.base_uri}/create", _data=params)
        return cluster.get("cluster_id")

    def list(self) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.base_uri}/list")
        return response.get("clusters", list())

    def list_node_types(self):
        response = self.client.api("GET", f"{self.base_uri}/list-node-types")
        return response.get("node_types", list())

    # I'm not 100% sure this isn't called outside of this library -JDP
    def get_by_id(self, cluster_id) -> Optional[Dict[str, Any]]:
        cluster_id = validate.str_value(cluster_id=cluster_id)
        return self.client.api("GET", f"{self.base_uri}/get?cluster_id={cluster_id}", _expected=[200, 400])

    def get_by_name(self, cluster_name) -> Optional[Dict[str, Any]]:
        for cluster in self.list():
            if cluster_name == cluster.get("cluster_name"):
                return self.get_by_id(cluster.get("cluster_id"))

        return None

    def terminate_by_id(self, cluster_id) -> None:
        cluster_id = validate.str_value(cluster_id=cluster_id)
        return self.client.api("POST", f"{self.base_uri}/delete", cluster_id=cluster_id)

    def terminate_by_name(self, cluster_name) -> None:
        from dbacademy.clients.rest.common import DatabricksApiException

        cluster_name = validate.str_value(cluster_name=cluster_name)
        cluster = self.get_by_name(cluster_name)
        if cluster is not None:
            cluster_id = cluster.get("cluster_id")
            return self.terminate_by_id(cluster_id)

        # 400 is the same returned by get_by_id and delete_by_id when it doesn't exist.
        raise DatabricksApiException(f"The cluster {cluster_name} was not found", http_code=400)

    def destroy_by_id(self, cluster_id) -> None:
        cluster_id = validate.str_value(cluster_id=cluster_id)
        self.client.api("POST", f"{self.base_uri}/permanent-delete", cluster_id=cluster_id, _expected=[200, 400])
        return None

    def destroy_by_name(self, cluster_name) -> None:
        cluster_name = validate.str_value(cluster_name=cluster_name)
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
