from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer
from dbacademy import common

class ClustersClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def list(self):
        return self.client.api("GET", f"{self.client.endpoint}/api/2.0/clusters/list")

    def get(self, cluster_id):
        cluster_id = common.validate_type(cluster_id, "cluster_id", str)

        return self.client.api("GET", f"{self.client.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")

    def get_current_spark_version(self):
        from dbacademy import dbgems
        cluster_id = dbgems.get_tags().get("clusterId")
        return self.get(cluster_id).get("spark_version", None)

    def get_current_instance_pool_id(self):
        from dbacademy import dbgems
        cluster_id = dbgems.get_tags()["clusterId"]
        return self.get(cluster_id).get("instance_pool_id", None)

    def get_current_node_type_id(self):
        from dbacademy import dbgems
        cluster_id = dbgems.get_tags()["clusterId"]
        return self.get(cluster_id).get("node_type_id", None)
