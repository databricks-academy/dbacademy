from dbacademy.dbrest import DBAcademyRestClient


class SqlEndpointsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

        self.COST_OPTIMIZED = "COST_OPTIMIZED"
        self.RELIABILITY_OPTIMIZED = "RELIABILITY_OPTIMIZED"

        self.CHANNEL_NAME_PREVIEW = "CHANNEL_NAME_PREVIEW"
        self.CHANNEL_NAME_CURRENT = "CHANNEL_NAME_CURRENT"

    def start(self, endpoint_id):
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/sql/endpoints/{endpoint_id}/start", {})

    def stop(self, endpoint_id):
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/sql/endpoints/{endpoint_id}/stop", {})

    def get(self, endpoint_id):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/sql/endpoints/{endpoint_id}")

    def delete(self, endpoint_id):
        return self.client.execute_delete_json(f"{self.endpoint}/api/2.0/sql/endpoints/{endpoint_id}")

    def list(self):
        result = self.client.execute_get_json(f"{self.endpoint}/api/2.0/sql/endpoints")
        return [] if "endpoints" not in result else result.get("endpoints")

    def create(self, name:str,
                     cluster_size:str,
                     min_num_clusters:int,
                     max_num_clusters:int,
                     auto_stop_mins:int,
                     tags:dict,
                     spot_instance_policy:str,
                     enable_photon:bool,
                     enable_serverless_compute:bool,
                     channel:str):

        assert spot_instance_policy in [self.COST_OPTIMIZED, self.RELIABILITY_OPTIMIZED], "Expected spot_instance_policy to be one of {self.COST_OPTIMIZED} or {self.RELIABILITY_OPTIMIZED}, found {spot_instance_policy}"
        assert channel in [self.CHANNEL_NAME_PREVIEW, self.CHANNEL_NAME_CURRENT], "Expected channel to be one of {self.CHANNEL_NAME_PREVIEW} or {self.CHANNEL_NAME_CURRENT}, found {spot_instance_policy}"
