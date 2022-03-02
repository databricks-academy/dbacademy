from dbacademy.dbrest import DBAcademyRestClient

COST_OPTIMIZED = "COST_OPTIMIZED"
RELIABILITY_OPTIMIZED = "RELIABILITY_OPTIMIZED"
SPOT_POLICIES = [COST_OPTIMIZED, RELIABILITY_OPTIMIZED]

CHANNEL_NAME_PREVIEW = "CHANNEL_NAME_PREVIEW"
CHANNEL_NAME_CURRENT = "CHANNEL_NAME_CURRENT"
CHANNELS = [CHANNEL_NAME_PREVIEW, CHANNEL_NAME_CURRENT]


CLUSTER_SIZE_2X_SMALL = "2X-Small" 
CLUSTER_SIZE_X_SMALL = "X-Small" 
CLUSTER_SIZE_SMALL = "Small" 
CLUSTER_SIZE_MEDIUM = "Medium" 
CLUSTER_SIZE_LARGE = "Large" 
CLUSTER_SIZE_X_LARGE = "X-Large" 
CLUSTER_SIZE_2X_LARGE = "2X-Large" 
CLUSTER_SIZE_3X_LARGE = "3X-Large" 
CLUSTER_SIZE_4X_LARGE = "4X-Large" 
CLUSTER_SIZES = [CLUSTER_SIZE_2X_SMALL,
                 CLUSTER_SIZE_X_SMALL, 
                 CLUSTER_SIZE_SMALL, 
                 CLUSTER_SIZE_MEDIUM, 
                 CLUSTER_SIZE_LARGE, 
                 CLUSTER_SIZE_X_LARGE, 
                 CLUSTER_SIZE_2X_LARGE, 
                 CLUSTER_SIZE_3X_LARGE, 
                 CLUSTER_SIZE_4X_LARGE]

class SqlEndpointsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

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
                     min_num_clusters:int = 1,
                     max_num_clusters:int = 1,
                     auto_stop_mins:int = 120,
                     enable_photon:bool = True,
                     enable_serverless_compute:bool = True,
                     spot_instance_policy:str = RELIABILITY_OPTIMIZED,
                     channel:str = CHANNEL_NAME_CURRENT,
                     tags:dict = dict()):

        assert spot_instance_policy in SPOT_POLICIES, f"Expected spot_instance_policy to be one of {SPOT_POLICIES}, found {spot_instance_policy}"
        assert channel in CHANNELS, f"Expected channel to be one of {CHANNELS}, found {channel}"
        assert cluster_size in CLUSTER_SIZES, f"Expected cluster_size to be one of {CLUSTER_SIZES}, found {cluster_size}"

        params = {
            name: name,
            cluster_size: cluster_size,
            # min_num_clusters: min_num_clusters,
            # max_num_clusters: max_num_clusters,
            # auto_stop_mins: auto_stop_mins,
            # tags: [],
            # spot_instance_policy: spot_instance_policy,
            # enable_photon: enable_photon,
            # enable_serverless_compute: enable_serverless_compute,
            # channel: channel,
        }

        print("Creating cluster: ")
        print(params)

        # for key in tags:
        #     value = tags[key]
        #     params.append({
        #         "key": key,
        #         "value": value
        #     })

        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/sql/endpoints", params)
