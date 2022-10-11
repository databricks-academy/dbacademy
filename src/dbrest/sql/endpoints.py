from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer

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


class SqlEndpointsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/sql/warehouses"

    def start(self, endpoint_id):
        return self.client.execute_post_json(f"{self.base_uri}/{endpoint_id}/start", {})

    def stop(self, endpoint_id):
        return self.client.execute_post_json(f"{self.base_uri}/{endpoint_id}/stop", {})

    def get_by_id(self, endpoint_id):
        return self.client.execute_get_json(f"{self.base_uri}/{endpoint_id}")

    def get_by_name(self, name):
        for endpoint in self.list():
            if name == endpoint.get("name"):
                return endpoint
        return None

    def delete_by_id(self, endpoint_id):
        self.client.execute_delete_json(f"{self.base_uri}/{endpoint_id}")
        return None

    def delete_by_name(self, name):
        endpoint = self.get_by_name(name)
        if endpoint:
            self.delete_by_id(endpoint.get("id"))

        return None

    def list(self):
        result = self.client.execute_get_json(self.base_uri)
        return result.get("warehouses", [])

    def create_or_update(self,
                         name: str,
                         cluster_size: str,
                         enable_serverless_compute: bool,
                         min_num_clusters: int = 1,
                         max_num_clusters: int = 1,
                         auto_stop_mins: int = 120,
                         enable_photon: bool = True,
                         spot_instance_policy: str = RELIABILITY_OPTIMIZED,
                         channel: str = CHANNEL_NAME_CURRENT,
                         tags: dict = None):

        endpoint = self.get_by_name(name)

        if endpoint is None:
            return self.create(
               name=name,
               cluster_size=cluster_size,
               enable_serverless_compute=enable_serverless_compute,
               min_num_clusters=min_num_clusters,
               max_num_clusters=max_num_clusters,
               auto_stop_mins=auto_stop_mins,
               enable_photon=enable_photon,
               spot_instance_policy=spot_instance_policy,
               channel=channel,
               tags=tags)
        else:
            return self.update(
               endpoint_id=endpoint.get("id"),
               name=name,
               cluster_size=cluster_size,
               enable_serverless_compute=enable_serverless_compute,
               min_num_clusters=min_num_clusters,
               max_num_clusters=max_num_clusters,
               auto_stop_mins=auto_stop_mins,
               enable_photon=enable_photon,
               spot_instance_policy=spot_instance_policy,
               channel=channel,
               tags=tags)

    def create(self,
               name: str,
               cluster_size: str,
               enable_serverless_compute: bool,
               min_num_clusters: int = 1,
               max_num_clusters: int = 1,
               auto_stop_mins: int = 120,
               enable_photon: bool = True,
               spot_instance_policy: str = RELIABILITY_OPTIMIZED,
               channel: str = CHANNEL_NAME_CURRENT,
               tags: dict = None):

        tags = dict() if tags is None else tags
        assert spot_instance_policy in SPOT_POLICIES, f"Expected spot_instance_policy to be one of {SPOT_POLICIES}, found {spot_instance_policy}"
        assert channel in CHANNELS, f"Expected channel to be one of {CHANNELS}, found {channel}"
        assert cluster_size in CLUSTER_SIZES, f"Expected cluster_size to be one of {CLUSTER_SIZES}, found {cluster_size}"

        params = {
            "name": name,
            "cluster_size": cluster_size,
            "min_num_clusters": min_num_clusters,
            "max_num_clusters": max_num_clusters,
            "auto_stop_mins": auto_stop_mins,
            "tags": {
                "custom_tags": []
            },
            "spot_instance_policy": spot_instance_policy,
            "enable_photon": enable_photon,
            "enable_serverless_compute": enable_serverless_compute,
            "channel": {
                "name": channel
            },
        }

        for item in tags.items():
            custom_tags = params.get("tags").get("custom_tags", [])
            custom_tags.append({
                "key": item[0],
                "value": item[1]
            })

        result = self.client.execute_post_json(f"{self.base_uri}", params)
        return self.get_by_id(result.get("id"))

    def edit(self, endpoint_id: str, name: str = None, cluster_size: str = None, enable_serverless_compute: bool = None, min_num_clusters: int = None, max_num_clusters: int = None, auto_stop_mins: int = None, enable_photon: bool = None, spot_instance_policy: str = None, channel: str = None, tags: dict = None):
        return self.update(endpoint_id, name, cluster_size, enable_serverless_compute, min_num_clusters, max_num_clusters, auto_stop_mins, enable_photon, spot_instance_policy, channel, tags)

    def update(self,
               endpoint_id: str,
               name: str = None,
               cluster_size: str = None,
               enable_serverless_compute: bool = None,
               min_num_clusters: int = None,
               max_num_clusters: int = None,
               auto_stop_mins: int = None,
               enable_photon: bool = None,
               spot_instance_policy: str = None,
               channel: str = None,
               tags: dict = None):

        params = dict()

        if name is not None:
            params["name"] = name

        if cluster_size is not None:
            assert cluster_size in CLUSTER_SIZES, f"Expected cluster_size to be one of {CLUSTER_SIZES}, found {cluster_size}"
            params["cluster_size"] = cluster_size
            
        if enable_serverless_compute is not None:
            params["enable_serverless_compute"] = enable_serverless_compute
            
        if min_num_clusters is not None:
            params["min_num_clusters"] = min_num_clusters
            
        if max_num_clusters is not None:
            params["max_num_clusters"] = max_num_clusters
            
        if auto_stop_mins is not None:
            params["auto_stop_mins"] = auto_stop_mins
            
        if enable_photon is not None:
            params["enable_photon"] = enable_photon

        if spot_instance_policy is not None:
            assert spot_instance_policy in SPOT_POLICIES, f"Expected spot_instance_policy to be one of {SPOT_POLICIES}, found {spot_instance_policy}"
            params["spot_instance_policy"] = spot_instance_policy
            
        if channel is not None:
            assert channel in CHANNELS, f"Expected channel to be one of {CHANNELS}, found {channel}"
            params["channel"] = {
                "name": channel
            }
            
        if tags is not None:
            params["tags"] = {
                "custom_tags": []
            }
            for key in tags:
                value = tags[key]
                params.get("tags").get("custom_tags").append({
                    "key": key,
                    "value": value
                })

        self.client.execute_post_json(f"{self.base_uri}/{endpoint_id}/edit", params)
        return self.get_by_id(endpoint_id)

    @staticmethod
    def to_endpoint_name(user, naming_template: str, naming_params: dict):
        username = user.get("userName")

        if "{da_hash}" in naming_template:
            assert naming_params.get("course", None) is not None, "The template is employing da_hash which requires course to be specified in naming_params"
            course = naming_params["course"]
            da_hash = abs(hash(f"{username}-{course}")) % 10000
            naming_params["da_hash"] = da_hash
            
        naming_params["da_name"] = username.split("@")[0]
        return naming_template.format(**naming_params)

    def create_user_endpoints(self,
                              naming_template: str,
                              naming_params: dict,
                              cluster_size: str,
                              enable_serverless_compute: bool,
                              min_num_clusters: int = 1,
                              max_num_clusters: int = 1,
                              auto_stop_mins: int = 120,
                              enable_photon: bool = True,
                              spot_instance_policy: str = RELIABILITY_OPTIMIZED,
                              channel: str = CHANNEL_NAME_CURRENT,
                              tags: dict = None,
                              users: list = None):
        """Creates one SQL endpoint per user in the current workspace. The list of users can be limited to a subset of users with the "users" parameter.
    Parameters: 
        naming_template (str): The template used to name each user's endpoint.
        naming_params (str): The parameters used in completing the template.
        cluster_size (str): The size of the cluster - see CLUSTER_SIZES for the list of valid values.
        enable_serverless_compute (bool): Set to True to employ serverless compute, otherwise classic compute.
        min_num_clusters (int = 1): The minimum number of clusters for this endpoint.
        max_num_clusters (int = 1): The maximum number of clusters for this endpoint.
        auto_stop_mins (int = 120): The number of minutes after which idle endpoints will be shut down.
        enable_photon (bool = True): Set to True to employ the Photon runtime otherwise classic runtime.
        spot_instance_policy (str = RELIABILITY_OPTIMIZED): The spot instance policy - see SPOT_POLICIES for the list of valid values.
        channel (str = CHANNEL_NAME_CURRENT): The endpoint's channel - see CHANNELS for the list of valid values.
        tags (dict = dict()): The list of tags expressed as key-value pairs.
        users (list[str or dict] = None, str): unlike other parameters, this value is eventually converted to a list of user objects but may be specified as a list or single value (which is converted to a list). String values are assumed to be the user's username if it includes the @ symbole and the user's ID otherwise.
        """

        tags = dict() if tags is None else tags

        for user in self.client.scim().users().to_users_list(users):
            self.create_user_endpoint(user=user, 
                                      naming_template=naming_template, 
                                      naming_params=naming_params,
                                      cluster_size=cluster_size,
                                      enable_serverless_compute=enable_serverless_compute,
                                      min_num_clusters=min_num_clusters,
                                      max_num_clusters=max_num_clusters,
                                      auto_stop_mins=auto_stop_mins,
                                      enable_photon=enable_photon,
                                      spot_instance_policy=spot_instance_policy,
                                      channel=channel,
                                      tags=tags)

    def create_user_endpoint(self,
                             user,
                             naming_template: str,
                             naming_params: dict,
                             cluster_size: str,
                             enable_serverless_compute: bool,
                             min_num_clusters: int,
                             max_num_clusters: int,
                             auto_stop_mins: int,
                             enable_photon: bool,
                             spot_instance_policy: str,
                             channel: str,
                             tags: dict):
        username = user.get("userName")
        active = user.get("active")
        
        if not active:
            print(f"Skipping creation of endpoint for the user \"{username}\":\n - Inactive user\n")
            return
        
        endpoint_name = self.to_endpoint_name(user, naming_template, naming_params)

        for endpoint in self.client.sql().endpoints().list():
            if endpoint.get("name") == endpoint_name:
                print(f"Skipping creation of the endpoint \"{endpoint_name}\" for the user \"{username}\":\n - The endpoint already exists\n")
                return

        print(f"Creating the endpoint \"{endpoint_name}\" for the user \"{username}\"")

        endpoint = self.create(name=endpoint_name,
                               cluster_size=cluster_size,
                               enable_serverless_compute=enable_serverless_compute,
                               min_num_clusters=min_num_clusters,
                               max_num_clusters=max_num_clusters,
                               auto_stop_mins=auto_stop_mins,
                               enable_photon=enable_photon,
                               spot_instance_policy=spot_instance_policy,
                               channel=channel,
                               tags=tags)

        # Give the user CAN_MANAGE to their new endpoint
        endpoint_id = endpoint.get("id")
        self.client.permissions().sql().endpoints().update_user(endpoint_id, username, "CAN_MANAGE")

    def delete_user_endpoints(self, naming_template: str, naming_params: dict, users: list = None):
        for user in self.client.scim().users().to_users_list(users):
            self.delete_user_endpoint(user=user, naming_template=naming_template, naming_params=naming_params)

    def delete_user_endpoint(self, user, naming_template: str, naming_params: dict):
        username = user.get("userName")
        endpoint_name = self.to_endpoint_name(user, naming_template, naming_params)

        for endpoint in self.client.sql().endpoints().list():
            if endpoint.get("name") == endpoint_name:
                print(f"Deleting the endpoint \"{endpoint_name}\" for the user \"{username}\"")
                self.delete_by_id(endpoint.get("id"))
                return

        print(f"Skipping deletion of the endpoint \"{endpoint_name}\" for the user \"{username}\": Not found\n")

    def start_user_endpoints(self, naming_template: str, naming_params: dict, users: list = None):
        for user in self.client.scim().users().to_users_list(users):
            self.start_user_endpoint(user=user, naming_template=naming_template, naming_params=naming_params)

    def start_user_endpoint(self, user, naming_template: str, naming_params: dict):
        username = user.get("userName")
        endpoint_name = self.to_endpoint_name(user, naming_template, naming_params)

        for endpoint in self.client.sql().endpoints().list():
            if endpoint.get("name") == endpoint_name:
                print(f"Starting the endpoint \"{endpoint_name}\" for the user \"{username}\"")
                self.start(endpoint.get("id"))
                return

        print(f"Skipping start of the endpoint \"{endpoint_name}\" for the user \"{username}\": Not found\n")

    def stop_user_endpoints(self, naming_template: str, naming_params: dict, users: list = None):
        for user in self.client.scim().users().to_users_list(users):
            self.stop_user_endpoint(user=user, naming_template=naming_template, naming_params=naming_params)

    def stop_user_endpoint(self, user, naming_template: str, naming_params: dict):
        username = user.get("userName")
        endpoint_name = self.to_endpoint_name(user, naming_template, naming_params)

        for endpoint in self.client.sql().endpoints().list():
            if endpoint.get("name") == endpoint_name:
                print(f"Stopping the endpoint \"{endpoint_name}\" for the user \"{username}\"")
                self.stop(endpoint.get("id"))
                return

        print(f"Skipping stop of the endpoint \"{endpoint_name}\" for the user \"{username}\": Not found\n")
