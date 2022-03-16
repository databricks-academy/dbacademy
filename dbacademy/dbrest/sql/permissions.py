from dbacademy.dbrest import DBAcademyRestClient
import builtins

class SqlPermissionsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_uri = f"{self.endpoint}/api/2.0/preview/sql/permissions"

        self.max_page_size = 250
        self.valid_objects = ["alerts", "dashboards", "data_sources", "queries"]
        self.valid_permissions = ["CAN_VIEW", "CAN_RUN", "CAN_MANAGE"]

    def _validate_object_type(self, object_type):
        assert object_type in self.valid_objects, f"Expected \"object_type\" to be one of {self.valid_objects}, found \"{object_type}\""

    def _validate_permission_level(self, permission_level):
        assert permission_level in self.valid_permissions, f"Expected \"permission_level\" to be one of {self.valid_permissions}, found \"{permission_level}\""

    def get(self, object_type, object_id):
        self._validate_object_type(self, object_type)
        return self.client.execute_get_json(f"{self.base_uri}/{object_type}/{object_id}")

    def update_group(self, object_type, object_id, group_name, permission_level):
        self._validate_object_type(object_type)
        self._validate_permission_level(permission_level)

        params = {
            "group_name": group_name,
            "permission_level": permission_level
        }

        return self.client.execute_post_json(f"{self.base_uri}/{object_type}/{object_id}", params)

    def update_group(self, object_type, object_id, user_name, permission_level):
        self._validate_object_type(object_type)
        self._validate_permission_level(permission_level)

        params = {
            "user_name": user_name,
            "permission_level": permission_level
        }

        return self.client.execute_post_json(f"{self.base_uri}/{object_type}/{object_id}", params)
