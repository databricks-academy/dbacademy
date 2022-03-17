from dbacademy.dbrest import DBAcademyRestClient
import builtins

class SqlEndpointsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint
        self.base_uri = f"{self.endpoint}/api/2.0/preview/permissions/sql/endpoints"

    def _validate_what(self, what:str):
        valid_whats = ["user_name", "group_name", "service_principal_name"]
        assert what in valid_whats, f"Expected \"permission_level\" to be one of {valid_whats}, found \"{what}\""

    def _validate_permission_level(self, permission_level:str, allow_none:bool=False):
        valid_permissions = ["CAN_USE", "CAN_MANAGE"]
        assert permission_level in valid_permissions, f"Expected \"permission_level\" to be one of {valid_permissions}, found \"{permission_level}\""

    def get_levels(self, id):
        return self.client.execute_get_json(f"{self.base_uri}/{id}/permissionLevels")

    def get(self, id):
        return self.client.execute_get_json(f"{self.base_uri}/{id}")

    def update(self, id, what, value, permission_level):
        _validate_what(what)
        _validate_permission_level(permission_level)
        params = {
            "access_control_list": [
                {
                    what: value,
                    "permission_level": permission_level
                }
            ]
        }
        return self.client.execute_patch_json(f"{self.base_uri}/{id}", params)

    def update_user(self, id, username, permission_level):
        return update("user_name", username, permission_level)

    def update_group(self, id, group_name, permission_level):
        return update("group_name", group_name, permission_level)

    def update_service_principal(self, id, service_principal_name, permission_level):
        return update("service_principal_name", service_principal_name, permission_level)
