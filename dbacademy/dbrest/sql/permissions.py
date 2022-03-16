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

    def _validate_object_type(self, object_type:str):
        assert object_type in self.valid_objects, f"Expected \"object_type\" to be one of {self.valid_objects}, found \"{object_type}\""

    def _validate_permission_level(self, permission_level:str, allow_none:bool=False):
        if allow_none and permission_level is None: return
        assert permission_level in self.valid_permissions, f"Expected \"permission_level\" to be one of {self.valid_permissions}, found \"{permission_level}\""

    def get(self, object_type:str, object_id:str):
        self._validate_object_type(object_type)
        return self.client.execute_get_json(f"{self.base_uri}/{object_type}/{object_id}")

    def update(self, object_type:str, object_id:str, params:dict):
        self._validate_object_type(object_type)

        if object_type == "queries":
            expected = f"queries/{object_id}"
            actual = params.get("object_id", None)
            assert actual == expected, f"The param's object_id expected to be \"{expected}\", found \"{actual}\""
            
            expected = "query"
            actual = params.get("object_type", None)
            assert actual == expected, f"The param's object_type expected to be \"{expected}\", found \"{actual}\""
      
        access_control_list = params.get("access_control_list", None)
        assert type(access_control_list) == list, f"The param's access_control_list expected to be of type list, found {type(access_control_list)}"

        for access_control in access_control_list:
            assert "user_name" in access_control or "group_name" in access_control, "Expected the access_control to contain either user_name or group_name"
            self._validate_permission_level(access_control.get("permission_level", None))

        return self.client.execute_post_json(f"{self.base_uri}/{object_type}/{object_id}", params)

    def update_user(self, object_type:str, object_id:str, username:str, permission_level:str, params:dict):
        self._validate_permission_level(permission_level, allow_none=True)

        permissions = self.get(object_type, object_id)
        access_control_list = builtins.list()
        
        for access_control in permissions.get("access_control_list", builtins.list()):
            if access_control.get("user_name", None) != username:
                access_control_list.append(access_control) # Keep it

        if permission_level is not None:
            access_control_list.append({
                "user_name": username,
                "permission_level": permission_level
            })

        permissions["access_control_list"] = access_control_list
        return self.update(object_type, object_id, permissions)

    def update_group(self, object_type:str, object_id:str, group_name:str, permission_level:str, params:dict):
        self._validate_permission_level(permission_level, allow_none=True)

        permissions = self.get(object_type, object_id)
        access_control_list = builtins.list()
        
        for access_control in permissions.get("access_control_list", builtins.list()):
            if access_control.get("group_name", None) != group_name:
                access_control_list.append(access_control) # Keep it

        if permission_level is not None:
            access_control_list.append({
                "group_name": group_name,
                "permission_level": permission_level
            })

        permissions["access_control_list"] = access_control_list
        return self.update(object_type, object_id, permissions)
