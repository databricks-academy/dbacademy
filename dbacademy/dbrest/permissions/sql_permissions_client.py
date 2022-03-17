from dbacademy.dbrest import DBAcademyRestClient
import builtins

class SqlPermissionsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str, singular_obj_type:str, plural_obj_type:str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.max_page_size = 250
        self.valid_objects = ["alerts", "dashboards", "data_sources", "queries"]
        self.valid_permissions = ["CAN_VIEW", "CAN_RUN", "CAN_MANAGE"]

        self.singular_obj_type = singular_obj_type

        assert plural_obj_type in self.valid_objects, f"Expected \"plural_obj_type\" to be one of {self.valid_objects}, found \"{plural_obj_type}\""
        self.plural_obj_type = plural_obj_type

        self.base_uri = f"{self.endpoint}/api/2.0/preview/sql/permissions"

    def _validate_permission_level(self, permission_level:str, allow_none:bool=False):
        if allow_none and permission_level is None: return
        assert permission_level in self.valid_permissions, f"Expected \"permission_level\" to be one of {self.valid_permissions}, found \"{permission_level}\""

    def _validate_params(self, params:dict, object_id:str):
        expected = f"{self.plural_obj_type}/{object_id}"
        actual = params.get("object_id", None)
        assert actual == expected, f"The param's object_id expected to be \"{expected}\", found \"{actual}\""
        
        actual = params.get("object_type", None)
        assert actual == self.singular_obj_type, f"The param's object_type expected to be \"{self.singular_obj_type}\", found \"{actual}\""

    def get(self, object_id:str):
        return self.client.execute_get_json(f"{self.base_uri}/{self.plural_obj_type}/{object_id}")


    def update(self, object_id:str, params:dict):
        self._validate_params(params, object_id)

        access_control_list = params.get("access_control_list", None)
        assert type(access_control_list) == list, f"The param's access_control_list expected to be of type list, found {type(access_control_list)}"

        for access_control in access_control_list:
            assert "user_name" in access_control or "group_name" in access_control, "Expected the access_control to contain either user_name or group_name"
            self._validate_permission_level(access_control.get("permission_level", None))

        return self.client.execute_post_json(f"{self.base_uri}/{self.plural_obj_type}/{object_id}", params)

    def update_user(self, object_id:str, username:str, permission_level:str, params:dict):
        self._validate_permission_level(permission_level, allow_none=True)

        permissions = self.get(object_id)
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
        return self.update(object_id, permissions)

    def update_group(self, object_id:str, group_name:str, permission_level:str, params:dict):
        self._validate_permission_level(permission_level, allow_none=True)

        permissions = self.get(object_id)
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
        return self.update(object_id, permissions)
