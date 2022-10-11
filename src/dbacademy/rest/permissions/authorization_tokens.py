from dbacademy.rest.common import ApiClient, ApiContainer

# from dbacademy.rest.permissions.crud import What, PermissionLevel

__all__ = ["Tokens"]


class Tokens(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client

    def get_levels(self) -> dict:
        return self.client.execute_get_json(f"2.0/preview/permissions/authorization/tokens/permissionLevels")

    # def update(self, object_id: str, what: What, value: str, permission_level: PermissionLevel):
    #     self._validate_what(what)
    #     self._validate_permission_level(permission_level)
    #     acl = [
    #             {
    #                 what: value,
    #                 "permission_level": permission_level
    #             }
    #         ]
    #     return self.client.api_simple("PATCH", f"{self.path}", access_control_list=acl)
