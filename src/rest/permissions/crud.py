from typing import Any, Dict, List, Literal

from dbacademy.rest.common import *

__all__ = ["PermissionsCrud"]

valid_whats = ("user_name", "group_name", "service_principal_name")
What = Literal[valid_whats]
PermissionLevel = str
ACL = Dict[str, Any]
PermissionLevelList = List[Dict[str, str]]


class PermissionsCrud(ApiContainer):
    valid_permissions = ()
    PermissionLevel = Literal[valid_permissions]

    def __init__(self,
                 client: ApiClient,
                 path: str,
                 noun: str,
                 *,
                 singular: str = None,
                 plural: str = None,
                 id_key: str = None,
                 name_key: str = None):

        super().__init__()
        self.client = client
        if path.startswith("/"):
            path = path[1:]
        self.path = path
        self.noun = noun
        self.singular = singular or self.noun
        self.plural = plural or self.singular + "s"
        self.id_key = id_key or noun + "_id"
        self.name_key = name_key or noun + "_name"

        # Update doc strings, replacing placeholders with actual values.
        cls = type(self)
        methods = [attr for attr in dir(cls) if not attr.startswith("__") and callable(getattr(cls, attr))]
        for name in methods:
            m = getattr(cls, name)
            if isinstance(m.__doc__, str):
                m.__doc__ = m.__doc__.format(**self.__dict__)

    @staticmethod
    def _validate_what(what: What):
        if what not in valid_whats:
            raise ValueError(f"Expected 'permission_level' to be one of {valid_whats}, found '{what}'")

    def _validate_permission_level(self, permission_level: PermissionLevel):
        if permission_level not in self.valid_permissions:
            raise ValueError(f"Expected 'permission_level' to be one of {self.valid_permissions},"
                             f" found '{permission_level}'")

    def get_levels(self, id: ItemId) -> PermissionLevelList:
        return self.client.api("GET", f"{self.path}/{id}/permissionLevels")["permission_levels"]

    def get(self, id: ItemId) -> ACL:
        return self.client.api("GET", f"{self.path}/{id}")

    def update(self, id, what: What, value: str, permission_level: PermissionLevel):
        self._validate_what(what)
        self._validate_permission_level(permission_level)
        acl = [
                {
                    what: value,
                    "permission_level": permission_level
                }
            ]
        return self.client.api_simple("PATCH", f"{self.path}/{id}", access_control_list=acl)

    def replace(self, id, acl: ACL):
        return self.client.api_simple("PUT", f"{self.path}/{id}", access_control_list=acl)

    def update_user(self, id, username, permission_level):
        return self.update(id, "user_name", username, permission_level)

    def update_group(self, id, group_name, permission_level):
        return self.update(id, "group_name", group_name, permission_level)

    def update_service_principal(self, id, service_principal_name, permission_level):
        return self.update(id, "service_principal_name", service_principal_name, permission_level)
