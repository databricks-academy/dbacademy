__all__ = ["PermissionsCrud", "What"]

from typing import Any, Dict, List

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from dbacademy.clients.rest.common import *


valid_whats = ("user_name", "group_name", "service_principal_name")

# noinspection PyTypeHints
What = Literal[valid_whats]

PermissionLevel = str
ACL = Dict[str, Any]
PermissionLevelList = List[Dict[str, str]]


class PermissionsCrud(ApiContainer):
    valid_permissions = ()

    # noinspection PyTypeHints
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

    def get_levels(self, id_value: ItemId = None) -> PermissionLevelList:
        if id_value is None:
            url = f"{self.path}/permissionLevels"
        else:
            url = f"{self.path}/{id_value}/permissionLevels"

        return self.client.api("GET", url).get("permission_levels")

    def get(self, id_value: ItemId = None) -> ACL:
        return self.client.api("GET", f"{self.path}/{id_value}")

    def update(self, id_value: ItemId = None, *, what: What, value: str, permission_level: PermissionLevel):
        self._validate_what(what)
        self._validate_permission_level(permission_level)
        acl = [
                {
                    what: value,
                    "permission_level": permission_level
                }
            ]
        if id_value is None:
            url = f"{self.path}"
        else:
            url = f"{self.path}/{id_value}"
        return self.client.api("PATCH", url, access_control_list=acl)

    def replace(self, id_value: ItemId = None, *, acl: ACL):
        if id_value is None:
            url = f"{self.path}"
        else:
            url = f"{self.path}/{id_value}"
        return self.client.api("PUT", url, access_control_list=acl)

    def update_user(self, id_value: ItemId = None, *, username, permission_level):
        return self.update(id_value=id_value,
                           what="user_name",
                           value=username,
                           permission_level=permission_level)

    def update_group(self, id_value: ItemId = None, *, group_name, permission_level):
        return self.update(id_value=id_value,
                           what="group_name",
                           value=group_name,
                           permission_level=permission_level)

    def update_service_principal(self, id_value: ItemId = None, *, service_principal_name, permission_level):
        return self.update(id_value=id_value,
                           what="service_principal_name",
                           value=service_principal_name,
                           permission_level=permission_level)
