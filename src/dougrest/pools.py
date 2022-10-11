from typing import Dict, List

from dbacademy.dougrest import DatabricksApi
from dbacademy.rest.common import HttpErrorCodes, Item, ItemId, ApiClient
from dbacademy.rest.crud import CRUD


class Pools(CRUD):
    def __init__(self, databricks: DatabricksApi):
        super().__init__(databricks, "2.0/instance-pools", "instance_pool")
        self.databricks = databricks

    def _list(self, *, expected: HttpErrorCodes = None) -> List[Item]:
        result = self.databricks.api("GET", f"{self.path}/list")
        return result.get("instance_pools", [])

    def _get(self, item_id: ItemId, *, expected: HttpErrorCodes = None) -> Item:
        result = self.databricks.api_simple("GET", f"{self.path}/get?{self.id_key}={item_id}")
        return result

    def _create(self, item: Item, *, expected: HttpErrorCodes = None) -> ItemId:
        result = self.databricks.api("POST", f"{self.path}/create", item)
        return result["instance_pool_id"]

    def _update(self, item: Item, *, expected: HttpErrorCodes = None) -> ItemId:
        pass

    def _delete(self, item_id, *, expected: HttpErrorCodes = None):
        pass

    def create(self, name, machine_type=None, min_idle=3):
        if machine_type is None:
            machine_type = self.databricks.default_machine_type
        data = {
            'instance_pool_name': name,
            'min_idle_instances': min_idle,
            'node_type_id': machine_type,
            'idle_instance_autotermination_minutes': 5,
            'enable_elastic_disk': True,
            'preloaded_spark_versions': [self.databricks.default_preloaded_versions],
        }
        response = self.databricks.api("POST", "2.0/instance-pools/create", data)
        return response["instance_pool_id"]

    def edit(self, pool, min_idle):
        if isinstance(pool, str):
            pool = self.get_by_id(pool)
        valid_keys = ['instance_pool_id', 'instance_pool_name', 'min_idle_instances',
                      'node_type_id', 'idle_instance_autotermination_minutes']
        data = {key: pool[key] for key in valid_keys}
        data["min_idle_instances"] = min_idle
        response = self.databricks.api("POST", "2.0/instance-pools/edit", data)
        return pool["instance_pool_id"]

    def edit_by_name(self, name, min_idle):
        pool = self.get_by_name(name)
        return self.edit(pool, min_idle)

    def edit_or_create(self, name, machine_type=None, min_idle=3):
        if machine_type is None:
            machine_type = self.databricks.default_machine_type
        pool = self.get_by_name(name)
        if pool:
            return self.edit(pool, min_idle)
        else:
            return self.create(name, machine_type, min_idle)

    def set_acl(self, instance_pool_id,
                user_permissions: Dict[str, str] = {},
                group_permissions: Dict[str, str] = {"users": "CAN_ATTACH_TO"}):
        data = {
            "access_control_list": [
                                       {
                                           "user_name": user_name,
                                           "permission_level": permission,
                                       } for user_name, permission in user_permissions.items()
                                   ] + [
                                       {
                                           "group_name": group_name,
                                           "permission_level": permission,
                                       } for group_name, permission in group_permissions.items()
                                   ]
        }
        return self.databricks.api(
            "PUT", f"2.0/preview/permissions/instance-pools/{instance_pool_id}", data)

    def add_to_acl(self, instance_pool_id,
                   user_permissions: Dict[str, str] = {},
                   group_permissions: Dict[str, str] = {"users": "CAN_ATTACH_TO"}):
        data = {
            "access_control_list": [
                                       {
                                           "user_name": name,
                                           "permission_level": permission,
                                       } for name, permission in user_permissions.items()
                                   ] + [
                                       {
                                           "group_name": name,
                                           "permission_level": permission,
                                       } for name, permission in group_permissions.items()
                                   ]
        }
        return self.databricks.api(
            "PATCH", f"2.0/preview/permissions/instance-pools/{instance_pool_id}", data)
