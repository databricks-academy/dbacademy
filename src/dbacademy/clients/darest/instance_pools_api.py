__all__ = ["InstancePoolsApi"]

from typing import List, Dict, Any, Optional, Tuple
from dbacademy.common import validate
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class InstancePoolsApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_uri = f"{self.__client.endpoint}/api/2.0/instance-pools"

    def get_by_id(self, instance_pool_id: str) -> Dict[str, Any]:
        return self.__client.api("GET", f"{self.base_uri}/get?instance_pool_id={instance_pool_id}")

    def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        pools: List[Dict[str, Any]] = self.list()
        for pool in pools:
            if pool.get("instance_pool_name") == name:
                return pool
        return None

    def list(self) -> List[Dict[str, Any]]:
        # Does not support pagination
        return self.__client.api("GET", f"{self.base_uri}/list").get("instance_pools", list())

    def create_or_update(self,
                         instance_pool_name: str,
                         idle_instance_autotermination_minutes: int,
                         min_idle_instances: int = 0,
                         max_capacity: int = None,
                         node_type_id: str = None,
                         preloaded_spark_version: str = None,
                         tags: List[Tuple[str, Any]] = None) -> Dict[str, Any]:

        pool = self.get_by_name(instance_pool_name)
        tags: List[Tuple[str, Any]] = validate(tags=tags).required.list(Tuple)

        # Convert empty string to None
        preloaded_spark_version = None if preloaded_spark_version is not None and preloaded_spark_version.strip() == "" else preloaded_spark_version

        if pool is not None:
            # Issue an update request to the pool
            instance_pool_id = pool.get("instance_pool_id")
            self.update_by_id(instance_pool_id=instance_pool_id,
                              instance_pool_name=instance_pool_name,
                              max_capacity=max_capacity,
                              min_idle_instances=min_idle_instances,
                              idle_instance_autotermination_minutes=idle_instance_autotermination_minutes,
                              node_type_id=node_type_id,
                              preloaded_spark_version=preloaded_spark_version)
        else:
            # Issue a create request for a new pool
            definition = {
                "max_capacity": max_capacity,
                "min_idle_instances": min_idle_instances,
                "idle_instance_autotermination_minutes": idle_instance_autotermination_minutes,
            }
            if node_type_id is not None:
                definition["node_type_id"] = node_type_id

            if preloaded_spark_version is not None:
                definition["preloaded_spark_versions"] = [preloaded_spark_version]

            result = self.create(instance_pool_name, definition, tags)
            instance_pool_id = result.get("instance_pool_id")

        return self.get_by_id(instance_pool_id)

    def create(self,
               name: str,
               definition: Dict[str, Any],
               tags: Optional[List[Tuple[str, Any]]] = None) -> Dict[str, Any]:

        from dbacademy.common import Cloud

        assert type(name) == str, f"Expected name to be of type str, found {type(name)}"
        assert type(definition) == dict, f"Expected definition to be of type dict, found {type(definition)}"

        name = validate(name=name).str()
        definition = validate(definition=definition).required.dict(str)
        tags: List[Tuple[str, Any]] = validate(tags=tags).list(Tuple, auto_create=True)

        definition["instance_pool_name"] = name

        custom_tags = list()
        for tag in tags:
            key_value = {
                "key": tag[0],
                "value": tag[1]
            }
            custom_tags.append(key_value)
        definition["custom_tags"] = custom_tags

        if Cloud.current_cloud().is_aws:
            definition["node_type_id"] = definition.get("node_type_id", "i3.xlarge")

            aws_attributes = definition.get("aws_attributes", {})
            aws_attributes["availability"] = aws_attributes.get("availability", "ON_DEMAND")

            definition["aws_attributes"] = aws_attributes

        elif Cloud.current_cloud().is_msa:
            definition["node_type_id"] = definition.get("node_type_id", "Standard_DS3_v2")

        elif Cloud.current_cloud().is_gcp:
            definition["node_type_id"] = definition.get("node_type_id", "n1-standard-4")

        else:
            raise Exception(f"The cloud {Cloud.current_cloud()} is not supported.")

        pool = self.__client.api("POST", f"{self.base_uri}/create", definition)
        return self.get_by_id(pool.get("instance_pool_id"))

    def update_by_name(self,
                       instance_pool_name: str,
                       min_idle_instances: int = None,
                       max_capacity: int = None,
                       idle_instance_autotermination_minutes: int = None) -> Dict[str, Any]:

        pool = self.get_by_name(instance_pool_name)
        assert pool is not None, f"A pool named \"{instance_pool_name}\" was not found."

        instance_pool_id = pool.get("instance_pool_id")

        return self.update_by_id(instance_pool_id=instance_pool_id,
                                 instance_pool_name=instance_pool_name,
                                 min_idle_instances=min_idle_instances,
                                 max_capacity=max_capacity,
                                 idle_instance_autotermination_minutes=idle_instance_autotermination_minutes)

    def update_by_id(self,
                     instance_pool_id: str,
                     instance_pool_name: str,
                     min_idle_instances: int = None,
                     max_capacity: int = None,
                     idle_instance_autotermination_minutes: int = None,
                     node_type_id: str = None,
                     preloaded_spark_version: str = None) -> Dict[str, Any]:

        assert type(instance_pool_id) == str, f"Expected id to be of type str, found {type(instance_pool_id)}"
        assert instance_pool_name is None or type(instance_pool_name) == str, f"Expected name to be of type str, found {type(instance_pool_name)}"
        assert min_idle_instances is None or type(min_idle_instances) == int, f"Expected min_idle_instances to be of type int, found {type(min_idle_instances)}"
        assert max_capacity is None or type(max_capacity) == int, f"Expected max_capacity to be of type int, found {type(max_capacity)}"
        assert idle_instance_autotermination_minutes is None or type(idle_instance_autotermination_minutes) == int, f"Expected idle_instance_autotermination_minutes to be of type int, found {type(idle_instance_autotermination_minutes)}"

        pool = self.get_by_id(instance_pool_id)

        params = {
            "instance_pool_id": instance_pool_id,
            "instance_pool_name": pool.get("instance_pool_name") if instance_pool_name is None else instance_pool_name
        }

        if max_capacity is not None:
            params["max_capacity"] = max_capacity
        if min_idle_instances is not None:
            params["min_idle_instances"] = min_idle_instances
        if idle_instance_autotermination_minutes is not None:
            params["idle_instance_autotermination_minutes"] = idle_instance_autotermination_minutes
        if node_type_id is not None:
            params["node_type_id"] = node_type_id
        if preloaded_spark_version is not None:
            params["preloaded_spark_versions"] = [preloaded_spark_version]

        self.__client.api("POST", f"{self.base_uri}/edit", params)
        return self.get_by_id(instance_pool_id)

    def delete_by_id(self, instance_pool_id) -> None:
        self.__client.api("POST", f"{self.base_uri}/delete", instance_pool_id=instance_pool_id, _expected=(200, 404))

        # The pool doesn't exist so there is nothing to do
        return None

    def delete_by_name(self, name) -> None:
        pool = self.get_by_name(name)

        if pool is not None:
            # We found the pool, delete it.
            instance_pool_id = pool.get("instance_pool_id")
            return self.delete_by_id(instance_pool_id)

        # The pool doesn't exist so there is nothing to do
        return None
