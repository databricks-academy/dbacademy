__all__ = ["Availability", "ClusterConfig", "JobClusterConfig", "LibraryFactory"]

from enum import Enum
from typing import Optional, Dict, Any, List, Union
from dbacademy.common import validate
from dbacademy.common import Cloud


class Availability(Enum):
    ON_DEMAND = "ON_DEMAND"
    SPOT = "SPOT"
    SPOT_WITH_FALLBACK = "SPOT_WITH_FALLBACK"

    @property
    def is_on_demand(self) -> bool:
        return self == Availability.ON_DEMAND

    @property
    def is_spot(self) -> bool:
        return self == Availability.SPOT

    @property
    def is_spot_with_fallback(self) -> bool:
        return self == Availability.SPOT_WITH_FALLBACK


class LibraryFactory:
    def __init__(self, libraries: Optional[List[Dict[str, Any]]]):
        # self.__definitions = libraries if libraries else list()
        self.__definitions: List[Dict[str, Any]] = validate(libraries=libraries).optional.list(dict, auto_create=True)

    @property
    def definitions(self) -> List[Dict[str, Any]]:
        return self.__definitions

    def jar(self, location: str):
        self.definitions.append({
            "jar": validate(location=location).required.str()
        })

    def egg(self, location: str):
        self.definitions.append({
            "egg": validate(location=location).required.str()
        })

    def wheel(self, location: str):
        self.definitions.append({
            "egg": validate(location=location).required.str()
        })

    def pypi(self, definition: Dict[str, Any]):
        self.definitions.append({
            "pypi": validate(definition=definition).required.dict(str)
        })

    def maven(self, definition: Dict[str, Any]):
        self.definitions.append({
            "maven": validate(definition=definition).required.dict(str)
        })

    def cran(self, definition: Dict[str, Any]):
        self.definitions.append({
            "cran": validate(definition=definition).required.dict(str)
        })

    def from_dict(self, library: Dict[str, Any]) -> None:
        self.definitions.append(library)


class CommonConfig:

    def __init__(self, *,
                 cloud: Union[str, Cloud],
                 cluster_name: Optional[str],
                 spark_version: str,
                 node_type_id: Optional[str],
                 driver_node_type_id: Optional[str],
                 instance_pool_id: Optional[str],
                 policy_id: Optional[str],
                 num_workers: int,
                 autotermination_minutes: Optional[int],
                 single_user_name: Optional[str],
                 availability: Optional[Union[str, Availability]],
                 spark_conf: Optional[Dict[str, str]],
                 spark_env_vars: Optional[Dict[str, str]],
                 custom_tags: Optional[Dict[str, str]],
                 extra_params: Optional[Dict[str, Any]],
                 libraries: Optional[List[Dict[str, Any]]]):

        print("="*80)
        print(f"instance_pool_id: {instance_pool_id}")
        print("="*80)

        self.__params = {
            "cluster_name": validate(cluster_name=cluster_name).optional.str(),
            "spark_version": validate(spark_version=spark_version).required.str(),
            "num_workers": validate(num_workers=num_workers).required.int(),
        }

        extra_params = validate(extra_params=extra_params).optional.dict(str, auto_create=True)
        spark_conf = validate(spark_conf=spark_conf).optional.dict(str, auto_create=True)
        spark_env_vars = validate(spark_env_vars=spark_env_vars).optional.dict(str, auto_create=True)
        custom_tags = validate(custom_tags=custom_tags).optional.dict(str, auto_create=True)

        if autotermination_minutes is not None:
            # Not set for job clusters
            self.__params["autotermination_minutes"] = validate(autotermination_minutes=autotermination_minutes).required.int()

        if instance_pool_id is not None:
            print(f"instance_pool_id: {instance_pool_id} & node_type_id: {node_type_id}")
            extra_params["instance_pool_id"]: validate(instance_pool_id=instance_pool_id).required.str()
            assert node_type_id is None, f"""The parameter "node_type_id" should be None when the parameter "instance_pool_id" is specified."""
        else:
            print(f"instance_pool_id: {instance_pool_id} & node_type_id: {node_type_id}")
            extra_params["node_type_id"] = validate(node_type_id=node_type_id).required.str()

        print("="*80)

        if policy_id is not None:
            extra_params["policy_id"] = validate(policy_id=policy_id).required.str()

        if single_user_name is not None:
            extra_params["single_user_name"] = validate(single_user_name=single_user_name).required.str()
            extra_params["data_security_mode"] = "SINGLE_USER"

        if num_workers == 0:
            # Don't use "local[*, 4] because the node type might have more cores
            custom_tags["ResourceClass"] = "SingleNode"
            spark_conf["spark.master"] = "local[*]"
            spark_conf["spark.databricks.cluster.profile"] = "singleNode"
            assert driver_node_type_id is None, f"""The parameter "driver_node_type_id" should be None when "num_workers" is zero."""
        else:
            # More than one worker so define the driver_node_type_id and if necessary, default it to the node_type_id
            extra_params["driver_node_type_id"] = validate(driver_node_type_id=driver_node_type_id or node_type_id).required.str()

        assert extra_params.get("custom_tags") is None, f"The parameter \"extra_params.custom_tags\" should not be specified directly, use \"custom_tags\" instead."
        assert extra_params.get("spark_conf") is None, f"The parameter \"extra_params.spark_conf\" should not be specified directly, use \"spark_conf\" instead."
        assert extra_params.get("spark_env_vars") is None, f"The parameter \"extra_params.spark_env_vars\" should not be specified directly, use \"spark_env_vars\" instead."

        assert extra_params.get("aws_attributes", dict()).get("availability") is None, f"The parameter \"aws_attributes.availability\" should not be specified directly, use \"availability\" instead."
        assert extra_params.get("azure_attributes", dict()).get("availability") is None, f"The parameter \"azure_attributes.availability\" should not be specified directly, use \"availability\" instead."
        assert extra_params.get("gcp_attributes", dict()).get("availability") is None, f"The parameter \"gcp_attributes.availability\" should not be specified directly, use \"availability\" instead."

        if instance_pool_id is None and availability is None:
            # Default to on-demand if the instance profile was not defined
            availability = Availability.ON_DEMAND

        cloud = validate(cloud=cloud).required.enum(Cloud, auto_convert=True)
        availability = validate(availability=availability).optional.enum(Availability, auto_convert=True)

        if availability is not None:
            assert instance_pool_id is None, f"The parameter \"availability\" cannot be specified when \"instance_pool_id\" is specified."

            cloud_attributes = f"{cloud.value.lower()}_attributes".replace("msa_", "azure_")
            extra_params[cloud_attributes] = dict()

            if cloud.is_aws:
                extra_params.get(cloud_attributes)["availability"] = availability.value

            elif cloud.is_msa:
                if availability.is_on_demand:
                    extra_params.get(cloud_attributes)["availability"] = "ON_DEMAND_AZURE"
                else:  # Same for SPOT and SPOT_WITH_FALLBACK
                    extra_params.get(cloud_attributes)["availability"] = "SPOT_WITH_FALLBACK_AZURE"

            elif cloud.is_gcp:
                if availability.is_on_demand:
                    extra_params.get(cloud_attributes)["availability"] = "ON_DEMAND_GCP"
                else:  # Same for SPOT and SPOT_WITH_FALLBACK
                    extra_params.get(cloud_attributes)["availability"] = "PREEMPTIBLE_WITH_FALLBACK_GCP"

        if len(custom_tags) > 0:
            extra_params["custom_tags"] = custom_tags

        if len(spark_conf) > 0:
            extra_params["spark_conf"] = spark_conf

        if len(spark_env_vars) > 0:
            extra_params["spark_env_vars"] = spark_env_vars

        # libraries are validated in the constructor.
        self.__libraries = LibraryFactory(libraries)
        extra_params["libraries"] = self.library_factory.definitions

        # Process last just in case there is an exclusion bug...
        # This will result in replacing any previously defined parameters
        self.__params.update(extra_params)

        import json
        print(f"| Extra Params")
        print(json.dumps(extra_params, indent=4))
        print(f"="*80)
        print(f"| Self Params")
        print(json.dumps(self.__params, indent=4))
        print(f"="*80)

    @property
    def library_factory(self) -> LibraryFactory:
        return self.__libraries

    @property
    def params(self) -> Dict[str, Any]:
        return self.__params


class ClusterConfig(CommonConfig):

    def __init__(self, *,
                 cloud: Union[str, Cloud],
                 cluster_name: Optional[str],
                 spark_version: str,
                 num_workers: int,
                 node_type_id: Optional[str] = None,
                 driver_node_type_id: Optional[str] = None,
                 instance_pool_id: Optional[str] = None,
                 policy_id: Optional[str] = None,
                 autotermination_minutes: int = 120,
                 single_user_name: Optional[str] = None,
                 availability: Optional[Union[str, Availability]] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 spark_env_vars: Optional[Dict[str, str]] = None,
                 custom_tags: Optional[Dict[str, str]] = None,
                 extra_params: Optional[Dict[str, Any]] = None,
                 libraries: Optional[List[Dict[str, Any]]] = None):

        # Parameters are validated in the call to CommonConfig
        super().__init__(cloud=cloud,
                         cluster_name=cluster_name,
                         spark_version=spark_version,
                         node_type_id=node_type_id,
                         driver_node_type_id=driver_node_type_id,
                         instance_pool_id=instance_pool_id,
                         policy_id=policy_id,
                         num_workers=num_workers,
                         autotermination_minutes=autotermination_minutes,
                         single_user_name=single_user_name,
                         spark_conf=spark_conf,
                         spark_env_vars=spark_env_vars,
                         custom_tags=custom_tags,
                         availability=availability,
                         extra_params=extra_params,
                         libraries=libraries)


class JobClusterConfig(CommonConfig):

    def __init__(self, *,
                 cloud: Union[str, Cloud],
                 # cluster_name: Optional[str],
                 spark_version: str,
                 num_workers: int,
                 node_type_id: Optional[str] = None,
                 driver_node_type_id: Optional[str] = None,
                 instance_pool_id: Optional[str] = None,
                 policy_id: Optional[str] = None,
                 # autotermination_minutes: int = 120,
                 single_user_name: Optional[str] = None,
                 availability: Optional[Union[str, Availability]] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 spark_env_vars: Optional[Dict[str, str]] = None,
                 custom_tags: Optional[Dict[str, str]] = None,
                 extra_params: Optional[Dict[str, Any]] = None,
                 libraries: Optional[List[Dict[str, Any]]] = None):

        # Parameters are validated in the call to CommonConfig
        super().__init__(cloud=cloud,
                         cluster_name=None,  # Not allowed when used as a job
                         spark_version=spark_version,
                         node_type_id=node_type_id,
                         driver_node_type_id=driver_node_type_id,
                         instance_pool_id=instance_pool_id,
                         policy_id=policy_id,
                         num_workers=num_workers,
                         autotermination_minutes=None,  # Not allowed when used as a job
                         single_user_name=single_user_name,
                         spark_conf=spark_conf,
                         spark_env_vars=spark_env_vars,
                         custom_tags=custom_tags,
                         availability=availability,
                         extra_params=extra_params,
                         libraries=libraries)
