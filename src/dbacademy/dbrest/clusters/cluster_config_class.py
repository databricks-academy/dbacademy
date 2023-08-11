from enum import Enum
from typing import Optional, Dict, Any, List


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


class ClusterConfig:
    from dbacademy.common import Cloud

    def __init__(self, *,
                 cloud: Cloud,
                 cluster_name: Optional[str],
                 spark_version: str,
                 node_type_id: Optional[str],
                 driver_node_type_id: str = None,
                 instance_pool_id: str = None,
                 policy_id: str = None,
                 num_workers: int,
                 autotermination_minutes: Optional[int],
                 single_user_name: str = None,
                 availability: Availability = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 spark_env_vars: Optional[Dict[str, str]] = None,
                 custom_tags: Optional[Dict[str, str]] = None,
                 extra_params: Dict[str, Any] = None,
                 libraries: List[Dict] = None):

        self.__params = {
            "cluster_name": cluster_name,
            "spark_version": spark_version,
            "num_workers": num_workers,
            "node_type_id": node_type_id,
            "instance_pool_id": instance_pool_id,
            "autotermination_minutes": autotermination_minutes,
        }

        extra_params = extra_params or dict()
        spark_conf = spark_conf or dict()
        spark_env_vars = spark_env_vars or dict()
        custom_tags = custom_tags or dict()

        if policy_id is not None:
            extra_params["policy_id"] = policy_id

        if single_user_name is not None:
            extra_params["single_user_name"] = single_user_name
            extra_params["data_security_mode"] = "SINGLE_USER"

        if driver_node_type_id is not None:
            extra_params["driver_node_type_id"] = driver_node_type_id

        if num_workers == 0:
            # Don't use "local[*, 4] because the node type might have more cores
            spark_conf["spark.master"] = "local[*]"
            custom_tags["ResourceClass"] = "SingleNode"

            spark_conf["spark.databricks.cluster.profile"] = "singleNode"

        assert extra_params.get("custom_tags") is None, f"The parameter \"extra_params.custom_tags\" should not be specified directly, use \"custom_tags\" instead."
        assert extra_params.get("spark_conf") is None, f"The parameter \"extra_params.spark_conf\" should not be specified directly, use \"spark_conf\" instead."
        assert extra_params.get("spark_env_vars") is None, f"The parameter \"extra_params.spark_env_vars\" should not be specified directly, use \"spark_env_vars\" instead."

        assert extra_params.get("aws_attributes", dict()).get("availability") is None, f"The parameter \"aws_attributes.availability\" should not be specified directly, use \"availability\" instead."
        assert extra_params.get("azure_attributes", dict()).get("availability") is None, f"The parameter \"azure_attributes.availability\" should not be specified directly, use \"availability\" instead."
        assert extra_params.get("gcp_attributes", dict()).get("availability") is None, f"The parameter \"gcp_attributes.availability\" should not be specified directly, use \"availability\" instead."

        if instance_pool_id is None and availability is None:
            # Default to on-demand if the instance profile was not defined
            availability = Availability.ON_DEMAND

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
            self.__params["custom_tags"] = custom_tags

        if len(spark_conf) > 0:
            self.__params["spark_conf"] = spark_conf

        if len(spark_env_vars) > 0:
            self.__params["spark_env_vars"] = spark_env_vars

        class Library:
            def __init__(self, _libraries: List[Dict[str, Any]]):
                self.libraries = libraries

            def jar(self, location: str):
                self.libraries.append({
                    "jar": location
                })

            def egg(self, location: str):
                self.libraries.append({
                    "egg": location
                })

            def wheel(self, location: str):
                self.libraries.append({
                    "egg": location
                })

            def pypi(self, definition: Dict[str, Any]):
                self.libraries.append({
                    "pypi": definition
                })

            def maven(self, definition: Dict[str, Any]):
                self.libraries.append({
                    "maven": definition
                })

            def cran(self, definition: Dict[str, Any]):
                self.libraries.append({
                    "cran": definition
                })

            def from_dict(self, _libraries: Dict[str, Any]):
                self.libraries.append(_libraries)

        libraries = libraries if libraries else list()
        self.libraries = Library(libraries)
        extra_params["libraries"] = libraries

        # Process last just in case there is an exclusion bug...
        # This will result in replacing any previously defined parameters
        for key, value in extra_params.items():
            self.__params[key] = value

    @property
    def params(self) -> Dict[str, Any]:
        return self.__params


class JobClusterConfig(ClusterConfig):
    from dbacademy.common import Cloud

    def __init__(self, *,
                 cloud: Cloud,
                 # cluster_name: Optional[str],
                 spark_version: str,
                 node_type_id: Optional[str],
                 driver_node_type_id: str = None,
                 instance_pool_id: str = None,
                 policy_id: str = None,
                 num_workers: int,
                 autotermination_minutes: Optional[int],
                 single_user_name: str = None,
                 availability: Availability = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 spark_env_vars: Optional[Dict[str, str]] = None,
                 custom_tags: Optional[Dict[str, str]] = None,
                 extra_params: Dict[str, Any] = None,
                 libraries: List[Dict[str, Any]] = None):

        super().__init__(cloud=cloud,
                         cluster_name=None,
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
