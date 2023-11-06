import json
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
from dbacademy.common import Cloud


class Special(Enum):
    UNTESTED = "UNTESTED"


@dataclass
class LabSpec:
    enabled: bool
    cloud: Cloud
    name: str
    url: str

    account_id: str
    token: Optional[str]
    username: str
    password: str

    service_principle: str

    course: str
    version: str

    cluster_spec: Dict[str, Any]
    # api: DBAcademyRestClient

    def create_cluster_spec(self, runtime_engine: str, dbr: str) -> Dict[str, Any]:

        if runtime_engine == "STANDARD":
            spark_version = f"{dbr}.x-scala2.12"
        elif runtime_engine == "PHOTON":
            spark_version = f"{dbr}.x-scala2.12"
        elif runtime_engine == "ML":
            runtime_engine = "STANDARD"
            spark_version = f"{dbr}.x-cpu-ml-scala2.12"
        else:
            raise ValueError(f"""Invalid runtime engine, found "{runtime_engine}".""")

        spec = {
            "num_workers": 0,
            "spark_version": spark_version,
            "spark_conf": {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*, 4]",
            },
            "custom_tags": {
                "ResourceClass": "SingleNode"
            },
            # "policy_id": "E0631F5C0D0035E0",
            # "driver_instance_pool_id": "1205-215430-cog54-pool-s8ttwetu",
            # "instance_pool_id": "1205-215430-cog54-pool-s8ttwetu",
            "node_type_id": None,
            "driver_node_type_id": None,
            "autotermination_minutes": 60,
            "data_security_mode": "NONE",
            "runtime_engine": runtime_engine,
            "enable_elastic_disk": None,
            "disk_spec": None,
        }

        if self.cloud.is_aws:
            spec["node_type_id"] = "i3.xlarge"
            spec["driver_node_type_id"] = "i3.xlarge"
            spec["aws_attributes"] = {
                "spot_bid_price_percent": None
            }

        elif self.cloud.is_msa:
            spec["node_type_id"] = "Standard_D4ds_v4"
            spec["driver_node_type_id"] = "Standard_D4ds_v4"
            spec["azure_attributes"] = {
                "first_on_demand": 1,
                "availability": "ON_DEMAND_AZURE",
                "spot_bid_max_price": -1
            }

        elif self.cloud.is_gcp:
            raise ValueError(f"""The cloud "{self.cloud.name}" is not implemented.""")
        else:
            raise ValueError(f"""Invalid cloud, found "{self.cloud.name}".""")

        return spec


def create_lab(*, lab_specs: Dict[str, LabSpec], global_config: Dict[str, Any], global_cloud_config: Dict[str, Any], cloud: str, i: int, lab_spec_def: Dict[str, Any]):
    from dbacademy.clients import databricks

    base_name = lab_spec_def.get("name")
    assert base_name is not None, f"""The parameter "name" was not found for lab #{i}."""

    definition = dict()                         # Start with the empty dict
    definition.update(global_config)            # Then add the global config
    definition.update(global_cloud_config)      # Then cloud-specific
    definition.update(lab_spec_def)             # Finally the actual labs

    name = f"{base_name} ({cloud.upper()})"     # Rewrite the name
    definition["name"] = name                   # Update the name

    definition["cloud"] = Cloud(cloud.upper())  # Define the cloud str to enum

    try:
        lab_spec = LabSpec(**definition)
        lab_spec.api = databricks.from_token(token=lab_spec.token, endpoint=lab_spec.url)
        assert name not in lab_specs.keys(), f"""Duplicate lab name, found "{name}" twice."""
        lab_specs[name] = lab_spec

    except AssertionError as e:
        raise e
    except Exception as e:
        raise Exception(f"Exception creating lab definition for {name}") from e


def load_lab_specs(config_path: str) -> Dict[str, LabSpec]:
    lab_specs_definition = json.load(open(config_path))
    global_config = lab_specs_definition.get("global_config")

    lab_specs: Dict[str, LabSpec] = dict()

    for cloud in ["aws", "msa"]:
        global_cloud_config = lab_specs_definition.get(f"global_{cloud}_config")
        lab_spec_defs = lab_specs_definition.get(f"labs-{cloud}", list())

        for i, lab_spec_def in enumerate(lab_spec_defs):
            create_lab(lab_specs=lab_specs,
                       global_config=global_config,
                       global_cloud_config=global_cloud_config,
                       cloud=cloud,
                       i=i, lab_spec_def=lab_spec_def)

    return lab_specs
