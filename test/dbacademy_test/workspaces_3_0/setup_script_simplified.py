import os
import re
import time
from dataclasses import dataclass, field
from typing import Optional

from dbacademy.dougrest import AccountsApi, DatabricksApiException
from dbacademy.dougrest.accounts.workspaces import Workspace

# Read environment variables
env_id = "PROSVC"
cds_api_token = os.environ.get("WORKSPACE_SETUP_CDS_API_TOKEN")
account_id = os.environ.get(f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID")
account_username = os.environ.get(f"WORKSPACE_SETUP_{env_id}_USERNAME")
account_password = os.environ.get(f"WORKSPACE_SETUP_{env_id}_PASSWORD")
assert account_id is not None, f"""Set environment variable "{f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID"}"."""
assert account_username is not None, f"""Set environment variable "{f"WORKSPACE_SETUP_{env_id}_USERNAME"}"."""
assert account_password is not None, f"""Set environment variable "{f"WORKSPACE_SETUP_{env_id}_PASSWORD"}"."""

# Accounts console client
accounts_api = AccountsApi(account_id=account_id,
                           user=account_username,
                           password=account_password)


@dataclass
class WorkspaceConfig:
    cloud: str
    lab_id: int
    lab_description: str
    workspace_name: str
    region: str
    instructors: list[str]
    users: list[str]
    default_dbr: str = "11.3.x-cpu-ml-scala2.12"
    default_node_type_id: str = "i3.xlarge"
    credentials_name: str = "default"
    storage_configuration: str = None
    uc_storage_root: str = None
    uc_aws_iam_role_arn: Optional[str] = "arn:aws:iam::981174701421:role/Unity-Catalog-Role"
    uc_msa_access_connector_id: Optional[str] = None
    entitlements: dict[str, bool] = field(default_factory=lambda: dict(
        {
            "allow-cluster-create": False,  # False to enforce policy
            "allow-instance-pool-create": False,  # False to enforce policy
            "databricks-sql-access": True,
            "workspace-access": True,
        }))
    courseware: dict[str, str] = field(default_factory=lambda: dict(
        {
            # "folder_name": "https://..../import_url.dbc"
        }
    ))
    datasets: list[str] = None  # Only needs to be defined for DAWD

    def __post_init__(self):
        if self.lab_description is None:
            self.lab_description = f"Lab {self.lab_id:03d}"
        if self.workspace_name is None:
            self.workspace_name = f"training-{self.lab_id:03d}"
        if self.storage_configuration is None:
            self.storage_configuration = self.region  # Not region, just named after the region.
        if self.uc_storage_root is None:
            self.uc_storage_root = f"s3://unity-catalogs-{self.region}/"
        if self.datasets is None:
            self.datasets = []


def generate_usernames(first: int, last: int = None, pattern: str = "class+{num:03d}@databricks.com") -> list[str]:
    if last is None:
        last = first
    return [pattern.format(num=i) for i in range(first, last + 1)]


def create_workspace(config: WorkspaceConfig):
    # Azure: Query for existing workspace (created using ARM templates)
    print("Check for existing workspace")
    for workspace in accounts_api.api("GET", "/workspaces"):
        if workspace["workspace_name"] == config.workspace_name:
            # Found existing workspace
            break
    else:
        # AWS: Create workspace
        print("Create workspace")
        for c in accounts_api.api("GET", "/credentials"):
            if c["credentials_name"] == config.credentials_name:
                credentials_id = c["credentials_id"]
                break
        else:
            raise ValueError(f"Credentials '{config.credentials_name} does not exist.")
        for s in accounts_api.api("GET", "/storage-configurations"):
            if s["storage_configuration_name"] == config.storage_configuration:
                storage_configuration_id = s["storage_configuration_id"]
                break
        else:
            raise ValueError(f"Storage Configuration '{config.storage_configuration} does not exist.")
        workspace = accounts_api.api("POST", "/workspaces", {
            "workspace_name": config.workspace_name,
            "deployment_name": config.workspace_name,
            "aws_region": config.region,
            "credentials_id": credentials_id,
            "storage_configuration_id": storage_configuration_id
        })

    # Workspaces are created asynchronously, wait here until the workspace creation is complete.
    print("Waiting until the workspace is ready", end="")
    while workspace["workspace_status"] == "PROVISIONING":
        workspace_id = workspace["workspace_id"]
        response = accounts_api.api("GET", f"/workspaces/{workspace_id}")
        workspace.update(response)
        if workspace["workspace_status"] == "PROVISIONING":
            print(".", end="")
            time.sleep(15)
    print()
    workspace = Workspace(workspace, accounts_api)

    # Determine group id for users and admins
    all_groups = workspace.api("GET", "/2.0/preview/scim/v2/Groups").get("Resources", [])
    users_group = next((g for g in all_groups if g.get("displayName") == "users"))
    users_group_id = users_group["id"]
    admins_group = next((g for g in all_groups if g.get("displayName") == "admins"))
    admins_group_id = admins_group["id"]

    # Configure the entitlements for the group "users"
    print("Configure the entitlements for the group 'users'")
    # NOTE to JACOB: You may wish to do the entitlements update in one call like this
    # Hack: Seed at least 1 entitlement to work around a Databricks bug when there are no current entitlements
    operations = [{"op": "add", "value": {"entitlements": [{"value": "workspace-access"}]}}]
    for entitlement, keep in config.entitlements.items():
        if keep:
            operations.append({"op": "add", "value": {"entitlements": [{"value": entitlement}]}})
        else:
            operations.append({"op": "remove", "path": f'entitlements[value eq "{entitlement}"'})
    workspace.api("PATCH", f"/2.0/preview/scim/v2/Groups/{users_group_id}", {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": operations
    })

    # Add instructors to the workspace as admins
    print("Add instructors to the workspace as admins")
    for username in config.instructors:
        workspace.api("POST", "2.0/preview/scim/v2/Users", {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "userName": username,
            "groups": [{"value": admins_group_id}]
        })

    # Add users to the workspace
    print("Add users to the workspace")
    usernames = set(config.users) - set(config.users)  # Avoid duplicates
    for username in usernames:
        workspace.api("POST", "2.0/preview/scim/v2/Users", {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "userName": username,
        })

    # Create an instructors group at the account-level to serve as the metastore owner
    print("Create an instructors group at the account-level for the metastore")
    instructors_group_name = f"instructors-{config.workspace_name}"
    response = accounts_api.api("GET", "scim/v2/Users", count=1000)
    users = {u["userName"]: u for u in response["Resources"]}
    group_members = [{"values": users[instructor]["id"]} for instructor in config.instructors]
    instructors_group = accounts_api.api("POST", "/scim/v2/Groups", {
        "displayName": instructors_group_name,
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "members": group_members,
    })

    # Create a new metastore
    print("Create a new metastore")
    metastore_id = workspace.api("POST", "2.1/unity-catalog/metastores", {
        "name": config.workspace_name,
        "storage_root": config.uc_storage_root,
        "region": config.region
    })["metastore_id"]

    # Configure the metastore settings
    print("Configure the metastore settings")
    workspace.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
        "owner": instructors_group["displayName"],
        "delta_sharing_scope": "INTERNAL_AND_EXTERNAL",
        "delta_sharing_recipient_token_lifetime_in_seconds": 90 * 24 * 60 * 60,  # 90 days
        "delta_sharing_organization_name": config.workspace_name
    })

    # Assign the metastore to the workspace
    print("Assign the metastore to the workspace")
    workspace_id = workspace["workspace_id"]
    workspace.api("PUT", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
        "metastore_id": metastore_id,
        "default_catalog_name": "main"
    })

    # Grant all users the permissions to create resources in the metastore
    print("Grant all users the permissions to create resources in the metastore")
    workspace.api("PATCH", f"2.1/unity-catalog/permissions/metastore/{metastore_id}", {
        "changes": [{
            "principal": "account users",
            "add": ["CREATE CATALOG", "CREATE EXTERNAL LOCATION", "CREATE SHARE", "CREATE RECIPIENT", "CREATE PROVIDER"]
        }]
    })

    # Create the storage credential for the metastore
    print("Create the storage credential for the metastore")
    credentials_spec = {
        "name": config.workspace_name,
        "skip_validation": False,
        "read_only": False,
    }
    if config.uc_aws_iam_role_arn is not None:
        credentials_spec["aws_iam_role"] = {
            "role_arn": config.uc_aws_iam_role_arn
        }
    if config.uc_msa_access_connector_id is not None:
        credentials_spec["azure_managed_identity"] = {
            "access_connector_id": config.uc_msa_access_connector_id
        }
    credentials = workspace.api("POST", "2.1/unity-catalog/storage-credentials", credentials_spec)
    storage_root_credential_id = credentials["id"]
    workspace.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
        "storage_root_credential_id": storage_root_credential_id
    })

    # Enable serverless SQL Warehouses
    print("Enable serverless SQL Warehouses")
    settings = workspace.api("GET", "2.0/sql/config/endpoints")
    settings["enable_serverless_compute"] = True
    workspace.api("PUT", "2.0/sql/config/endpoints", settings)

    # Configure workspace feature flags
    print("Configure workspace feature flags")
    workspace.api("PATCH", "2.0/workspace-conf", {
        "enable-X-Frame-Options": "false",  # Turn off iframe prevention
        "intercomAdminConsent": "false",  # Turn off product welcome
        "enableDbfsFileBrowser": "true",  # Enable DBFS UI
        "enableWebTerminal": "true",  # Enable Web Terminal
        "enableExportNotebook": "true"  # We will disable this in due time
    })

    # # Install courseware for all users in the workspace
    # workspace_setup.install_courseware(workspace)

    # Cloud specific settings for the Universal Workspace-Setup
    print("Cloud specific settings for the Universal Workspace-Setup")
    if ".cloud.databricks.com" in workspace.url:  # AWS
        cloud_attributes = {
            "node_type_id": "i3.xlarge",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 100
            },
        }
    elif ".gcp.databricks.com" in workspace.url:  # GCP
        cloud_attributes = {
            "node_type_id": "n1-highmem-4",
            "gcp_attributes": {
                "use_preemptible_executors": True,
                "availability": "PREEMPTIBLE_WITH_FALLBACK_GCP",
            },
        }
    elif ".azuredatabricks.net" in workspace.url:  # Azure
        cloud_attributes = {
            "node_type_id": "Standard_DS3_v2",
            "azure_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK_AZURE"
            },
        }
    else:
        raise ValueError("Workspace is in unknown cloud.")

    # Run the Universal Workspace-Setup
    print("Run the Universal Workspace-Setup")
    workspace_hostname = re.match("https://([^/]+)/api/", workspace.url)[1]
    job_spec = {
        "name": "DBAcademy Workspace-Setup",
        "timeout_seconds": 60 * 60 * 6,  # 6 hours
        "max_concurrent_runs": 1,
        "tasks": [{
            "task_key": "Workspace-Setup",
            "notebook_task": {
                "notebook_path": "Workspace-Setup",
                "base_parameters": {
                    "lab_id": f"Classroom #{config.lab_id}",
                    "description": f"Classroom #{config.lab_description}",
                    "node_type_id": config.default_node_type_id,
                    "spark_version": config.default_dbr,
                    "datasets": ",".join(config.datasets),
                    "courses": ",".join(config.courseware.values()),
                },
                "source": "GIT"
            },
            "job_cluster_key": "Workspace-Setup-Cluster",
            "timeout_seconds": 0
        }],
        "job_clusters": [{
            "job_cluster_key": "Workspace-Setup-Cluster",
            "new_cluster": {
                "spark_version": "11.3.x-scala2.12",
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "custom_tags": {
                    "ResourceClass": "SingleNode",
                    "dbacademy.event_name": "Unknown",
                    "dbacademy.event_description": "Unknown",
                    "dbacademy.workspace": workspace_hostname
                },
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "enable_elastic_disk": True,
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
                "num_workers": 0
            }
        }],
        "git_source": {
            "git_url": "https://github.com/databricks-academy/workspace-setup.git",
            "git_provider": "gitHub",
            "git_branch": "published"
        },
        "format": "MULTI_TASK"
    }
    job_spec["job_clusters"][0]["new_cluster"].update(cloud_attributes)
    job_id = workspace.jobs.create_multi_task_job(**job_spec)
    run_id = workspace.jobs.run(job_id)["run_id"]

    # Wait for job completion
    print("Wait for job completion", end="")
    while True:
        response = workspace.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
        life_cycle_state = response.get("state").get("life_cycle_state")
        if life_cycle_state not in ["PENDING", "RUNNING", "TERMINATING"]:
            job_state = response.get("state", {})
            job_result = job_state.get("result_state")
            job_message = job_state.get("state_message", "Unknown")
            break
        time.sleep(60)  # timeout_seconds
        print(".", end="")
    print()
    if job_result != "SUCCESS":
        raise Exception(
            f"""Expected the final state of Universal-Workspace-Setup to be "SUCCESS", """
            f"""found "{job_result}" for "{config.workspace_name}" | {job_message}""")

    # All done
    print("Done")


def remove_workspace(workspace_name):
    # Remove the instructors group
    print(f"Steps to remove workspace '{workspace_name}:")
    print("Remove the instructors group")
    groups = accounts_api.api("GET", "scim/v2/Groups")["Resources"]
    instructors_group_name = f"instructors-{workspace_config.workspace_name}"
    instructors_group = next((g for g in groups if g["displayName"] == instructors_group_name), None)
    if instructors_group is not None:
        accounts_api.api("DELETE", f"/scim/v2/Groups/{instructors_group['id']}")

    # Get the workspace
    workspace_api = accounts_api.workspaces.get_by_name(workspace_name, if_not_exists="ignore")
    if not workspace_api:
        print("Workspace already removed.")
        return

    # Remove the metastore
    print("Remove the metastore")
    try:
        response = workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
        metastore_id = response["metastore_id"]
        workspace_id = response["workspace_id"]
        workspace_api.api("DELETE", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
            "metastore_id": metastore_id
        })
        workspace_api.api("DELETE", f"2.1/unity-catalog/metastores/{metastore_id}", {
            "force": True
        })
    except DatabricksApiException as e:
        if e.error_code != "METASTORE_DOES_NOT_EXIST":
            raise e

    # Remove the Workspace
    print("Remove the workspace")
    accounts_api.workspaces.delete_by_name(workspace_config.workspace_name, if_not_exists="ignore")


# Configuration
lab_id = 900
workspace_config = WorkspaceConfig(
    cloud="AWS",
    lab_id=lab_id,
    lab_description=f"Example lab test-{lab_id}",
    workspace_name=f"test-{lab_id}",
    region="us-west-2",
    courseware={
        "ml-in-prod": f"https://labs.training.databricks.com/api/v1/courses/download.dbc?"
                      f"course=ml-in-production&token={cds_api_token}"
    },
    instructors=["class+000@databricks.com"],
    users=generate_usernames(1, 250),
    # All items below would get good defaults, but adding here for completeness.
    default_dbr="11.3.x-cpu-ml-scala2.12",
    default_node_type_id="i3.xlarge",
    credentials_name="default",
    storage_configuration="us-west-2",  # Not region, just named after the region.
    uc_storage_root=f"s3://unity-catalogs-us-west-2/",
    uc_aws_iam_role_arn="arn:aws:iam::981174701421:role/Unity-Catalog-Role",
    uc_msa_access_connector_id=None,
    entitlements={
        "allow-cluster-create": False,  # False to enforce policy
        "allow-instance-pool-create": False,  # False to enforce policy
        "databricks-sql-access": True,
        "workspace-access": True,
    },
    datasets=[]  # Only needs to be defined for DAWD
)


create_workspace(workspace_config)
# remove_workspace(workspace_config.workspace_name)
