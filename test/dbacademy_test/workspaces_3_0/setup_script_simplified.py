import os
import re
import time
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.dougrest import AccountsApi
from dbacademy.rest.common import DatabricksApiException


class Config(object):
    """An all-purpose struct for grouping together configuration values."""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


# Read environment variables
env_id = "PROSVC"
cds_api_token = os.environ.get("WORKSPACE_SETUP_CDS_API_TOKEN") or "X"
account_id = os.environ.get(f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID") or ""
password = os.environ.get(f"WORKSPACE_SETUP_{env_id}_PASSWORD")
username = os.environ.get(f"WORKSPACE_SETUP_{env_id}_USERNAME")
assert account_id is not None, f"""Failed to load the environment variable "{f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID"}", please check your configuration and try again."""
assert password is not None, f"""Failed to load the environment variable "{f"WORKSPACE_SETUP_{env_id}_PASSWORD"}", please check your configuration and try again."""
assert username is not None, f"""Failed to load the environment variable "{f"WORKSPACE_SETUP_{env_id}_USERNAME"}", please check your configuration and try again."""

# Accounts console client
accounts_api = AccountsApi(account_id=account_id,
                           user=username,
                           password=password)

# Configuration
cloud = "AWS"
region = "us-west-2"
lab_id = 910  # on-demand lab id

uc_storage_config = Config(
    region=region,
    storage_root=f"s3://unity-catalogs-{region}/",
    aws_iam_role_arn="arn:aws:iam::981174701421:role/Unity-Catalog-Role",
    msa_access_connector_id=None)

workspace_config = Config(
    lab_id=lab_id,
    lab_description=f"Example lab test-{lab_id}",
    workspace_name=f"test-{lab_id}",
    default_dbr="11.3.x-cpu-ml-scala2.12",
    default_node_type_id="i3.xlarge",
    credentials_name="default",
    storage_configuration=region,  # Not region, just named after the region.
    region=region,
    uc_storage_config=uc_storage_config,
    entitlements={
        "allow-cluster-create": False,  # False to enforce policy
        "allow-instance-pool-create": False,  # False to enforce policy
        "databricks-sql-access": True,
        "workspace-access": True,
    },
    courseware={
        "ml-in-prod": f"https://labs.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production&token={cds_api_token}"
    },
    datasets=[]  # Appended to based on course_definitions; Only needs to be defined for DAWD
)


def create_workspace():
    # Get or Create Workspace
    # Azure: Query for existing workspace (created using ARM templates)
    workspace_api = accounts_api.workspaces.get_by_name(workspace_config.workspace_name, if_not_exists="ignore")
    if workspace_api is None:
        # AWS: Create workspace
        workspace_api = accounts_api.workspaces.create(workspace_name=workspace_config.workspace_name,
                                                       deployment_name=workspace_config.workspace_name,
                                                       region=workspace_config.region,
                                                       credentials_name=workspace_config.credentials_name,
                                                       storage_configuration_name=workspace_config.storage_configuration)

    # Create API Clients
    client = DBAcademyRestClient(authorization_header=workspace_api.authorization_header, endpoint=workspace_api.url[:-5])
    # Trio
    workspace = Config(workspace_config=workspace_config,
                       workspace_api=workspace_api,
                       client=client)

    # Workspaces were all created synchronously, blocking here until we confirm that all workspaces are created
    print("Waiting until the workspace is ready.")
    workspace.workspace_api.wait_until_ready()

    # Configure the entitlements for the group "users"
    print("Configure the entitlements for the group 'users'")
    users_group_id = workspace.client.scim.groups.get_by_name("users")["id"]
    for name, value in workspace_config.entitlements.items():
        if value:
            workspace.client.scim.groups.add_entitlement(users_group_id, name)
        else:
            workspace.client.scim.groups.remove_entitlement(users_group_id, name)

    # Add users to the workspace
    print("Add users to the workspace")
    usernames = []  # Put list of usernames to create here
    for username in usernames:
        workspace.client.scim.users.create(username)

    # Create an instructors group at the account-level for the metastore
    print("Create an instructors group at the account-level for the metastore")
    instructors = ["class+000@databricks.com"]  # Provide the list of instructors here.
    instructors_group_name = f"instructors-{workspace_config.workspace_name}"
    response = accounts_api.api("GET", "scim/v2/Users", count=1000)
    users = {u["userName"]: u for u in response["Resources"]}
    group_members = [{"values": users[instructor]["id"]} for instructor in instructors]
    groups = accounts_api.api("GET", "scim/v2/Groups")["Resources"]
    # TODO: Cleanup
    instructors_group = next((g for g in groups if g["displayName"] == instructors_group_name), None)
    if instructors_group is None:
        instructors_group = accounts_api.api("POST", "/scim/v2/Groups", {
            "displayName": instructors_group_name,
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "members": group_members,
        })

    # Add instructors as workspace admins
    print("Add instructors as workspace admins")
    admins_group_id = workspace.client.scim.groups.get_by_name("admins")["id"]
    # TODO: Can we just add the instructors group to the admins group?
    workspace.client.scim.groups.add_member(admins_group_id, instructors_group["id"])
    for user_name in instructors:
        user_id = workspace.workspace_api.users.create(user_name, allow_cluster_create=False)["id"]
        workspace.client.scim.groups.add_member(admins_group_id, user_id)

    # Create a new metastore
    print("Create a new metastore")
    metastore_id = workspace.workspace_api.api("POST", "2.1/unity-catalog/metastores", {
        "name": workspace.workspace_config.workspace_name,
        "storage_root": uc_storage_config.storage_root,
        "region": uc_storage_config.region
    })["metastore_id"]

    # Configure the metastore settings
    print("Configure the metastore settings")
    workspace.workspace_api.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
        "owner": instructors_group["displayName"],
        "delta_sharing_scope": "INTERNAL_AND_EXTERNAL",
        "delta_sharing_recipient_token_lifetime_in_seconds": 90 * 24 * 60 * 60,  # 90 days
        "delta_sharing_organization_name": workspace.workspace_config.workspace_name
    })

    # Assign the metastore to the workspace
    print("Assign the metastore to the workspace")
    workspace_id = workspace.workspace_api["workspace_id"]
    workspace.workspace_api.api("PUT", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
        "metastore_id": metastore_id,
        "default_catalog_name": "main"
    })

    # Grant all users the permissions to create resources in the metastore
    print("Grant all users the permissions to create resources in the metastore")
    workspace.workspace_api.api("PATCH", f"2.1/unity-catalog/permissions/metastore/{metastore_id}", {
        "changes": [{
            "principal": "account users",
            "add": ["CREATE CATALOG", "CREATE EXTERNAL LOCATION", "CREATE SHARE", "CREATE RECIPIENT", "CREATE PROVIDER"]
        }]
    })

    # Create the storage credential for the metastore
    print("Create the storage credential for the metastore")
    credentials_spec = {
        "name": workspace.workspace_config.workspace_name,
        "skip_validation": False,
        "read_only": False,
    }
    if uc_storage_config.aws_iam_role_arn is not None:
        credentials_spec["aws_iam_role"] = {
            "role_arn": uc_storage_config.aws_iam_role_arn
        }
    if uc_storage_config.msa_access_connector_id is not None:
        credentials_spec["azure_managed_identity"] = {
            "access_connector_id": uc_storage_config.msa_access_connector_id
        }
    credentials = workspace.workspace_api.api("POST", "2.1/unity-catalog/storage-credentials", credentials_spec)
    storage_root_credential_id = credentials["id"]
    workspace.workspace_api.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
        "storage_root_credential_id": storage_root_credential_id
    })

    # Enable serverless SQL Warehouses
    print("Enable serverless SQL Warehouses")
    settings = workspace.workspace_api.api("GET", "2.0/sql/config/endpoints")
    settings["enable_serverless_compute"] = True

    # Configure workspace feature flags
    print("Configure workspace feature flags")
    workspace.workspace_api.api("PATCH", "2.0/workspace-conf", {
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
    if ".cloud.databricks.com" in workspace_api.url:  # AWS
        cloud_attributes = {
            "node_type_id": "i3.xlarge",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 100
            },
        }
    elif ".gcp.databricks.com" in workspace_api.url:  # GCP
        cloud_attributes = {
            "node_type_id": "n1-highmem-4",
            "gcp_attributes": {
                "use_preemptible_executors": True,
                "availability": "PREEMPTIBLE_WITH_FALLBACK_GCP",
            },
        }
    elif ".azuredatabricks.net" in workspace_api.url:  # Azure
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
    workspace_hostname = re.match("https://([^/]+)/api/", workspace_api.url)[1]
    job_spec = {
        "name": "DBAcademy Workspace-Setup",
        "timeout_seconds": 60 * 60 * 6,  # 6 hours
        "max_concurrent_runs": 1,
        "tasks": [{
            "task_key": "Workspace-Setup",
            "notebook_task": {
                "notebook_path": "Workspace-Setup",
                "base_parameters": {
                    "lab_id": f"Classroom #{workspace.workspace_config.lab_id}",
                    "description": f"Classroom #{workspace.workspace_config.lab_description}",
                    "node_type_id": workspace.workspace_config.default_node_type_id,
                    "spark_version": workspace.workspace_config.default_dbr,
                    "datasets": ",".join(workspace.workspace_config.datasets),
                    "courses": ",".join(workspace.workspace_config.courseware.values()),
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
    job_id = workspace_api.jobs.create_multi_task_job(**job_spec)
    run_id = workspace_api.jobs.run(job_id)["run_id"]

    # Wait for job completion
    print("Wait for job completion")
    while True:
        response = workspace_api.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
        life_cycle_state = response.get("state").get("life_cycle_state")
        if life_cycle_state not in ["PENDING", "RUNNING", "TERMINATING"]:
            job_state = response.get("state", {})
            job_result = job_state.get("result_state")
            job_message = job_state.get("state_message", "Unknown")
            break
        time.sleep(60)  # seconds
    if job_result != "SUCCESS":
        raise Exception(
            f"""Expected the final state of Universal-Workspace-Setup to be "SUCCESS", found "{job_result}" for "{workspace_config.workspace_name}" | {job_message}""")

    # All done
    print("Done")


def remove_workspace():
    # Get the Workspace
    workspace_api = accounts_api.workspaces.get_by_name(workspace_config.workspace_name, if_not_exists="ignore")
    if not workspace_api:
        print("Workspace already removed.")
        return

    # Create API Clients
    client = DBAcademyRestClient(authorization_header=workspace_api.authorization_header, endpoint=workspace_api.url[:-5])
    # Trio
    workspace = Config(workspace_config=workspace_config,
                       workspace_api=workspace_api,
                       client=client)

    # Delete the Metastore
    print("Remove the metastore")
    try:
        response = workspace.workspace_api.api("GET", f"2.1/unity-catalog/current-metastore-assignment")
        metastore_id = response["metastore_id"]
        workspace_id = response["workspace_id"]
        workspace.workspace_api.api("DELETE", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
            "metastore_id": metastore_id
        })
        workspace.workspace_api.api("DELETE", f"2.1/unity-catalog/metastores/{metastore_id}", {
            "force": True
        })
    except DatabricksApiException as e:
        if e.error_code != "METASTORE_DOES_NOT_EXIST":
            raise e

    # Remove the instructors group
    print("Remove the instructors group")
    instructors_group_name = f"instructors-{workspace_config.workspace_name}"
    groups = accounts_api.api("GET", "scim/v2/Groups")["Resources"]
    instructors_group = next((g for g in groups if g["displayName"] == instructors_group_name), None)
    if instructors_group is not None:
        instructors_group = accounts_api.api("DELETE", f"/scim/v2/Groups/{instructors_group['id']}")

    # Remove the Workspace
    print("Remove the workspace")
    accounts_api.workspaces.delete_by_name(workspace_config.workspace_name)


remove_workspace()
