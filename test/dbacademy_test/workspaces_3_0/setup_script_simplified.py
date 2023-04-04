import os
import re
from typing import List
from time import sleep
from dbacademy import common
from dbacademy.dougrest import AccountsApi
from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup, WorkspaceTrio
from dbacademy.classrooms.classroom import Classroom


total_workspaces = 1
first_workspace_number = 1
specific_workspace_numbers = [1]
max_participants = 250  # Max users per workspace


# TODO There is a bug that doesn't allow us to use the "instructors" group for the owner
uc_storage_config = UcStorageConfig(storage_root="s3://unity-catalogs-us-west-2/",
                                    storage_root_credential_id="be2549d1-3f5b-40db-900d-1b0fcdb419ee",  # ARN
                                    region="us-west-2",
                                    meta_store_owner="instructors",
                                    aws_iam_role_arn="arn:aws:iam::981174701421:role/Unity-Catalog-Role",
                                    msa_access_connector_id=None)

workspace_config_template = WorkspaceConfig(max_participants=max_participants,
                                            datasets=None,  # Appended to based on course_definitions; Only needs to be defined for DAWD
                                            course_definitions=[
                                                # "course=welcome",
                                                # "course=example-course&version=v1.1.8",
                                                # "course=template-course&version=v1.0.0&artifact=template-course.dbc",
                                                # "https://labs.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production"
                                            ],
                                            cds_api_token=os.environ.get("WORKSPACE_SETUP_CDS_API_TOKEN"),
                                            default_dbr="11.3.x-cpu-ml-scala2.12",
                                            default_node_type_id="i3.xlarge",
                                            credentials_name="default",
                                            storage_configuration="us-west-2",                         # Not region, just named after the region.
                                            username_pattern="class+{student_number}@databricks.com",  # student_number defaults to 3 digits, zero prefixed
                                            entitlements={  # TODO - this should be applied to the "users" group, not each user.
                                                "allow-cluster-create": False,        # False to enforce policy
                                                "allow-instance-pool-create": False,  # False to enforce policy
                                                "databricks-sql-access": True,
                                                "workspace-access": True,
                                            },
                                            # Cloud Labs needs are different here because they have a list of users, not a range of users.
                                            workspace_name_pattern="classroom-{workspace_number}",  # workspace_number defaults to 3 digits, zero prefixed
                                            workspace_group={                                       # User class+000@ should always be an admin
                                                "admins": [0],                                      # Defined as user "0" or by full username.
                                                # These should be defined at the account level.
                                                # "instructors": [0],                               # User class+000@ should always be an instructor
                                                # "analysts": ["class+analyst@databricks.com"],     # "Special" config for DEWD/ADEWD v3
                                            })

env_id = "PROSVC"
account_id = os.environ.get(f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID")
password = os.environ.get(f"WORKSPACE_SETUP_{env_id}_PASSWORD")
username = os.environ.get(f"WORKSPACE_SETUP_{env_id}_USERNAME")
assert account_id is not None, f"""Failed to load the environment variable "{f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID"}", please check your configuration and try again."""
assert password is not None, f"""Failed to load the environment variable "{f"WORKSPACE_SETUP_{env_id}_PASSWORD"}", please check your configuration and try again."""
assert username is not None, f"""Failed to load the environment variable "{f"WORKSPACE_SETUP_{env_id}_USERNAME"}", please check your configuration and try again."""

account = AccountConfig(region="us-west-2",
                        account_id=account_id,
                        username=username,
                        password=password,
                        uc_storage_config=uc_storage_config,
                        workspace_config_template=workspace_config_template,
                        ignored_workspaces=[],
                        first_workspace_number=0,
                        specific_workspace_numbers=specific_workspace_numbers,
                        total_workspaces=0)


workspace_setup = WorkspaceSetup(account)
workspace_setup.__errors: List[str] = list()
workspace_setup.__workspaces: List[WorkspaceTrio] = list()
workspace_setup.__account_config = common.verify_type(AccountConfig, non_none=True, account_config=account)
workspace_setup.__accounts_api = AccountsApi(account_id=workspace_setup.account_config.account_id,
                                             user=workspace_setup.account_config.username,
                                             password=workspace_setup.account_config.password)


# Start workspace setup

workspace_config = workspace_setup.account_config.workspaces[0]

workspace_api = workspace_setup.accounts_api.workspaces.create(workspace_name=workspace_config.workspace_name,
                                                               deployment_name=workspace_config.workspace_name,
                                                               region=workspace_setup.account_config.region,
                                                               credentials_name=workspace_setup.account_config.workspace_config_template.credentials_name,
                                                               storage_configuration_name=workspace_setup.account_config.workspace_config_template.storage_configuration)
classroom = Classroom(num_students=len(workspace_config.usernames),
                      username_pattern=workspace_config.username_pattern,
                      databricks_api=workspace_api)

workspace = WorkspaceTrio(workspace_config, workspace_api, classroom)

# Workspaces were all created synchronously, blocking here until we confirm that all workspaces are created.
workspace.workspace_api.wait_until_ready()

# Configure the entitlements for the group "users"
entitlements = {
    "allow-cluster-create": False,        # False to enforce policy
    "allow-instance-pool-create": False,  # False to enforce policy
    "databricks-sql-access": True,
    "workspace-access": True,
}
users_group_id = workspace.client.scim.groups.get_by_name("users")["id"]
for name, value in entitlements.items():
    if value:
        workspace.client.scim.groups.add_entitlement(users_group_id, name)
    else:
        workspace.client.scim.groups.remove_entitlement(users_group_id, name)

# # Configuring users in each workspace.
# workspace_setup.create_users(workspace)

# Add instructor users as admins
admin_users = ["class+000@databricks.com"]
admins_group_id = workspace.client.scim.groups.get_by_name("admins")["id"]
for user_name in admin_users:
    user_id = workspace.get_by_username(username)["id"]
    print("UserID", user_id)
    workspace.client.scim.groups.add_member(admins_group_id, user_id)

# Create a new metastore
metastore_id = workspace.workspace_api.api("POST", "2.1/unity-catalog/metastores", {
    "name": workspace.workspace_config.name,
    "storage_root": workspace_setup.account_config.uc_storage_config.storage_root,
    "region": workspace_setup.account_config.uc_storage_config.region
})["metastore_id"]

# Configure the metastore settings
# TODO: Add Instructors to a temporary instructors group for this workspace.
workspace.workspace_api.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
    "owner": workspace_setup.account_config.uc_storage_config.meta_store_owner,
    "delta_sharing_scope": "INTERNAL_AND_EXTERNAL",
    "delta_sharing_recipient_token_lifetime_in_seconds": 90 * 24 * 60 * 60,  # 90 days
    "delta_sharing_organization_name": workspace.workspace_config.name
})

# Assign the metastore to the workspace
workspace_id = workspace.workspace_api["workspace_id"]
workspace.workspace_api.api("PUT", f"2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
    "metastore_id": metastore_id,
    "default_catalog_name": "main"
})

# Grant all users permission to create resources in the metastore
workspace.workspace_api.api("PATCH", f"2.1/unity-catalog/permissions/metastore/{metastore_id}", {
    "changes": [{
        "principal": "account users",
        "add": ["CREATE CATALOG", "CREATE EXTERNAL LOCATION", "CREATE SHARE", "CREATE RECIPIENT", "CREATE PROVIDER"]
    }]
})

# Create the storage credential for the metastore
credentials_spec = {
    "name": workspace.workspace_config.name,
    "skip_validation": False,
    "read_only": False,
}
if workspace_setup.account_config.uc_storage_config.aws_iam_role_arn is not None:
    credentials_spec["aws_iam_role"] = {
        "role_arn": workspace_setup.account_config.uc_storage_config.aws_iam_role_arn
    }
if workspace_setup.account_config.uc_storage_config.msa_access_connector_id is not None:
    credentials_spec["azure_managed_identity"] = {
        "access_connector_id": workspace_setup.account_config.uc_storage_config.msa_access_connector_id
    }
credentials = workspace.workspace_api.api("POST", "2.1/unity-catalog/storage-credentials", credentials_spec)
storage_root_credential_id = credentials["id"]
workspace.workspace_api.api("PATCH", f"2.1/unity-catalog/metastores/{metastore_id}", {
    "storage_root_credential_id": storage_root_credential_id
})

# Enabling serverless SQL Warehouses.
settings = workspace.workspace_api.api("GET", "2.0/sql/config/endpoints")
settings["enable_serverless_compute"] = True
workspace.workspace_api.api("PUT", "2.0/sql/config/endpoints", settings)

# Configure workspace feature flags
workspace.workspace_api.api("PATCH", "2.0/workspace-conf", {
    "enable-X-Frame-Options": "false",  # Turn off iframe prevention
    "intercomAdminConsent": "false",    # Turn off product welcome
    "enableDbfsFileBrowser": "true",    # Enable DBFS UI
    "enableWebTerminal": "true",        # Enable Web Terminal
    "enableExportNotebook": "true"      # We will disable this in due time
})

# # Install courseware for all users in the workspace
# workspace_setup.install_courseware(workspac)

# Cloud specific settings for the Universal Workspace-Setup
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
                "lab_id": f"Classroom #{workspace.workspace_config.workspace_number}",
                "description": f"Classroom #{workspace.workspace_config.workspace_number}",
                # "node_type_id": workspace.workspace_config.default_node_type_id,
                # "spark_version": workspace.workspace_config.default_dbr,
                "datasets": ",".join(workspace.workspace_config.dbc_urls),
                "courses": ",".join(workspace.workspace_config.datasets),
            },
            "source": "GIT"
        },
        "job_cluster_key": "Workspace-Setup-Cluster",
        "timeout_seconds": 0
    }],
    "job_clusters": [{
        "job_cluster_key": "Workspace-Setup-Cluster",
        "new_cluster": {
            "spark_version": "11.3.x-cpu-ml-scala2.12",
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
run_id = workspace.api("POST", "/2.1/jobs/run-now", {"job_id": job_id})["run_id"]

# Wait for Universal-Workspace-Setup run completion
while True:
    response = workspace.api("GET", f"/2.1/jobs/runs/get?run_id={run_id}")
    life_cycle_state = response.get("state").get("life_cycle_state")
    if life_cycle_state not in ["PENDING", "RUNNING", "TERMINATING"]:
        job_state = response.get("state", {})
        job_result = job_state.get("result_state")
        job_message = job_state.get("state_message", "Unknown")
        break
    sleep(60)  # seconds
if job_result != "SUCCESS":
    raise Exception(
        f"""Expected the final state of Universal-Workspace-Setup to be "SUCCESS", found "{job_result}" for "{workspace.name}" | {job_message}""")

