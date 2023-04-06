import time, os
from dataclasses import dataclass
from typing import Optional
from dbacademy_test.workspaces_cloudlabs.simplified_rest_client import SimpleRestClient


@dataclass()
class WorkspaceConfig:
    account_id: str
    account_username: str
    account_password: str
    cloud: str
    lab_id: int
    lab_description: str
    workspace_name: str
    region: str
    instructors: list[str]
    instructors_group_name: str
    users: list[str]
    default_dbr: str
    default_node_type_id: str
    credentials_name: str
    storage_configuration: str
    uc_storage_root: str
    uc_aws_iam_role_arn: Optional[str]
    uc_msa_access_connector_id: Optional[str]
    entitlements: dict[str, bool]
    job_name: str
    courseware_urls: list[str]
    datasets: list[str]


def create_workspace(config: WorkspaceConfig):
    if config.cloud == "AWS":
        domain_suffix = ".cloud.databricks.com"
    elif config.cloud == "GCP":
        domain_suffix = ".gcp.databricks.com"
    elif config.cloud == "MSA":
        domain_suffix = ".azuredatabricks.net"
    else:
        raise Exception(f"Unsupported cloud, found {config.cloud}")

    accounts_api = SimpleRestClient(username=config.account_username, password=config.account_password, url=f"https://accounts.cloud.databricks.com")

    # Azure: Query for existing workspace (created using ARM templates)
    print(f"Looking for the workspace {config.workspace_name}.")
    response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/workspaces")
    workspace = next((w for w in response if w.get("workspace_name") == config.workspace_name), None)

    if workspace is None:
        # The workspace wasn't found, presuming to create one
        print(f"""Looking up the credentials "{config.credentials_name}" in {config.workspace_name}.""")
        response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/credentials")
        credentials_id = next((c.get("credentials_id") for c in response if c.get("credentials_name") == config.credentials_name), None)
        assert credentials_id is not None, f"""The credentials "{config.credentials_name}" were not found in {config.workspace_name}."""

        print(f"""Looking up the storage configuration "{config.storage_configuration}" in {config.workspace_name}.""")
        response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/storage-configurations")
        storage_configuration_id = next((s.get("storage_configuration_id") for s in response if s.get("storage_configuration_name") == config.storage_configuration), None)
        assert storage_configuration_id is not None, f"""The storage configuration "{config.storage_configuration}" was not found in {config.workspace_name}."""

        print(f"""Creating the workspace {config.workspace_name}.""")
        workspace = accounts_api.call("POST", f"/api/2.0/accounts/{config.account_id}/workspaces", {
            "workspace_name": config.workspace_name,
            "deployment_name": config.workspace_name,
            "aws_region": config.region,  # TODO don't hard code this
            "credentials_id": credentials_id,
            "storage_configuration_id": storage_configuration_id
        })

    workspace_id = workspace.get("workspace_id")
    deployment_name = workspace.get("deployment_name")

    # workspace_domain_name is used to create the workspace client, and later as a tag to the Universal-Workspace-Setup job
    workspace_domain_name = deployment_name + domain_suffix
    workspaces_api = SimpleRestClient(username=config.account_username, password=config.account_password, url=f"https://{workspace_domain_name}")

    ###############################################################################################
    # Workspaces are created asynchronously, wait here until the workspace creation is complete.
    ###############################################################################################
    print(f"Waiting until the workspace provisioning for {config.workspace_name} to complete.", end="...")
    start = time.time()
    timeout_seconds = 30 * 60  # Up to 30 minutes
    response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/workspaces/{workspace_id}")
    workspace_status = response.get("workspace_status")

    while workspace_status == "PROVISIONING":
        time.sleep(15)
        response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/workspaces/{workspace_id}")
        workspace_status = response.get("workspace_status")
        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Workspace not ready after waiting {timeout_seconds} seconds")

    print(f"{int(time.time() - start)} seconds")

    ###############################################################################################
    # Create the account level instructor's group
    ###############################################################################################
    print(f"""Creating the account-level group for instructors for use as the metastore admin in {config.workspace_name}.""")

    # List all account-level groups, required for conditional logic
    response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/scim/v2/Groups", count=1000)
    acct_instructors_group = next((u for u in response.get("Resources", list()) if config.instructors_group_name == u.get("displayName")), None)

    # if acct_instructors_group is not None:
    #     # TODO remove testing of create logic.
    #     group_id = acct_instructors_group.get("id")
    #     accounts_api.call("DELETE", f"/api/2.0/accounts/{config.account_id}/scim/v2/Groups/{group_id}")

    if acct_instructors_group is None:
        acct_instructors_group = accounts_api.call("POST", f"/api/2.0/accounts/{config.account_id}/scim/v2/Groups", {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "displayName": config.instructors_group_name,
        })

    # We will use the list of members next
    acct_group_id = acct_instructors_group.get("id")                                   # Get the group id
    acct_instructors_members = acct_instructors_group.get("members", dict())           # Convert the list of groups to list of members
    acct_instructors_members_ids = [i.get("value") for i in acct_instructors_members]  # Convert members to list of members_ids

    ###############################################################################################
    # Add the instructors at the account level and to the instructor's group
    ###############################################################################################
    print(f"""Creating the account-level instructors in {config.workspace_name}.""")

    # List all account-level users, required for conditional logic
    response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/scim/v2/Users", count=1000)  # Get the list of users. TODO result is paginated, this call assumes no more than 1000 users exist.
    all_acct_users = {u.get("userName"): u for u in response.get("Resources", list())}                       # Convert the list of users to list of usernames

    for instructor in config.instructors:
        # if instructor in all_acct_users:
        #     # TODO remove testing of create logic.
        #     user_id = all_acct_users.get(instructor).get("id")
        #     accounts_api.call("DELETE", f"/api/2.0/accounts/{config.account_id}/scim/v2/Users/{user_id}")
        #     del all_acct_users.get(instructor)

        acct_user = all_acct_users.get(instructor)
        if acct_user is None:
            payload = {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                "userName": instructor,
                # TODO Instructors can be added to the admin group but makes testing & re-execution a pain
                # "groups": [
                #     {
                #         "value": acct_group_id
                #     }
                # ],
            }
            acct_user = accounts_api.call("POST", f"/api/2.0/accounts/{config.account_id}/scim/v2/Users", payload)

        user_id = acct_user.get("id")

        if user_id not in acct_instructors_members_ids:
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{
                    "op": "add",
                    "value": {
                        "members": [
                            {
                                "value": user_id
                            }
                        ]
                    }
                }]
            }
            accounts_api.call("PATCH", f"/api/2.0/accounts/{config.account_id}/scim/v2/Groups/{acct_group_id}", payload)

    # Force a compilation error if I use it by accident
    del accounts_api

    ###############################################################################################
    # Load all the "existing" workspace attributes
    ###############################################################################################
    response = workspaces_api.call("GET", "/api/2.0/preview/scim/v2/Groups")
    all_groups = response.get("Resources", list())

    users_group = next(g for g in all_groups if g.get("displayName") == "users")
    users_group_id = users_group.get("id")

    admins_group = next((g for g in all_groups if g.get("displayName") == "admins"))
    admins_group_id = admins_group.get("id")
    admins_group_members = admins_group.get("members", dict())
    admins_group_members_ids = [i.get("value") for i in admins_group_members]

    response = workspaces_api.call("GET", "/api/2.0/preview/scim/v2/Users?excludedAttributes=roles")
    all_users = {u.get("userName"): u for u in response.get("Resources", list())}

    ###############################################################################################
    # Configure the entitlements for the group "users"
    ###############################################################################################
    print(f"""Configure the entitlements for the group "users" in {config.workspace_name}.""")
    operations = list()
    for entitlement, keep in config.entitlements.items():
        if keep:
            operations.append({
                "op": "add",
                "value": {
                    "entitlements": [
                        {
                            "value": entitlement
                        }
                    ]
                }
            })
        else:
            operations.append({"op": "remove", "path": f'entitlements[value eq "{entitlement}"'})

    params = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": operations
    }
    workspaces_api.call("PATCH", f"/api/2.0/preview/scim/v2/Groups/{users_group_id}", params)

    ###############################################################################################
    # Add instructors to the workspace
    ###############################################################################################
    print(f"Adding {len(config.instructors)} instructors as admins to the workspace {config.workspace_name}.")
    for instructor in config.instructors:
        user_instructor = all_users.get(instructor)
        if user_instructor is None:
            user_instructor = workspaces_api.call("POST", "/api/2.0/preview/scim/v2/Users", {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                "userName": instructor,
                # TODO Instructors can be added to the admin group but makes testing & re-execution a pain
                # "groups": [
                #     {
                #         "value": admins_group_id
                #     }
                # ]
            })

        # Add each instructor to the admin group
        # TODO Doug, do we want this group in the workspace?
        user_id = user_instructor.get("id")
        if user_id not in admins_group_members_ids:
            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{
                        "op": "add",
                        "value": {
                            "members": [
                                {
                                    "value": user_id
                                }
                            ]
                        }}]}
            workspaces_api.call("PATCH", f"/api/2.0/preview/scim/v2/Groups/{admins_group_id}", payload)

    ###############################################################################################
    # Add users to the workspace
    ###############################################################################################
    print(f"Adding {len(config.users)} users to the workspace {config.workspace_name}.")
    for username in config.users:
        if username not in all_users:
            params = {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                "userName": username,
                # Users are auto-added to the "users" group, no need to specify here.
                # "groups": [
                #     {
                #         "value": whatever_id
                #     }
                # ]
            }
            workspaces_api.call("POST", "/api/2.0/preview/scim/v2/Users", params)

    ###############################################################################################
    # Create the new metastore
    ###############################################################################################
    print(f"""Creating the metastore for {config.workspace_name}.""")
    response = workspaces_api.call("GET", "/api/2.1/unity-catalog/metastores")
    metastore_id = next((m.get("metastore_id") for m in response.get("metastores") if m.get("name") == config.workspace_name), None)

    if metastore_id is None:
        metastore = workspaces_api.call("POST", "/api/2.1/unity-catalog/metastores", {
            "name": config.workspace_name,
            "storage_root": config.uc_storage_root,
            "region": config.region
        })
        metastore_id = metastore.get("metastore_id")

    ###############################################################################################
    # Configure the metastore's settings
    ###############################################################################################
    print(f"""Configure the metastore for {config.workspace_name}.""")
    workspaces_api.call("PATCH", f"/api/2.1/unity-catalog/metastores/{metastore_id}", {
        "owner": config.instructors_group_name,
        "delta_sharing_scope": "INTERNAL_AND_EXTERNAL",
        "delta_sharing_recipient_token_lifetime_in_seconds": 90 * 24 * 60 * 60,  # 90 days
        "delta_sharing_organization_name": config.workspace_name
    })

    ###############################################################################################
    # Assign the metastore to the workspace
    ###############################################################################################
    print(f"""Assigning the metastore to {config.workspace_name}.""")
    workspace_id = workspace.get("workspace_id")
    workspaces_api.call("PUT", f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", {
        "metastore_id": metastore_id,
        "default_catalog_name": "main"
    })

    ###############################################################################################
    # Grant all users the permissions to create resources in the metastore
    ###############################################################################################
    print(f"""Grant all users the permissions to create resources in the metastore for {config.workspace_name}.""")
    workspaces_api.call("PATCH", f"/api/2.1/unity-catalog/permissions/metastore/{metastore_id}", {
        "changes": [{
            "principal": "account users",
            "add": ["CREATE CATALOG", "CREATE EXTERNAL LOCATION", "CREATE SHARE", "CREATE RECIPIENT", "CREATE PROVIDER"]
        }]
    })

    ###############################################################################################
    # Create the storage credential for the metastore
    ###############################################################################################
    print(f"""Updating the metastore's storage credentials for {config.workspace_name}.""")
    storage_credentials = workspaces_api.call("GET", f"/api/2.1/unity-catalog/storage-credentials/{config.workspace_name}", _expected=(200, 404))

    if storage_credentials is None:
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
        storage_credentials = workspaces_api.call("POST", "/api/2.1/unity-catalog/storage-credentials", credentials_spec)  # Create the storage credentials

    # Path the metastore with the storage credentials
    storage_root_credential_id = storage_credentials.get("id")
    workspaces_api.call("PATCH", f"/api/2.1/unity-catalog/metastores/{metastore_id}", {
        "storage_root_credential_id": storage_root_credential_id
    })

    ###############################################################################################
    # Enable serverless SQL Warehouses
    ###############################################################################################
    print(f"Enabling serverless SQL Warehouses in {config.workspace_name}.")
    settings = workspaces_api.call("GET", "/api/2.0/sql/config/endpoints")  # Get the current endpoint configuration
    settings["enable_serverless_compute"] = True                            # Appears to be the default for AWS as of 2023-04-01.
    workspaces_api.call("PUT", "/api/2.0/sql/config/endpoints", settings)   # Update the configuration

    ###############################################################################################
    # Delete default SQL Warehouses TODO already addressed in the "DBAcademy Workspace-Setup" job
    ###############################################################################################
    response = workspaces_api.call("GET", "/api/2.0/sql/warehouses")
    for warehouse in response.get("warehouses", list()):
        if "Starter Warehouse" in warehouse.get("name"):
            warehouse_id = warehouse.get("id")
            workspaces_api.call("DELETE", f"/api/2.0/sql/warehouses/{warehouse_id}", _expected=(200, 404))

    ###############################################################################################
    # Configure workspace feature flags
    ###############################################################################################
    print(f"Configuring workspace feature flags for {config.workspace_name}.")
    workspaces_api.call("PATCH", "/api/2.0/workspace-conf", {
        "enable-X-Frame-Options": "false",  # Turn off iframe prevention
        "intercomAdminConsent": "false",    # Turn off product welcome; TODO this doesn't appear to be working, seeing project dialog, getting started and VS Code advertisement
        "enableDbfsFileBrowser": "true",    # Enable DBFS UI
        "enableWebTerminal": "true",        # Enable Web Terminal
        "enableExportNotebook": "true"      # We will disable this in due time
    })

    ###############################################################################################
    # Delete the "DBAcademy Workspace-Setup" job if it exists
    # Deleting the job ensures that it gets recreated with the correct parameters but destroys the history needed when diagnosing problems
    # Also addresses jobs that are currently running by canceling before deleting.
    # TODO consider looking up the job runs, sorting by execution date/time, and then wait on that run to complete.
    ###############################################################################################
    print(f"""Looking for the job "{config.job_name}" in {config.workspace_name}.""")
    response = workspaces_api.call("GET", "/api/2.1/jobs/list")                                               # Get the list of jobs.
    jobs = response.get("jobs", list())                                                                       # TODO result is paginated, this call assumes no more than 25 jobs exist, expecting 0 or 1
    job_id = next((j.get("job_id") for j in jobs if j.get("settings").get("name") == config.job_name), None)  # Convert the list of jobs to a list of job_ids

    if job_id is not None:
        # TODO Check for active runs: If the job is running, deleting it can leave the environment in an unexpected state
        workspaces_api.call("POST", "/api/2.1/jobs/delete", {"job_id": job_id})

    ###############################################################################################
    # Configuring loud specific settings for the "DBAcademy Workspace-Setup" job
    ###############################################################################################
    print(f"""Configuring cloud specific settings for the job "{config.job_name}" in {config.workspace_name}.""")
    if ".cloud.databricks.com" in workspaces_api.url:  # AWS
        cloud_attributes = {
            "node_type_id": config.default_node_type_id,
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 100
            },
        }
    elif ".gcp.databricks.com" in workspaces_api.url:  # GCP
        cloud_attributes = {
            "node_type_id": config.default_node_type_id,
            "gcp_attributes": {
                "use_preemptible_executors": True,
                "availability": "PREEMPTIBLE_WITH_FALLBACK_GCP",
            },
        }
    elif ".azuredatabricks.net" in workspaces_api.url:  # MSA
        cloud_attributes = {
            "node_type_id": config.default_node_type_id,
            "azure_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK_AZURE"
            },
        }
    else:
        raise ValueError("Workspace is in an unknown cloud.")

    ###############################################################################################
    # Create and run the "DBAcademy Workspace-Setup" job
    ###############################################################################################
    print(f"""Creating the job "{config.job_name}" in {config.workspace_name}.""")
    job_spec = {
        "name": config.job_name,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "timeout_seconds": 60 * 60 * 2,  # 2 hours; installing all datasets takes about ~20 minutes, no longer running the Configure-Permissions sub-job
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
                    "courses": ",".join(config.courseware_urls),
                },
                "source": "GIT"
            },
            "job_cluster_key": "Workspace-Setup-Cluster",
            "timeout_seconds": 0
        }],
        "job_clusters": [{
            "job_cluster_key": "Workspace-Setup-Cluster",
            "new_cluster": {
                "spark_version": config.default_dbr,
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "custom_tags": {
                    "ResourceClass": "SingleNode",
                    "dbacademy.event_id": config.lab_id,
                    "dbacademy.event_description": config.lab_description,
                    "dbacademy.workspace": workspace_domain_name
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
    }
    # Patch in the cloud-specific attributes defined in the
    # previous section to the job spec's cluster configuration.
    job_cluster_spec = job_spec.get("job_clusters")[0]                              # Get the first (and only) job_clusters from the job_spec
    job_cluster_spec.get("new_cluster").update(cloud_attributes)                    # Get the "new_cluster" parameters and update them with the cloud specific attributes

    job = workspaces_api.call("POST", "/api/2.1/jobs/create", job_spec)             # Create the job
    job_id = job.get("job_id")                                                      # Get the job id from the job
    run = workspaces_api.call("POST", "/api/2.1/jobs/run-now", {"job_id": job_id})  # Start the job
    run_id = run.get("run_id")

    ###############################################################################################
    # Wait for the "DBAcademy Workspace-Setup" job to finish execution: ~30 minutes
    ###############################################################################################
    print(f"""Waiting for the job "{config.job_name}" to complete in {config.workspace_name}.""", end="...")
    start = time.time()

    while True:
        response = workspaces_api.call("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        life_cycle_state = response.get("state").get("life_cycle_state")
        if life_cycle_state not in ["PENDING", "RUNNING", "TERMINATING"]:
            job_state = response.get("state", {})
            job_result = job_state.get("result_state")
            job_message = job_state.get("state_message", "Unknown")
            break
        time.sleep(5)  # Slow it down a bit so that we don't hammer the REST endpoints.
        print(".", end="")

    print(f"{int(time.time() - start)} seconds")

    if job_result != "SUCCESS":
        raise Exception(f"""Expected the final state of the job "{config.job_name}" to be "SUCCESS", """
                        f"""found "{job_result}" in {config.workspace_name} | {job_message}""")

    print(f"Provisioning of the workspace {config.workspace_name} completed succesfully.")


def remove_workspace(config: WorkspaceConfig):
    if config.cloud == "AWS":
        domain_suffix = ".cloud.databricks.com"
    elif config.cloud == "GCP":
        domain_suffix = ".gcp.databricks.com"
    elif config.cloud == "MSA":
        domain_suffix = ".azuredatabricks.net"
    else:
        raise Exception(f"Unsupported cloud, found {config.cloud}")

    accounts_api = SimpleRestClient(username=config.account_username, password=config.account_password, url=f"https://accounts.cloud.databricks.com")

    ###############################################################################################
    # Remove the account-level instructor's group
    ###############################################################################################
    print("Remove the instructors group")
    response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/scim/v2/Groups", count=1000)
    acct_instructors_group = next((u for u in response.get("Resources", list()) if config.instructors_group_name == u.get("displayName")), None)

    if acct_instructors_group is not None:
        group_id = acct_instructors_group.get("id")
        accounts_api.call("DELETE", f"/api/2.0/accounts/{config.account_id}/scim/v2/Groups/{group_id}")

    ###############################################################################################
    # Look up the workspace
    ###############################################################################################
    print(f"Looking for the workspace {config.workspace_name}.")
    response = accounts_api.call("GET", f"/api/2.0/accounts/{config.account_id}/workspaces")
    workspace = next((w for w in response if w.get("workspace_name") == config.workspace_name), None)

    if workspace is None:
        print(f"The workspace {config.workspace_name} doesn't exist.")
    else:
        workspace_id = workspace.get("workspace_id")
        deployment_name = workspace.get("deployment_name")

        workspace_domain_name = deployment_name + domain_suffix
        workspaces_api = SimpleRestClient(username=config.account_username, password=config.account_password, url=f"https://{workspace_domain_name}")

        print(f"Looking up the workspace {config.workspace_name}.")
        metastore = workspaces_api.call("GET", f"/api/2.1/unity-catalog/current-metastore-assignment", _expected=(200, 404))

        if metastore is not None:
            print(f"Un-assigning the metastore for {config.workspace_name}.")
            metastore_id = metastore.get("metastore_id")
            workspaces_api.call("DELETE", f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", {"metastore_id": metastore_id})

        print(f"Deleting the metastore {config.workspace_name}.")
        response = workspaces_api.call("GET", "/api/2.1/unity-catalog/metastores")
        metastore_id = next((m.get("metastore_id") for m in response.get("metastores") if m.get("name") == config.workspace_name), None)

        if metastore_id is not None:
            workspaces_api.call("DELETE", f"/api/2.1/unity-catalog/metastores/{metastore_id}", {"force": True})

        print(f"""Removing the workspace {config.workspace_name}.""")
        accounts_api.call("DELETE", f"/api/2.0/accounts/{config.account_id}/workspaces/{workspace_id}")


###################################################################################################
# Script Execution
###################################################################################################
def generate_usernames(first: int, last: int, pattern: str = "class+{num:03d}@databricks.com") -> list[str]:
    """
    Example factory method that follows Databricks Edu's naming convention and is based on a simple range of numbers.
    """
    return [pattern.format(num=i) for i in range(first, last + 1)]


# API key for REST calls to Databricks Edu's Content Distribution System, required for installing courseware.
cds_api_token = os.environ.get("WORKSPACE_SETUP_CDS_API_TOKEN")                    # The vendor specific API token to Databricks Edu's Content Delivery System
cds_url = "https://dev.training.databricks.com/api/v1/courses/download.dbc"        # The base URL for vender-downloads from the CDS.

lab_id = 901                                                                       # Typically referenced in multiple fields
env_code = "CURR"                                                                  # Broken out so that we can test multiple environments only

workspace_config = WorkspaceConfig(
    account_id=os.environ.get(f"WORKSPACE_SETUP_{env_code}_ACCOUNT_ID"),           # Securing these in a vault is mandatory
    account_username=os.environ.get(f"WORKSPACE_SETUP_{env_code}_USERNAME"),       # Use of an environment variable is for
    account_password=os.environ.get(f"WORKSPACE_SETUP_{env_code}_PASSWORD"),       # demonstration purposes only.
    cloud="AWS",                                                                   # Parameterizes cloud-specific settings
    lab_id=lab_id,                                                                 # The id, event number, class number for the environment
    lab_description=f"Classroom {lab_id:03d}",                                     # A description of what the environment is being used for.
    workspace_name=f"classroom-{lab_id:03d}",                                      # The name of the workspace as provisioned in Databricks; TODO use a stable-hash?
    region="us-west-2",
    datasets=[                                                                     # Maps one-to-one with the course parameter in courseware_urls below. The one exception is the deprecated course DAWD v1.
        f"example-course",
        # ml-in-production",
        # data-engineering-with-databricks",
        # introduction-to-python-for-data-science-and-data-engineering",
    ],
    courseware_urls=[                                                              # Courseware installs are done via the job "DBAcademy Workspace-Setup" job; TODO installs are not currently parallelized which can take a long time with the standard 250 users per workspace
        f"{cds_url}?course=example-course&version=vCURRENT&token={cds_api_token}",
        # f"{cds_url}?course=ml-in-production&version=v3.4.5&token={cds_api_token}",
        # f"{cds_url}?course=data-engineering-with-databricks&version=vCURRENT&token={cds_api_token}",
        # f"{cds_url}?course=introduction-to-python-for-data-science-and-data-engineering&version=v1.1.4&artifact=introduction-to-python-for-data-science-and-data-engineering.dbc&token={cds_api_token}",
    ],
    # Databricks Edu pattern to use a generator function above to create an array of users
    instructors=generate_usernames(0, 0),                                          # Account level group, assigned as Metastore Admins and also workspace admin
    instructors_group_name="instructors",  # f"instructors-{name}",                # The name of the account-level instructor's group; # TODO Doug, this naming convention doesn't make sense to me - JDP
    users=generate_usernames(1, 5),                                                # Databricks Edu convention is 1-250 inclusive,
    default_dbr="11.3.x-cpu-ml-scala2.12",                                         # ML runtimes are the only runtimes supported by all courses
    default_node_type_id="i3.xlarge",                                              # Supports Photon & Delta optimizations
    credentials_name="default",                                                    # Databricks Edu's naming convention
    storage_configuration="us-west-2",                                             # Not the region, just named after the region
    uc_storage_root=f"s3://unity-catalogs-us-west-2/",                             # Region included in name for convention
    uc_aws_iam_role_arn="arn:aws:iam::981174701421:role/Unity-Catalog-Role",       # TODO fix document that we need one or the other, possibly cloud-specific dictionary
    uc_msa_access_connector_id=None,                                               # None because we are using AWS here
    entitlements={                                                                 # Entitlements added to the "users" group
        "allow-cluster-create": False,                                             # Remove to enforce policy
        # "allow-instance-pool-create": False,                                     # TODO Can’t be granted to individual users or service principals nor removed from workspace admins. See https://docs.databricks.com/administration-guide/users-groups/service-principals.html#manage-entitlements-for-a-service-principal
        "databricks-sql-access": True,                                             # Default for new workspace but specified to guarantee it's set
        "workspace-access": True,                                                  # Default for new workspace but specified to guarantee it's set
    },
    job_name="DBAcademy Workspace-Setup",                                          # The name of the Workspace-Setup job; exact name required.
)

create_workspace(workspace_config)
# remove_workspace(workspace_config)
