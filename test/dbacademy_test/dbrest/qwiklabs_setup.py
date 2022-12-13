#!python3
"""
Sample script to create a Databricks workspace.
Before running this script install this library:
  pip install git+https://github.com/databricks-academy/dbacademy.git
"""

import re
import time
import urllib.parse

from dbacademy.dougrest.accounts import AccountsApi
from dbacademy.dougrest.accounts.workspaces import Workspace

# TODO: Enter your username and password below.
# It's best to read these from a file, environment variable, or secrets store.
account_id = "b338b505-b6ba-49e5-b6d9-c7174d67017a"
username = "class+qwiklabs@databricks.com"
password = "REDACTED"
databricks = AccountsApi(account_id, username, password)

# TODO: Configure these with the IAM role and s3 bucket you created.  The region should match the s3 bucket.
region = "us-west-2"
iam_role = "arn:aws:iam::981174701421:role/Workspaces-Role-Qwiklabs"
s3_bucket = "workspaces-qwiklabs-us-west-2"

# Metadata from Qwiklabs for resource tagging and cost tracking.
lab_id = "lab-000001"
lab_description = "Qwiklabs Testing"

# The name of the workspace you want to create
workspace_name = lab_id

# Normally I'd set if_exists to "error" to error out if a resource already exists.  But here I set it to "overwrite"
# in order to allow you to run this script multiple times without issue.  You can also set it to "ignore" to avoid
# rerunning operations that have already been run.
if_exists = "overwrite"


# Step 1: Create the workspace.

if if_exists == "overwrite":
    workspace = databricks.workspaces.get_by_name(workspace_name, if_not_exists="ignore")
    if workspace:
        databricks.workspaces.delete_by_name(workspace_name)
        workspace.wait_until_gone()

credentials_config = databricks.credentials.create_by_example(
    {
        "credentials_name": workspace_name,
        "aws_credentials": {
            "sts_role": {
                "role_arn": iam_role
            }
        }
    },
    if_exists=if_exists)

storage_config = databricks.storage.create_by_example(
    {
        "storage_configuration_name": workspace_name,
        "root_bucket_info": {
            "bucket_name": s3_bucket
        }
    },
    if_exists=if_exists)

print("Creating workspace...")
workspace: Workspace = databricks.workspaces.create(workspace_name, deployment_name=None,
                                                    region=region,
                                                    credentials=credentials_config,
                                                    storage_configuration=storage_config,
                                                    if_exists=if_exists)
workspace_url = f"https://{workspace['deployment_name']}.cloud.databricks.com/"
workspace.wait_until_ready()
print(f"Workspace Created: {workspace_url}")

# Step 2: Add end-user

end_user = "class+001@databricks.com"
workspace.users.create(end_user, allow_cluster_create=False, if_exists=if_exists)
print(f"User added: {end_user} / ***REMOVED***")


# Step 3: Run our Workspace-Setup script to configure the workspace to our specifications.

job_name = "DBAcademy Workspace-Setup"
cluster_type = "i3.xlarge"
spark_version = "11.3.x-cpu-ml-scala2.12"

job_spec = {
    "name": job_name,
    "timeout_seconds": 7200,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "Workspace-Setup",
            "notebook_task": {
                "notebook_path": "Universal-Workspace-Setup",
                "base_parameters": {
                    "node_type_id": cluster_type,
                    "spark_version": spark_version,
                    "lab_id": lab_id,
                    "description": lab_description
                },
                "source": "GIT"
            },
            "job_cluster_key": "Workspace-Setup-Cluster",
            "timeout_seconds": 7200
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "Workspace-Setup-Cluster",
            "new_cluster": {
                "node_type_id": cluster_type,
                "spark_version": spark_version,
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode",
                    "dbacademy.library.version": "qwiklabs"  # For production, delete this line
                },
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
                "num_workers": 0
            }
        }
    ],
    "git_source": {
        "git_url": "https://github.com/databricks-academy/workspace-setup.git",
        "git_provider": "gitHub",
        "git_branch": "published"
    },
    "format": "MULTI_TASK"
}

job = workspace.jobs.create_multi_task_job(**job_spec, if_exists=if_exists)
run = None
if if_exists != "ignore":
    run = workspace.jobs.run(job)
else:
    runs = workspace.jobs.runs.list(job_id=job["job_id"])
    if runs:
        run = runs[-1]
    else:
        run = workspace.jobs.run(job)

print("Running Workspace-Setup job.  This may take up to 30 minutes.", end="")
while True:
    run = workspace.jobs.runs.get(run)
    status = run.get("state", {}).get("life_cycle_state")
    if status != "RUNNING":
        break
    print(".", end="")
    time.sleep(10)  # 10 seconds.
print()

result = run.get("state", {}).get("result_state", "Unknown")
if result != "SUCCESS":
    raise Exception(f"Workspace-Setup job error: {result} {status}")
else:
    print(f"Workspace-Setup job completed successfully.")


# Step 4: Create end-user cluster (not all courses will require this)

policy = workspace.clusters.policies.get_by_name("DBAcademy")
end_user_name = re.subn("@.*", "", end_user)[0]
cluster_spec = {
    "cluster_name": f"{end_user_name}'s DBAcademy Cluster",
    "policy_id": policy["policy_id"],
    "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
    "single_user_name": end_user,
    "runtime_engine": "STANDARD"
}
cluster = workspace.clusters.create(**cluster_spec, timeout_minutes=15, if_exists=if_exists)
workspace.permissions.clusters.update_user(cluster["cluster_id"], end_user, "CAN_MANAGE")
print(f"{end_user}'s cluster created and and left running for 15 minutes so you can test.")


# Step 5: Install sample course

print("Installing lessons...")
course_url = ("https://github.com/databricks-academy/data-engineering-with-databricks-english/releases/download/"
              "v2.3.11/data-engineering-with-databricks-v2.3.11-notebooks.dbc")
course_name = "data-engineering-with-databricks"
lesson_path = "02 - Delta Lake/DE 2.1 - Managing Delta Tables"
workspace_path = f"/Users/{end_user}/{course_name}"
workspace.workspace.import_from_url(course_url, workspace_path, if_exists=if_exists)
print(f"Lessons installed at: {workspace_path}")


# Step 6: Test the sample course

notebook_path = urllib.parse.quote(f"workspace{workspace_path}/{lesson_path}")
print()
print("To manually test this workspace, login as:")
print(f"    {end_user} / ***REMOVED***")
print("And run this notebook:")
print(f"    {workspace_url}#{notebook_path}")
