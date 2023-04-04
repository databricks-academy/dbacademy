import os
from typing import Optional, List
from datetime import datetime
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

workspace_api = workspace_setup.accounts_api.workspaces.create(workspace_name=workspace_config.name,
                                                               deployment_name=workspace_config.name,
                                                               region=workspace_setup.account_config.region,
                                                               credentials_name=workspace_setup.account_config.workspace_config_template.credentials_name,
                                                               storage_configuration_name=workspace_setup.account_config.workspace_config_template.storage_configuration)
classroom = Classroom(num_students=len(workspace_config.usernames),
                      username_pattern=workspace_config.username_pattern,
                      databricks_api=workspace_api)

workspace = WorkspaceTrio(workspace_config, workspace_api, classroom)

# Workspaces were all created synchronously, blocking here until we confirm that all workspaces are created.
workspace.workspace_api.wait_until_ready()

# Configuring the entitlements for the group "users"
workspace_setup.update_entitlements(workspace)

# # Configuring users in each workspace.
# workspace_setup.create_users(workspace)

# Configure workspace-level groups
workspace_setup.create_group(workspace)

# Create the metastore for the workspace
workspace_setup.create_metastore(workspace)

# Enable workspace features
workspace_setup.enable_features(workspace)

# # Install courseware for all users in the workspace
# workspace_setup.install_courseware(workspac)

# Run the Universal Workspace-Setup
workspace_setup.run_workspace_setup(workspace)


for error in workspace_setup.errors:
    print(error)
    print("-" * 100)

print(f"Completed update at {datetime.now()}")
