import os
from typing import Optional
from datetime import datetime
from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup


def read_int(prompt: str, default_value: int) -> int:
    value = input(f"{prompt} ({default_value}):")
    return default_value if value.strip() == "" else int(value)


def read_str(prompt: str, default_value: Optional[str] = "") -> str:
    value = input(f"{prompt} ({default_value}):")
    return default_value if value.strip() == "" else value


print("-"*100)
print(f"Starting script at {datetime.now()}")

env_id = read_str("""Please select an environment such as "PROSVC" or "CURR".""", "PROSVC").upper()
first_workspace_number = read_int("First Workspace Number", 325)
max_participants = read_int("Max Participants Per Workspace", 250)
total_workspaces = read_int("Total Workspaces", 50)

print("1) Create workspaces.")
print("2) Delete a workspace.")
print("3) Remove a metastore.")
action = read_int("Select an action", 0)
if action not in [1, 2, 3]:
    print(f"""\n{"-" * 100}\nAborting script; Invalid action selection""")
    exit(1)

print()
print("-"*100)
print(f"Env ID (PROSVC, CURR, etc):  {env_id}")
print(f"First Workspace Number:      {first_workspace_number}")
print(f"Max Users Per Workspace:     {max_participants}")
print(f"Total Workspaces:            {total_workspaces}")

# TODO There is a bug that doesn't allow us to use the "instructors" group for the owner
owner = "instructors"
storage_config = UcStorageConfig(storage_root="s3://unity-catalogs-us-west-2/",
                                 storage_root_credential_id="be2549d1-3f5b-40db-900d-1b0fcdb419ee",  # ARN
                                 region="us-west-2",
                                 owner=owner)

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
                                                "allow-cluster-create": False,        # False to force policy
                                                "allow-instance-pool-create": False,  # False to force policy
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

account = AccountConfig.from_env(account_id_env_name=f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID",        # Loaded from the environment to make
                                 account_password_env_name=f"WORKSPACE_SETUP_{env_id}_PASSWORD",    # configuration easily swapped between systems
                                 account_username_env_name=f"WORKSPACE_SETUP_{env_id}_USERNAME",    # Refactor to use .databricks/profile-name/etc.
                                 first_workspace_number=first_workspace_number,
                                 total_workspaces=total_workspaces,
                                 region="us-west-2",
                                 uc_storage_config=storage_config,
                                 workspace_config_template=workspace_config_template,
                                 ignored_workspaces=[  # Either the number or name (not domain) to skip
                                     # "classroom-005-9j1zg",
                                     # "classroom-007-t8yxo",
                                     # 9, 11, 24
                                 ])

# Retries introduced to fix a throttling issues created by Databricks Eng.
workspace_setup = WorkspaceSetup(account, max_retries=0)

if action == 3:
    workspace_number = read_int("Enter the workspace number of the metastore to remove", 0)
    while workspace_number > 0:
        workspace_setup.remove_metastore(workspace_number)
        workspace_number = read_int("Enter the workspace number of the metastore to remove", 0)

if action == 2:
    delete_workspace_number = read_int("Enter the workspace number to delete", 0)
    while delete_workspace_number > 0:
        workspace_setup.delete_workspace(delete_workspace_number)
        delete_workspace_number = read_int("Enter the workspace number to delete", 0)

elif action == 1 and read_str("""Please "confirm" you wish to create these workspaces""", "no").lower() in ["c", "confirm", "confirmed", "y", "yes", "1"]:
    step = total_workspaces  # Process all at once

    for i in range(first_workspace_number, first_workspace_number+total_workspaces, step):
        workspace_setup.create_workspaces(create_users=True,
                                          create_groups=True,
                                          remove_metastore=False,      # This really should always be False
                                          create_metastore=True,
                                          enable_features=True,
                                          run_workspace_setup=True,   # <<<
                                          uninstall_courseware=False,  # This really should always be False
                                          install_courseware=True,
                                          workspace_numbers=range(i, i+step))

    print(f"Completed update at {datetime.now()}")
