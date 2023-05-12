import os
from typing import Optional, Union, List
from dbacademy_test.workspaces_3_0.support.account_config_class import AccountConfig
from dbacademy_test.workspaces_3_0.support.uc_storage_config_class import UcStorageConfig
from dbacademy_test.workspaces_3_0.support.workspace_config_classe import WorkspaceConfig
from dbacademy_test.workspaces_3_0.support.workspace_setup_class import WorkspaceSetup


def read_int(prompt: str, default_value: int) -> int:
    value = input(f"{prompt} ({default_value}):")
    return default_value if value.strip() == "" else int(value)


def read_str(prompt: str, default_value: Optional[str] = "") -> str:
    value = input(f"{prompt} ({default_value}):")
    return default_value if len(value.strip()) == 0 else value


def advertise(name: str, items: Union[List, str, bool], padding: int, indent: int):

    if type(items) != list:
        print(f"{name}: {' ' * padding}{items}")

    elif len(items) == 1:
        print(f"{name}: {' ' * padding}{items[0]}")

    elif len(items) > 0:
        first = items.pop()
        print(f"{name}: {' ' * padding}{first}")
        for item in items:
            print(f"{' '*indent}{item}")


deleting_wsj = False
workspace_numbers = list()
CONFIRMATIONS = ["y", "yes", "1", "t", "true"]

print("-"*100)
env_codes = ["PROSVC", "CURR"]
env_code = read_str(f"""Please select an environment {env_codes}""", None)

if env_code is None:
    print(f"""\n{"-" * 100}\nAborting script; no environment specified.""")
    exit(1)
else:
    env_code = env_code.upper()

print("\n1) Create workspaces.")
print("2) Delete a workspace.")
print("3) Remove a metastore.")
action = read_int("Select an action", 0)
if action not in [1, 2, 3]:
    print(f"""\n{"-" * 100}\nAborting script; no command specified.""")
    exit(1)

# If we are not creating, then we don't care.
if env_code == "CURR":
    max_participants_default = 5
elif env_code == "PROSVC":
    max_participants_default = 250
else:
    raise Exception(f"""Unsupported environment, expected one of {env_codes}, found {env_code}.""")

max_participants = 1 if action != 1 else read_int("\nMax Participants per Workspace", max_participants_default)

if action == 1:
    wsn_a = read_int("\nPlease enter the first workspace number to create", 0)
    if wsn_a == 0:
        print(f"""\n{"-" * 100}\nAborting script; no workspace number""")
        exit(1)

    wsn_b = read_int("Please enter the last workspace number to create", wsn_a)

    if wsn_a == wsn_b:
        workspace_numbers = [wsn_a]
    else:
        workspace_numbers = list(range(wsn_a, wsn_b+1))

    deleting_wsj = read_str("""\nDelete existing Workspace-Setup jobs""", "no").lower() in CONFIRMATIONS

print()
print("-"*100)
print(f"""Env ID (PROSVC, CURR, etc): {env_code}""")
if action == 1:
    print(f"""Max Users Per Workspace:    {max_participants}""")
    print(f"""Workspaces:                 {", ".join([str(n) for n in workspace_numbers])}""")

uc_storage_config = UcStorageConfig(storage_root="s3://unity-catalogs-us-west-2/",
                                    storage_root_credential_id="be2549d1-3f5b-40db-900d-1b0fcdb419ee",  # ARN
                                    region="us-west-2",
                                    meta_store_owner="instructors",
                                    aws_iam_role_arn="arn:aws:iam::981174701421:role/Unity-Catalog-Role",
                                    msa_access_connector_id=None)

workspace_config_template = WorkspaceConfig(max_participants=max_participants,
                                            datasets=None,  # Appended to based on course_definitions; Only needs to be defined for DAWD v1
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
                                            entitlements={
                                                "allow-cluster-create": False,          # Removed to enforce policy
                                                "databricks-sql-access": True,
                                                "workspace-access": True,
                                            },
                                            # Cloud Labs needs are different here because they have a list of users, not a range of users.
                                            workspace_name_pattern="classroom-{workspace_number}",  # workspace_number defaults to 3 digits, zero prefixed
                                            workspace_group={                                       # User class+000@ should always be an admin
                                                "admins": [0],                                      # Defined as user "0" or by full username.
                                                # The instructor's group is defined at the account level to be
                                                # the metastore owner, no value to add at the workspace level.
                                                # "instructors": [0],                               # User class+000@ should always be an instructor
                                                # "analysts": ["class+analyst@databricks.com"],     # "Special" config for DEWD/ADEWD v3; to be refactored out.
                                            })

account = AccountConfig.from_env(account_id_env_name=f"WORKSPACE_SETUP_{env_code}_ACCOUNT_ID",        # Loaded from the environment to make
                                 account_password_env_name=f"WORKSPACE_SETUP_{env_code}_PASSWORD",    # configuration easily swapped between systems
                                 account_username_env_name=f"WORKSPACE_SETUP_{env_code}_USERNAME",    # Refactor to use .databricks/profile-name/etc.
                                 workspace_numbers=workspace_numbers,                                 # Enumeration of all the workspaces to create
                                 region="us-west-2",
                                 uc_storage_config=uc_storage_config,
                                 workspace_config_template=workspace_config_template,
                                 ignored_workspaces=[                                                 # Either the number or name (not domain) to skip
                                     529, 544, 550, 590, 598
                                     # "classroom-005-9j1zg",
                                     # "classroom-007-t8yxo",
                                 ])

workspace_setup = WorkspaceSetup(account)

if action == 3 and read_str("""\nPlease confirm you wish to REMOVE these metastores""", "no").lower() in CONFIRMATIONS:
    for workspace_number in workspace_numbers:
        workspace_setup.remove_metastore(workspace_number)

if action == 2:
    message = "\nEnter the workspace number to delete"
    workspace_number = read_int(message, 0)
    while workspace_number > 0:
        if read_str("Please confirm the deletion of this workspace", "no") in CONFIRMATIONS:
            workspace_setup.delete_workspace(workspace_number)
            workspace_number = read_int(message, 0)
        else:
            print(f"""Workspace #{workspace_number} will NOT be created.""")

elif action == 1:
    print()
    advertise("Courses", workspace_config_template.course_definitions, 6, 15)
    advertise("Datasets", workspace_config_template.datasets, 5, 15)
    advertise("Delete WS Job", deleting_wsj, 0, 15)
    advertise("Skipping", account.ignored_workspaces, 5, 15)

    if read_str("""\nPlease confirm you wish to create these workspaces""", "no").lower() in CONFIRMATIONS:

        FALSE = False  # easier for my eyes to recognize
        workspace_setup.create_workspaces(run_workspace_setup=True,   # <<< TRUE
                                          remove_metastore=FALSE,      # This should always be False
                                          remove_users=FALSE,          # This should always be False
                                          uninstall_courseware=FALSE,  # This should always be False
                                          delete_workspace_setup_job=deleting_wsj)
