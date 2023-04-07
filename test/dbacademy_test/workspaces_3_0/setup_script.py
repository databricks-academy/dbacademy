import os
from typing import Optional
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


def advertise(name: str, items: list):
    if len(items) > 0:
        print(f"{name}: ")
        for item in items:
            print(f"    {item}")


print("-"*100)
env_code = read_str("""Please select an environment such as "PROSVC" or "CURR".""", "CURR").upper()

# workspace_numbers = list({99})
workspace_numbers = list(range(100, 200))

print("1) Create workspaces.")
print("2) Delete a workspace.")
print("3) Remove a metastore.")
action = read_int("Select an action", 0)
if action not in [1, 2, 3]:
    print(f"""\n{"-" * 100}\nAborting script; Invalid action selection""")
    exit(1)

# If we are not creating, then we don't care.
max_participants = 1 if action != 1 else read_int("Max Participants Per Workspace", 25)

print()
print("-"*100)
print(f"""Env ID (PROSVC, CURR, etc):  {env_code}""")
print(f"""Max Users Per Workspace:     {max_participants}""")
print(f"""Workspaces:                  {", ".join([str(n) for n in workspace_numbers])}""")

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
                                                # "allow-instance-pool-create": False,  # TODO Can’t be granted to individual users or service principals nor removed from workspace admins. See https://docs.databricks.com/administration-guide/users-groups/service-principals.html#manage-entitlements-for-a-service-principal
                                                "databricks-sql-access": True,
                                                "workspace-access": True,
                                            },
                                            # Cloud Labs needs are different here because they have a list of users, not a range of users.
                                            workspace_name_pattern="es-645542-{workspace_number}",  # workspace_number defaults to 3 digits, zero prefixed
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
                                     146
                                     # "classroom-005-9j1zg",
                                     # "classroom-007-t8yxo",
                                 ])

workspace_setup = WorkspaceSetup(account)
confirmations = ["y", "yes", "1"]

if action == 3 and read_str("""\nPlease confirm you wish to REMOVE these metastores""", "y/n").lower() in confirmations:
    for workspace_number in workspace_numbers:
        workspace_setup.remove_metastore(workspace_number)

if action == 2 and read_str("""\nPlease confirm you wish to DELETE these workspaces""", "y/n").lower() in confirmations:
    workspace_setup.delete_workspaces()

elif action == 1:
    if read_str("""\nPlease confirm you wish to create these workspaces""", "y/n").lower() in confirmations:
        advertise("Courses", workspace_config_template.course_definitions)
        advertise("Datasets", workspace_config_template.datasets)
        advertise("Ignored", account.ignored_workspaces)

        FALSE = False  # easier for my eyes to recognize
        workspace_setup.create_workspaces(run_workspace_setup=FALSE,   # <<< TRUE
                                          remove_metastore=FALSE,      # This should always be False
                                          remove_users=FALSE,          # This should always be False
                                          uninstall_courseware=FALSE)  # This should always be False
