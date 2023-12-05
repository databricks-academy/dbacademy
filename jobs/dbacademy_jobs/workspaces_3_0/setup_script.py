from typing import List

from dbacademy.jobs.pools.account_config_class import AccountConfig
from dbacademy.jobs.pools.uc_storage_config_class import UcStorageConfig
from dbacademy.jobs.pools.workspace_config_classe import WorkspaceConfig
from dbacademy.jobs.pools.workspace_setup_class import WorkspaceSetup, TRAINING_OPERATIONS_BASE, ALL_WORKSPACES_TABLE
from dbacademy.jobs.pools import read_str, read_int, advertise
from dbacademy.clients import airtable


CONFIRMATIONS = ["y", "yes", "1", "t", "true"]

env_codes = ["PROSVC", "CURR"]
env_code = read_str(f"""Please select an environment {env_codes}""", env_codes[0]).upper()

if env_code is None:
    print(f"""\n{"-" * 100}\nAborting script; no environment specified.""")
    exit(1)


def configure_workspace_setup(*,
                              _run_ws: bool,
                              _validate_ws: bool,
                              _max_participants: int = 0,
                              _env_code: str,
                              _workspace_numbers: List[int],
                              _ignored_workspaces: List[int] = None) -> (WorkspaceConfig, WorkspaceSetup):

    _ignored_workspaces = _ignored_workspaces or list()

    uc_storage_config = UcStorageConfig(storage_root="s3://unity-catalogs-us-west-2/",
                                        storage_root_credential_id="be2549d1-3f5b-40db-900d-1b0fcdb419ee",  # ARN
                                        region="us-west-2",
                                        meta_store_owner="instructors",
                                        aws_iam_role_arn="arn:aws:iam::981174701421:role/Unity-Catalog-Role")

    wct = WorkspaceConfig(max_participants=_max_participants,
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

    account = AccountConfig.from_env(account_id_env_name=f"WORKSPACE_SETUP_{_env_code}_ACCOUNT_ID",        # Loaded from the environment to make
                                     account_password_env_name=f"WORKSPACE_SETUP_{_env_code}_PASSWORD",    # configuration easily swapped between systems
                                     account_username_env_name=f"WORKSPACE_SETUP_{_env_code}_USERNAME",    # Refactor to use .databricks/profile-name/etc.
                                     workspace_numbers=_workspace_numbers,                                 # Enumeration of all the workspaces to create
                                     region="us-west-2",
                                     uc_storage_config=uc_storage_config,
                                     workspace_config_template=wct,
                                     ignored_workspaces=_ignored_workspaces)                                # Either the number or name (not domain) to skip

    return wct, WorkspaceSetup(account, run_ws=_run_ws, validate_ws=_validate_ws)


if env_code == "CURR":
    max_participants_default = 5
elif env_code == "PROSVC":
    max_participants_default = 250
else:
    raise Exception(f"""Unsupported environment, expected one of {env_codes}, found {env_code}.""")

max_participants = read_int("\nMax Participants per Workspace", max_participants_default)

airtable_client = airtable.from_environment(base_id=TRAINING_OPERATIONS_BASE)
workspaces_table_api = airtable_client.table(ALL_WORKSPACES_TABLE)

print("Checking for duplicates", end="... ")
duplicates = workspaces_table_api.list_duplicates("AWS Workspace URL")
duplicates.remove("https://training-classroom-326-6xu72.cloud.databricks.com")
duplicates.remove("https://training-classroom-325-oovzv.cloud.databricks.com")
duplicates.remove("https://training-classroom-327-cpnuw.cloud.databricks.com")
duplicates.remove("https://training-classroom-676-znuqc.cloud.databricks.com")
duplicates.remove("https://training-classroom-679-y5xt8.cloud.databricks.com")
duplicates.remove("https://training-classroom-000-857q5.cloud.databricks.com")
duplicates.remove("https://training-classroom-313-9vcdv.cloud.databricks.com")
duplicates.remove("https://training-classroom-680-xt6rc.cloud.databricks.com")
duplicates.remove("https://training-classroom-639-8eqi6.cloud.databricks.com")

if len(duplicates) == 0:
    print("None found.")
else:
    print(f"Found {len(duplicates)}.")
    for duplicate in duplicates:
        print(f" * {duplicate}")

    print(f"\n\nExisting script.")
    exit(-1)

print("Looking up the next workspace number", end="... ")
all_workspaces_records = workspaces_table_api.query()
all_workspaces = [w.get("fields").get("AWS Workspace URL") for w in all_workspaces_records]

last_workspace = 0
for workspace in all_workspaces:
    if workspace.startswith("https://training-classroom-"):
        workspace_number: str = workspace.split("-")[2]
        if workspace_number.isnumeric():
            last_workspace = max(last_workspace, int(workspace_number))
print(f"#{last_workspace+1}")

wsn_a = read_int("\nPlease enter the first workspace number to create", last_workspace+1)
if wsn_a < 0:
    print(f"""\n{"-" * 100}\nAborting script; no workspace number""")
    exit(1)

wsn_b = read_str("Please enter the last workspace number to create", "+25")

if str(wsn_a) == wsn_b:
    workspace_numbers = [wsn_a]
elif wsn_b.startswith("+"):
    workspace_numbers = list(range(wsn_a, wsn_a+int(wsn_b)))
else:
    workspace_numbers = list(range(wsn_a, int(wsn_b)+1))

deleting_wsj = True
# deleting_wsj = read_str("""\nDelete existing Workspace-Setup jobs""", "no").lower() in CONFIRMATIONS

ignored = []
workspace_config_template, workspace_setup = configure_workspace_setup(_env_code=env_code,
                                                                       _run_ws=True,
                                                                       _validate_ws=True,
                                                                       _ignored_workspaces=ignored,
                                                                       _max_participants=max_participants,
                                                                       _workspace_numbers=workspace_numbers)

workspaces = workspace_setup.accounts_api.api("GET", f"/workspaces")

print()
print("-"*100)
print(f"""Number of Used Workspaces:  {len(workspaces)}""")
print(f"""Number currently available: {200-len(workspaces)}""")
print(f"""Final number available:     {200-len(workspaces)-len(workspace_numbers)}""")
print()
print(f"""Env ID (PROSVC, CURR, etc): {env_code}""")
print(f"""Max Users Per Workspace:    {max_participants}""")
print(f"""Workspaces:                 {", ".join([str(n) for n in workspace_numbers])}""")

print()
advertise("Courses", workspace_config_template.course_definitions, 6, 15)
advertise("Datasets", workspace_config_template.datasets, 5, 15)
advertise("Skipping", ignored, 5, 15)

if len(workspaces) + len(workspace_numbers) >= 200:
    print("\nAborting, not enough available workspaces.")

if read_str("""Please confirm you wish to create these workspaces""", "no").lower() in CONFIRMATIONS:

    FALSE = False  # easier for my eyes to recognize
    workspace_setup.create_workspaces(remove_metastore=FALSE,      # This should always be False
                                      remove_users=FALSE,          # This should always be False
                                      uninstall_courseware=FALSE)  # This should always be False
