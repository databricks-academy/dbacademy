import math
import os
from datetime import datetime
from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.event_config_class import EventConfig
from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup

print("-"*100)
print(f"Starting script at {datetime.now()}")

first_workspace_number = 1

max_users_per_workspace = 250
# max_users_per_workspace = 5

# max_event_participants = 1000
max_event_participants = 30 * max_users_per_workspace  # Computed for testing

total_workspaces = math.ceil(max_event_participants / max_users_per_workspace)

event_config = EventConfig(event_id=0,
                           max_participants=max_event_participants,
                           description=None)

# TODO There is a bug that doesn't allow us to use the "instructors" group for the owner
owner = "instructors"
storage_config = UcStorageConfig(storage_root="s3://unity-catalogs-us-west-2/",
                                 storage_root_credential_id="be2549d1-3f5b-40db-900d-1b0fcdb419ee",
                                 region="us-west-2",
                                 owner=owner)

workspace_config = WorkspaceConfig(max_users=max_users_per_workspace,
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
                                   entitlements={
                                       "allow-cluster-create": True,
                                       "databricks-sql-access": True,
                                       "workspace-access": True,
                                       "allow-instance-pool-create": True,
                                   },
                                   workspace_name_pattern="classroom-{workspace_number}",     # workspace_number defaults to 3 digits, zero prefixed
                                   groups={
                                       "admins": [0],                                 # User class+000@ should always be an admin
                                       "instructors": [0],                            # User class+000@ should always be an instructor
                                       "analysts": ["class+analyst@databricks.com"],  # "Special" config for DEWD/ADEWD v3
                                   })

account = AccountConfig.from_env(qualifier="CURR",  # PROSVC, CURR; see code for expected format, or exception when not specified.
                                 first_workspace_number=first_workspace_number,
                                 region="us-west-2",
                                 event_config=event_config,
                                 uc_storage_config=storage_config,
                                 workspace_config=workspace_config,
                                 ignored_workspaces=[  # Either the number or name (not domain) to skip
                                     # "classroom-005-9j1zg",
                                     # "classroom-007-t8yxo",
                                     # 9, 11, 24
                                 ])

# Retries introduced to fix a throttling issues created by Databricks Eng.
workspace_setup = WorkspaceSetup(account, max_retries=100)

# step = 5  # We can only handle 5 at a time due to the throttling issues.
step = total_workspaces  # Process all at once

for i in range(first_workspace_number, first_workspace_number+total_workspaces, step):
    workspace_setup.create_workspaces(create_users=True,
                                      create_groups=True,
                                      create_metastore=True,
                                      enable_features=True,
                                      run_workspace_setup=True,
                                      uninstall_courseware=False,
                                      install_courseware=True,
                                      workspace_numbers=range(i, i+step))

# workspace_setup.remove_workspace_setup_jobs(remove_bootstrap_job=True, remove_final_job=True)
# workspace_setup.delete_workspaces()
# workspace_setup.remove_metastores()

print(f"Completed update at {datetime.now()}")
