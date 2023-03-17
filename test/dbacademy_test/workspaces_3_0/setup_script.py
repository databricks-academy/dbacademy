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

max_users_per_workspace = 2
max_event_participants = 2 * max_users_per_workspace
total_workspaces = math.ceil(max_event_participants / max_users_per_workspace)
first_workspace_number = 1

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
                                   course_definitions=[
                                       "course=welcome",
                                       "course=example-course&version=v1.1.8",
                                       "course=template-course&version=v1.0.0&artifact=template-course.dbc",
                                       "https://labs.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production"],
                                   cds_api_token=os.environ.get("WORKSPACE_SETUP_CDS_API_TOKEN"),
                                   datasets=None,  # defined via course_definitions
                                   default_dbr="11.3.x-cpu-ml-scala2.12",
                                   default_node_type_id="i3.xlarge",
                                   credentials_name="default",
                                   storage_configuration="us-west-2",  # Not region, just named after the region.
                                   username_pattern="class+{student_number}@databricks.com",
                                   entitlements={
                                       "allow-cluster-create": True,
                                       "databricks-sql-access": True,
                                       "workspace-access": True,
                                       "allow-instance-pool-create": True,
                                   },
                                   workspace_name_pattern="classroom-{workspace_number}",
                                   groups={
                                       "analysts": ["class+analyst@databricks.com"],
                                       "instructors": [0],
                                   })

account = AccountConfig.from_env(qualifier="CURR",  # PROSVC, CURR
                                 first_workspace_number=first_workspace_number,
                                 region="us-west-2",
                                 event_config=event_config,
                                 uc_storage_config=storage_config,
                                 workspace_config=workspace_config,
                                 ignored_workspaces=[])

workspace_setup = WorkspaceSetup(account, max_retries=100)

step = 5  # total_workspaces  # We can only handle 5 at a time.
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
