from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.event_config_class import EventConfig
from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup

# if __name__ == '__main__':
event_config = EventConfig(event_id=9997,
                           max_participants=20,
                           description="Fun and games!")

# TODO There is a bug that doesn't allow us to use the instructors group
storage_config = UcStorageConfig(storage_root="s3://unity-catalogs-us-west-2/",
                                 storage_root_credential_id="be2549d1-3f5b-40db-900d-1b0fcdb419ee",
                                 region="us-west-2",
                                 owner="class+000@databricks.com")

workspace_config = WorkspaceConfig(max_user_count=20,
                                   courses="example-course",
                                   datasets="example-course",
                                   default_dbr="10.4.x-scala2.12",
                                   default_node_type_id="i3.xlarge",
                                   dbc_urls="https://labs.training.databricks.com/api/courses?course=example-course&version=vLATEST&artifact=lessons.dbc&token=abcd",
                                   credentials_name="default",
                                   storage_configuration="us-west-2",  # Not region, just named after the region.
                                   username_pattern="class+{student_number}@databricks.com",
                                   workspace_name_pattern="classroom-{event_id}-{workspace_number}")

account = AccountConfig.from_env(first_workspace_number=1,  # 280
                                 region="us-west-2",
                                 event_config=event_config,
                                 uc_storage_config=storage_config,
                                 workspace_config=workspace_config,
                                 ignored_workspaces=["classroom-9999-001", "classroom-9999-002"])

workspace_setup = WorkspaceSetup(account)
# workspace_setup.create_workspaces()
workspace_setup.delete_workspaces()
