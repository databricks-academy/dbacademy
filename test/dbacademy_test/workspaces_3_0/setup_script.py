from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.event_config_class import EventConfig
from dbacademy.workspaces_3_0.storage_config_class import StorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup

if __name__ == '__main__':
    event_config = EventConfig(event_id=9999, max_participants=80, description="Fun and games!")

    storage_config = StorageConfig(credentials_name="default", storage_configuration="us-west-2")

    workspace_config = WorkspaceConfig(max_user_count=20,
                                       courses="example-course",
                                       datasets="example-course",
                                       default_dbr="10.4.x-scala2.12",
                                       default_node_type_id="i3.xlarge",
                                       dbc_urls="https://labs.training.databricks.com/api/courses?course=example-course&version=vLATEST&artifact=lessons.dbc&token=abcd")

    account = AccountConfig.from_env(region="us-west-2",
                                     event_config=event_config,
                                     storage_config=storage_config,
                                     workspace_config=workspace_config,
                                     ignored=[1])

    workspace_setup = WorkspaceSetup(account)
    workspace_setup.create_workspaces()
    # workspace_setup.delete_workspaces()
