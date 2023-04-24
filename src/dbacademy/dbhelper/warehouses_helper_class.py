from typing import Union, List, Optional


class WarehousesHelper:
    from dbacademy.dbrest import DBAcademyRestClient
    from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
    from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper

    WAREHOUSES_DEFAULT_NAME = "DBAcademy Warehouse"

    def __init__(self, workspace: WorkspaceHelper, da: DBAcademyHelper):
        self.da = da
        self.client = da.client
        self.workspace = workspace

    @property
    def autoscale_min(self):
        from dbacademy.dbhelper import DBAcademyHelper
        return 1 if DBAcademyHelper.is_smoke_test() else 2

    @property
    def autoscale_max(self):
        from dbacademy.dbhelper import DBAcademyHelper
        return 1 if DBAcademyHelper.is_smoke_test() else 20

    def delete_sql_warehouses_for(self, username):
        name = self.da.to_unique_name(username=username,
                                      course_code=self.da.course_config.course_code,
                                      lesson_name=self.da.lesson_config.name,
                                      sep="-")
        self.client.sql.warehouses.delete_by_name(name=name)

    def delete_sql_warehouses(self, configure_for: str):
        self.workspace.do_for_all_users(self.workspace.get_usernames(configure_for), lambda username: self.delete_sql_warehouses_for(username=username))

    @staticmethod
    def execute_statements(client: DBAcademyRestClient, warehouse_id: str, statements: List[str]) -> None:
        import json

        for statement in statements:
            results = client.sql.statements.execute(warehouse_id=warehouse_id,
                                                    catalog="main",
                                                    schema="default",
                                                    statement=statement)

            state = results.get("status", dict()).get("state")

            if state != "SUCCEEDED":
                print(f"""Expected state to be "SUCCEEDED", found "{state}".""")
                print(json.dumps(results, indent=4))

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_sql_warehouses(self, configure_for: str, auto_stop_mins=None, enable_serverless_compute=False):
        usernames = self.workspace.get_usernames(configure_for)
        self.workspace.do_for_all_users(usernames, lambda username: self.create_sql_warehouse_for(username=username,
                                                                                                  auto_stop_mins=auto_stop_mins,
                                                                                                  enable_serverless_compute=enable_serverless_compute))

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_sql_warehouse_for(self, username, auto_stop_mins=None, enable_serverless_compute=True):
        from dbacademy import dbgems
        from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper

        return WarehousesHelper.create_sql_warehouse(client=self.client,
                                                     name=self.da.to_unique_name(username=username,
                                                                                 course_code=self.da.course_config.course_code,
                                                                                 lesson_name=self.da.lesson_config.name,
                                                                                 sep="-"),
                                                     for_user=username,
                                                     auto_stop_mins=auto_stop_mins,
                                                     min_num_clusters=1,
                                                     max_num_clusters=1,
                                                     enable_serverless_compute=enable_serverless_compute,
                                                     lab_id=WorkspaceHelper.get_lab_id(),
                                                     workspace_description=WorkspaceHelper.get_workspace_description(),
                                                     workspace_name=WorkspaceHelper.get_workspace_name(),
                                                     org_id=dbgems.get_org_id())

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_shared_sql_warehouse(self, name: str, auto_stop_mins=None, enable_serverless_compute=True):
        from dbacademy import dbgems
        from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper

        return WarehousesHelper.create_sql_warehouse(client=self.client,
                                                     name=name,
                                                     for_user=None,
                                                     auto_stop_mins=auto_stop_mins,
                                                     min_num_clusters=self.autoscale_min,
                                                     max_num_clusters=self.autoscale_max,
                                                     enable_serverless_compute=enable_serverless_compute,
                                                     lab_id=WorkspaceHelper.get_lab_id(),
                                                     workspace_description=WorkspaceHelper.get_workspace_description(),
                                                     workspace_name=WorkspaceHelper.get_workspace_name(),
                                                     org_id=dbgems.get_org_id())

    @staticmethod
    def create_sql_warehouse(*, client: DBAcademyRestClient, name: str, auto_stop_mins: Optional[int],
                             min_num_clusters, max_num_clusters, enable_serverless_compute: bool, lab_id: str = None,
                             workspace_description: str = None, workspace_name: str = None,
                             org_id: str = None, for_user: Union[str, None] = None):
        from dbacademy import dbgems
        from dbacademy import common
        from dbacademy.rest.common import DatabricksApiException
        from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
        from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper
        from dbacademy.dbrest.sql.endpoints import RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

        lab_id = lab_id or WorkspaceHelper.get_lab_id()
        workspace_description = workspace_description or WorkspaceHelper.get_workspace_description()
        workspace_name = workspace_name or WorkspaceHelper.get_workspace_name()
        org_id = org_id or dbgems.get_org_id()

        def do_create(enable_serverless: bool):
            if auto_stop_mins is None:
                if enable_serverless_compute:
                    auto_stop_mins_inner = 20
                else:
                    auto_stop_mins_inner = 120
            else:
                auto_stop_mins_inner = auto_stop_mins

            return client.sql.warehouses.create_or_update(
                name=name,
                cluster_size=CLUSTER_SIZE_2X_SMALL,
                enable_serverless_compute=enable_serverless,
                min_num_clusters=min_num_clusters,
                max_num_clusters=max_num_clusters,
                auto_stop_mins=auto_stop_mins_inner,
                enable_photon=True,
                spot_instance_policy=RELIABILITY_OPTIMIZED,
                channel=CHANNEL_NAME_CURRENT,
                tags={
                    f"dbacademy.{WorkspaceHelper.PARAM_LAB_ID}": common.clean_string(lab_id),
                    f"dbacademy.{WorkspaceHelper.PARAM_DESCRIPTION}": common.clean_string(workspace_description),
                    f"dbacademy.workspace": common.clean_string(workspace_name),
                    f"dbacademy.org_id": common.clean_string(org_id),
                    f"dbacademy.source": common.clean_string("Smoke-Test" if DBAcademyHelper.is_smoke_test() else lab_id),
                })
        if enable_serverless_compute:
            try:
                warehouse = do_create(enable_serverless=True)
            except DatabricksApiException as e:
                if e.http_code == 400 and "Serverless Compute" in e.message:
                    warehouse = do_create(enable_serverless=False)
                else:
                    raise e
        else:
            warehouse = do_create(enable_serverless=False)

        warehouse_id = warehouse.get("id")

        # With the warehouse created, make sure that all users can attach to it.
        if for_user:
            print(f"Created warehouse \"{name}\" ({warehouse_id}) for {for_user}")
            client.permissions.warehouses.update_user(warehouse_id, for_user, "CAN_USE")
        else:
            print(f"Created warehouse \"{name}\" ({warehouse_id})")
            client.permissions.warehouses.update_group(warehouse_id, "users", "CAN_USE")

        print(f"| Lab ID:            {lab_id}")
        print(f"| Description:       {workspace_description}")
        print(f"| Workspace Name:    {workspace_name}")
        print(f"| Org ID:            {org_id}")
        print(f"| Autoscale minimum: {min_num_clusters}")
        print(f"| Autoscale maximum: {max_num_clusters}")

        if DBAcademyHelper.is_smoke_test():
            print(f"| Smoke Test:        {DBAcademyHelper.is_smoke_test()} ")

        dbgems.display_html(f"""
        <html style="margin:0"><body style="margin:0"><div style="margin:0">
            See <a href="/sql/warehouses/{warehouse_id}" target="_blank">{name} ({warehouse_id})</a>
        </div></body></html>
        """)

        return warehouse_id
