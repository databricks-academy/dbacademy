from typing import Union
from dbacademy import dbgems


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
        self.client.sql.endpoints.delete_by_name(name=name)

    def delete_sql_warehouses(self):
        self.workspace.do_for_all_users(self.workspace.usernames, lambda username: self.delete_sql_warehouses_for(username=username))

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_sql_warehouses(self, auto_stop_mins=120, enable_serverless_compute=False):
        self.workspace.do_for_all_users(self.workspace.usernames, lambda username: self.create_sql_warehouse_for(username=username,
                                                                                                                 auto_stop_mins=auto_stop_mins,
                                                                                                                 enable_serverless_compute=enable_serverless_compute))

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_sql_warehouse_for(self, username, auto_stop_mins=120, enable_serverless_compute=False):
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
    def create_shared_sql_warehouse(self, name: str, auto_stop_mins=120, enable_serverless_compute=False):
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
    def create_sql_warehouse(*, client: DBAcademyRestClient, name: str, auto_stop_mins: int, min_num_clusters, max_num_clusters, enable_serverless_compute: bool, lab_id: str = None, workspace_description: str = None, workspace_name: str = None, org_id: str = None, for_user: Union[str, None] = None):
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper_class import DBAcademyHelper
        from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper
        from dbacademy.dbrest.sql.endpoints import RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

        lab_id = lab_id or WorkspaceHelper.get_lab_id()
        workspace_description = workspace_description or WorkspaceHelper.get_workspace_description()
        workspace_name = workspace_name or WorkspaceHelper.get_workspace_name()
        org_id = org_id or dbgems.get_org_id()

        warehouse = client.sql.endpoints.create_or_update(
            name=name,
            cluster_size=CLUSTER_SIZE_2X_SMALL,
            enable_serverless_compute=enable_serverless_compute,
            min_num_clusters=min_num_clusters,
            max_num_clusters=max_num_clusters,
            auto_stop_mins=auto_stop_mins,
            enable_photon=True,
            spot_instance_policy=RELIABILITY_OPTIMIZED,
            channel=CHANNEL_NAME_CURRENT,
            tags={
                f"dbacademy.{WorkspaceHelper.PARAM_LAB_ID}": dbgems.clean_string(lab_id),
                f"dbacademy.{WorkspaceHelper.PARAM_DESCRIPTION}": dbgems.clean_string(workspace_description),
                f"dbacademy.workspace": dbgems.clean_string(workspace_name),
                f"dbacademy.org_id": dbgems.clean_string(org_id),
                f"dbacademy.source": dbgems.clean_string("Smoke-Test" if DBAcademyHelper.is_smoke_test() else lab_id),
            })
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
        print(f"| Org ID:            {workspace_name}")
        print(f"| Autoscale minimum: {min_num_clusters}")
        print(f"| Autoscale maximum: {max_num_clusters}")

        if DBAcademyHelper.is_smoke_test():
            print(f"| Smoke Test:        {DBAcademyHelper.is_smoke_test()} ")

        dbgems.display_html(f"""
        <html style="margin:0"><body style="margin:0"><div style="margin:0">
            See <a href="/sql/warehouses/{warehouse_id}" target="_blank">{name} ({warehouse_id})</a>
        </div></body></html>
        """)

