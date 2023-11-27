__all__ = ["WarehousesHelper"]

from typing import Union, List, Optional

from dbacademy.common import validate
from dbacademy.dbhelper.lesson_config import LessonConfig
from dbacademy.clients.dbrest import DBAcademyRestClient
from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper


class WarehousesHelper:

    def __init__(self, db_academy_reset_client: DBAcademyRestClient, workspace_helper: WorkspaceHelper):
        from dbacademy.common import validate

        self.__client = validate(db_academy_reset_client=db_academy_reset_client).required.as_type(DBAcademyRestClient)
        self.__workspace_helper = validate(workspace_helper=workspace_helper).required.as_type(WorkspaceHelper)

    @property
    def autoscale_min(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        return 1 if DBAcademyHelper.is_smoke_test() else 2

    @property
    def autoscale_max(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        return 1 if DBAcademyHelper.is_smoke_test() else 20

    def delete_sql_warehouses_for(self, lesson_config: LessonConfig) -> None:
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)

        name = DBAcademyHelper.to_unique_name(lesson_config=lesson_config, sep="-")

        self.__client.sql.warehouses.delete_by_name(name=name)

    def delete_sql_warehouses(self, lesson_config: LessonConfig) -> None:
        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper

        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)

        usernames = self.__workspace_helper.get_usernames(lesson_config=lesson_config)

        WorkspaceHelper.do_for_all_users(usernames, lambda username: self.delete_sql_warehouses_for(lesson_config=lesson_config))

    def execute_statements(self, warehouse_id: str, statements: List[str]) -> None:
        import json

        for statement in statements:
            results = self.__client.sql.statements.execute(warehouse_id=warehouse_id,
                                                           catalog="main",
                                                           schema="default",
                                                           statement=statement)

            state = results.get("status", dict()).get("state")

            if state != "SUCCEEDED":
                print(f"""Expected state to be "SUCCEEDED", found "{state}".""")
                print(json.dumps(results, indent=4))

    def create_sql_warehouses(self, *,
                              auto_stop_mins: Optional[int] = None,
                              lesson_config: LessonConfig,
                              enable_serverless_compute: bool = True) -> None:

        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper

        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)

        usernames = self.__workspace_helper.get_usernames(lesson_config=lesson_config)

        WorkspaceHelper.do_for_all_users(usernames, lambda username: self.create_sql_warehouse_for(username=username,
                                                                                                   auto_stop_mins=auto_stop_mins,
                                                                                                   lesson_config=lesson_config,
                                                                                                   enable_serverless_compute=enable_serverless_compute))

    def create_sql_warehouse_for(self, *,
                                 username: str,
                                 lesson_config: LessonConfig,
                                 auto_stop_mins: Optional[int] = None,
                                 enable_serverless_compute: bool = True):

        from dbacademy import dbgems
        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        username = validate(username=username).required.str()
        lesson_config = validate(lesson_config=lesson_config).required.as_type(LessonConfig)
        auto_stop_mins = validate(auto_stop_mins=auto_stop_mins).optional.int()
        enable_serverless_compute = validate(enable_serverless_compute=enable_serverless_compute).required.bool()

        name = DBAcademyHelper.to_unique_name(lesson_config=lesson_config, sep="-")

        return self.create_sql_warehouse(name=name,
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
    def create_shared_sql_warehouse(self, *,
                                    name: str,
                                    auto_stop_mins=None,
                                    enable_serverless_compute=True) -> str:

        from dbacademy import dbgems
        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper

        return self.create_sql_warehouse(name=name,
                                         for_user=None,
                                         auto_stop_mins=auto_stop_mins,
                                         min_num_clusters=self.autoscale_min,
                                         max_num_clusters=self.autoscale_max,
                                         enable_serverless_compute=enable_serverless_compute,
                                         lab_id=WorkspaceHelper.get_lab_id(),
                                         workspace_description=WorkspaceHelper.get_workspace_description(),
                                         workspace_name=WorkspaceHelper.get_workspace_name(),
                                         org_id=dbgems.get_org_id())

    def create_sql_warehouse(self, *,
                             name: str,
                             auto_stop_mins: Optional[int],
                             min_num_clusters,
                             max_num_clusters,
                             enable_serverless_compute: bool,
                             lab_id: str = None,
                             workspace_description: str = None,
                             workspace_name: str = None,
                             org_id: str = None,
                             for_user: Union[str, None] = None) -> str:
        """
        :param name:
        :param auto_stop_mins:
        :param min_num_clusters:
        :param max_num_clusters:
        :param enable_serverless_compute:
        :param lab_id:
        :param workspace_description:
        :param workspace_name:
        :param org_id:
        :param for_user:
        :return: The warehouse/endpoint id that was created.
        """

        from dbacademy import common, dbgems
        from dbacademy.dbhelper import dbh_constants
        from dbacademy.clients.rest.common import DatabricksApiException
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        from dbacademy.dbhelper.supporting.workspace_helper import WorkspaceHelper
        from dbacademy.clients.dbrest.sql_api.warehouses_api import RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

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

            return self.__client.sql.warehouses.create_or_update(
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
                    f"dbacademy.{dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_ID}": common.clean_string(lab_id),
                    f"dbacademy.{dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_DESCRIPTION}": common.clean_string(workspace_description),
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
            self.__client.permissions.warehouses.update_user(id_value=warehouse_id,
                                                             username=for_user,
                                                             permission_level="CAN_USE")
        else:
            print(f"Created warehouse \"{name}\" ({warehouse_id})")
            self.__client.permissions.warehouses.update_group(id_value=warehouse_id,
                                                              group_name="users",
                                                              permission_level="CAN_USE")
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
