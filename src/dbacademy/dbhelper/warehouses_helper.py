from typing import Union
from dbacademy_helper import DBAcademyHelper
from dbacademy_helper.workspace_helper import WorkspaceHelper


class WarehousesHelper:
    def __init__(self, workspace: WorkspaceHelper, da: DBAcademyHelper):
        self.da = da
        self.client = da.client
        self.workspace = workspace

    @property
    def autoscale_min(self):
        return 1 if self.da.is_smoke_test() else 2  # math.ceil(self.students_count / 20)

    @property
    def autoscale_max(self):
        return 1 if self.da.is_smoke_test() else 20  # math.ceil(self.students_count / 5)

    def delete_sql_warehouses_for(self, username):
        name = self.da.to_schema_name(username)
        self.client.sql.endpoints.delete_by_name(name=name)

    def delete_sql_warehouses(self):
        self.workspace.do_for_all_users(lambda username: self.delete_sql_warehouses_for(username=username))

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_sql_warehouses(self, auto_stop_mins=120, enable_serverless_compute=False):
        self.workspace.do_for_all_users(lambda username: self.create_sql_warehouse_for(username=username,
                                                                                       auto_stop_mins=auto_stop_mins,
                                                                                       enable_serverless_compute=enable_serverless_compute))

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_sql_warehouse_for(self, username, auto_stop_mins=120, enable_serverless_compute=False):
        return self._create_sql_warehouse(username=username,
                                          name=self.da.to_schema_name(username),
                                          auto_stop_mins=auto_stop_mins,
                                          min_num_clusters=1,
                                          max_num_clusters=1,
                                          enable_serverless_compute=enable_serverless_compute)

    def create_shared_sql_warehouse(self, name: str = "DBAcademy Warehouse", auto_stop_mins=120, enable_serverless_compute=False):
        return self._create_sql_warehouse(username=None,
                                          name=name,
                                          auto_stop_mins=auto_stop_mins,
                                          min_num_clusters=self.autoscale_min,
                                          max_num_clusters=self.autoscale_max,
                                          enable_serverless_compute=enable_serverless_compute)

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def _create_sql_warehouse(self, username: Union[str, None], name: str, auto_stop_mins: int, min_num_clusters, max_num_clusters, enable_serverless_compute: bool):
        from dbacademy.dbrest.sql.endpoints import RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

        warehouse = self.client.sql.endpoints.create_or_update(
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
                "dbacademy.event_name": self.da.clean_string(self.workspace.event_name),
                "dbacademy.students_count": self.da.clean_string(self.workspace.student_count),
                "dbacademy.workspace": self.da.clean_string(self.workspace.workspace_name),
                "dbacademy.org_id": self.da.clean_string(self.workspace.org_id),
                "dbacademy.course": self.da.clean_string(self.da.course_config.course_name),  # Tag the name of the course
                "dbacademy.source": self.da.clean_string("Smoke-Test" if self.da.is_smoke_test() else self.da.course_config.course_name),
            })
        warehouse_id = warehouse.get("id")

        # With the warehouse created, make sure that all users can attach to it.
        if username is None:
            print(f"Created warehouse \"{name}\" ({warehouse_id})")
            self.client.permissions.warehouses.update_group(warehouse_id, "users", "CAN_USE")
        else:
            print(f"Created warehouse \"{name}\" ({warehouse_id}) for {username}")
            self.client.permissions.warehouses.update_user(warehouse_id, username, "CAN_USE")

        print(f"  Configured for:    {self.workspace.configure_for}")
        print(f"  Event Name:        {self.workspace.event_name}")
        print(f"  Student Count:     {self.workspace.student_count}")
        print(f"  Provisioning:      {len(self.workspace.usernames)}")
        print(f"  Autoscale minimum: {min_num_clusters}")
        print(f"  Autoscale maximum: {max_num_clusters}")
        if self.da.is_smoke_test:
            print(f"  Smoke Test:        {self.da.is_smoke_test()} ")
