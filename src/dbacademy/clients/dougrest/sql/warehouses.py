from dbacademy.clients.rest.common import ApiContainer


class Warehouses(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def get_by_id(self, warehouse_id):
        return self.databricks.api("GET", f"/api/2.0/sql/warehouses/{warehouse_id}")

    def get_by_name(self, name):
        return next((ep for ep in self.list() if ep["name"] == name), None)

    def list(self):
        response = self.databricks.api("GET", "/api/2.0/sql/warehouses/")
        return response.get("warehouses", [])

    def list_by_name(self):
        warehouses = self.list()
        return {e["name"]: e for e in warehouses}

    def create(self, name, size="XSMALL", min_num_clusters=1, max_num_clusters=1, timeout_minutes=120,
               photon=False, spot=False, preview_channel=False, **spec):
        data = {
            "name": name,
            "size": size,
            "min_num_clusters": min_num_clusters,
            "max_num_clusters": max_num_clusters,
            "spot_instance_policy": "COST_OPTIMIZED" if spot else "RELIABILITY_OPTIMIZED",
            "enable_photon": str(bool(photon)).lower(),
        }
        if preview_channel:
            data["channel"] = {"name": "CHANNEL_NAME_PREVIEW"}
        if timeout_minutes and timeout_minutes > 0:
            data["auto_stop_mins"] = timeout_minutes
        data.update(spec)
        response = self.databricks.api("POST", "/api/2.0/sql/warehouses/", data)
        return response["id"]

    def edit(self, warehouse):
        warehouse_id = warehouse["id"]
        response = self.databricks.api("POST", f"/api/2.0/sql/warehouses/{warehouse_id}/edit", warehouse)
        return response

    # TODO Remove noinspection PyUnusedLocal once tested
    # noinspection PyUnusedLocal
    def edit_by_name(self, name, size="XSMALL", min_num_clusters=1, max_num_clusters=1, timeout_minutes=120,
                     photon=False, spot=False, preview_channel=False):
        raise Exception("Untested, function.  Test me first.")

        # TODO remove noinspection PyUnreachableCode once tested
        # noinspection PyUnreachableCode
        data = {
            "name": name,
            "size": size,
            "min_num_clusters": min_num_clusters,
            "max_num_clusters": max_num_clusters,
            "spot_instance_policy": "COST_OPTIMIZED" if spot else "RELIABILITY_OPTIMIZED",
            "enable_photon": str(bool(photon)).lower()
        }
        if timeout_minutes and timeout_minutes > 0:
            data["auto_stop_mins"] = timeout_minutes
        if preview_channel:
            data["channel"] = {"name": "CHANNEL_NAME_PREVIEW"}
        response = self.databricks.api("POST", f"/api/2.0/sql/warehouses/{id}/edit", data)
        return response["id"]

    def start(self, warehouse_id):
        response = self.databricks.api("POST", f"/api/2.0/sql/warehouses/{warehouse_id}/start")
        return response

    def stop(self, warehouse_id):
        response = self.databricks.api("POST", f"/api/2.0/sql/warehouses/{warehouse_id}/stop")
        return response

    def delete(self, warehouse_id):
        response = self.databricks.api("DELETE", f"/api/2.0/sql/warehouses/{warehouse_id}")
        return response
