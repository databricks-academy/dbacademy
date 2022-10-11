from dbacademy.rest.common import ApiContainer


class Endpoints(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def get_by_id(self, id):
        return self.databricks.api("GET", f"2.0/sql/endpoints/{id}")

    def get_by_name(self, name):
        return next((ep for ep in self.list() if ep["name"] == name), None)

    def list(self):
        response = self.databricks.api("GET", "2.0/sql/endpoints/")
        return response.get("endpoints", [])

    def list_by_name(self):
        endpoints = self.list()
        return {e["name"]: e for e in endpoints}

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
        response = self.databricks.api("POST", "2.0/sql/endpoints/", data)
        return response["id"]

    def edit(self, endpoint):
        id = endpoint["id"]
        response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/edit", endpoint)
        return response

    def edit_by_name(self, name, size="XSMALL", min_num_clusters=1, max_num_clusters=1, timeout_minutes=120,
                     photon=False, spot=False, preview_channel=False):
        raise Exception("Untested, function.  Test me first.")
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
        response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/edit", data)
        return response["id"]

    def start(self, id):
        response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/start")
        return response

    def stop(self, id):
        response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/stop")
        return response

    def delete(self, id):
        response = self.databricks.api("DELETE", f"2.0/sql/endpoints/{id}")
        return response
