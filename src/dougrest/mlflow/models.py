from dbacademy.rest.common import ApiContainer


class RegisteredModels(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def list(self, *, models_per_page=None):
        page_token = None
        while True:
            response = self.databricks.api("GET", "2.0/mlflow/registered-models/list", {
                "max_results": models_per_page,
                "page_token": page_token,
            })
            page_token = response.get("next_page_token")
            yield from response["registered_models"]
            if not page_token:
                return

    def create(self, name, description=None, tags={}):
        return self.databricks.api("POST", "2.0/mlflow/registered-models/create", {
            "name": name,
            "description": description,
            "tags": tags
        })

    def rename(self, name, new_name):
        return self.databricks.api("POST", "2.0/mlflow/registered-models/create", {
            "name": name,
            "new_name": new_name,
        })

    def update(self, model):
        return self.databricks.api("PATCH", "2.0/mlflow/registered-models/update", model)

    def delete(self, model, *, force=False):
        while force:
            model = self.get(model["name"])
            force = False
            for v in model.get("latest_versions", ()):
                if v.get("current_stage") in ("Production", "Staging"):
                    self.databricks.mlflow.model_versions.transition_stage(model["name"], v["version"],
                                                                           "Archived")
                    force = True
        return self.databricks.api("DELETE", "2.0/mlflow/registered-models/delete", {
            "name": model["name"]
        })

    def get(self, name):
        return self.databricks.api("GET", "2.0/mlflow/registered-models/get", {
            "name": name,
        }).get("registered_model")

    def search(self, filter, order_by, *, models_per_page=None):
        page_token = None
        while True:
            response = self.databricks.api("GET", "2.0/mlflow/registered-models/list", {
                "filter": filter,
                "order_by": order_by,
                "max_results": models_per_page,
                "page_token": page_token,
            })
            page_token = response.get("next_page_token")
            yield from response["registered_models"]
            if not page_token:
                return
