from dbacademy.rest.common import ApiContainer


class ModelVersions(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def transition_stage(self, name, version, new_stage, *, archive_existing_versions=None):
        if archive_existing_versions is None:
            archive_existing_versions = new_stage in ("Production", "Staging")
        return self.databricks.api("POST", "2.0/mlflow/model-versions/transition-stage", {
            "name": name,
            "version": version,
            "stage": new_stage,
            "archive_existing_versions": archive_existing_versions,
        })
