from dbacademy.rest.common import ApiContainer


class Secrets(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def create_scope(self, name, initial_manage_principal):
        """
        Create a Databricks-backed secret scope in which secrets are stored in Databricks-managed storage and encrypted with a cloud-based specific encryption key.

        >>> self.create_scope("my-simple-databricks-scope", "users")
        """
        pass
