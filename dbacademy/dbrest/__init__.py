from dbacademy import dbgems


class DBAcademyRestClient:

    def __init__(self, token=dbgems.get_notebooks_api_token(), endpoint=dbgems.get_notebooks_api_endpoint()):
        self.token = token
        self.endpoint = endpoint

    def workspace(self):
        from dbacademy.dbrest.workspace import WorkspaceClient
        return WorkspaceClient(self.token, self.endpoint)

    def jobs(self):
        from dbacademy.dbrest.jobs import JobsClient
        return JobsClient(self.token, self.endpoint)

    def runs(self):
        from dbacademy.dbrest.runs import RunsClient
        return RunsClient(self.token, self.endpoint)
