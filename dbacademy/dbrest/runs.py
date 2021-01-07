import requests
from dbacademy.dbtest import DBAcademyRestClient


class RunsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def get(self, run_id):
        response = requests.get(
            f"{self.endpoint}/api/2.0/jobs/runs/get?run_id={run_id}",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        return response.json()

    def list_by_job_id(self, job_id):
        response = requests.get(
            f"{self.endpoint}/api/2.0/jobs/runs/list?job_id={job_id}",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        runs = response.json()["runs"]
