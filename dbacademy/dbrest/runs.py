import requests


class RunsClient:

    def __init__(self, token, endpoint):
        self.token = token
        self.endpoint = endpoint

    def get(self, run_id):
        response = requests.get(
            f"{self.endpoint}/api/2.0/jobs/runs/get?run_id={run_id}",
            headers={"Authorization": "Bearer " + self.token}
        )
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        return response.json()
