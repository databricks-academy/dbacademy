import json
import requests


class JobsClient:

    def __init__(self, token, endpoint):
        self.token = token
        self.endpoint = endpoint

    def create(self, params):
        response = requests.post(
            f"{self.endpoint}/api/2.0/jobs/create",
            headers={"Authorization": "Bearer " + self.token},
            data=json.dumps(params)
        )
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        return response.json()

    def run_job_now(self, job_id):
        response = requests.post(
            f"{self.endpoint}/api/2.0/jobs/run-now",
            headers={"Authorization": "Bearer " + self.token},
            data=json.dumps({"job_id": job_id})
        )
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        return response.json()["run_id"]

    def delete_by_name(self, jobs, success_only):
        if type(jobs) == dict:
            job_list = list(jobs.keys())
        elif type(jobs) == list:
            job_list = jobs
        elif type(jobs) == str:
            job_list = [jobs]
        else:
            raise Exception(f"Unsupported type: {type(jobs)}")

        response = requests.get(
            f"{self.endpoint}/api/2.0/jobs/list",
            headers={"Authorization": "Bearer " + self.token}
        )
        assert response.status_code == 200, f"({response.status_code}): {response.text}"

        deleted = 0
        jobs = response.json()["jobs"]
        print(f"Found {len(jobs)} jobs total")

        for job_name in job_list:
            for job in jobs:
                if job_name == job["settings"]["name"]:
                    job_id = job["job_id"]

                    response = requests.get(
                        f"{self.endpoint}/api/2.0/jobs/runs/list?job_id={job_id}",
                        headers={"Authorization": "Bearer " + self.token}
                    )
                    assert response.status_code == 200, f"({response.status_code}): {response.text}"
                    runs = response.json()["runs"]
                    delete_job = True

                    for run in runs:
                        state = run["state"]
                        if success_only and state["life_cycle_state"] != "TERMINATED":
                            delete_job = False
                            print(f"""The job "{job_name}" was not "TERMINATED" but "{state["life_cycle_state"]}", this job must be deleted manually""")
                        if success_only and state["result_state"] != "SUCCESS":
                            delete_job = False
                            print(f"""The job "{job_name}" was not "SUCCESS" but "{state["result_state"]}", this job must be deleted manually""")

                    if delete_job:
                        print(f"Deleting job #{job_id}")
                        response = requests.post(
                            f"{self.endpoint}/api/2.0/jobs/delete",
                            headers={"Authorization": "Bearer " + self.token},
                            data=json.dumps({"job_id": job_id})
                        )
                        assert response.status_code == 200, f"({response.status_code}): {response.text}"
                        deleted += 1

        print(f"Deleted {deleted} jobs")
