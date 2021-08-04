# Databricks notebook source
import json
import time
import requests
from dbacademy.dbrest import DBAcademyRestClient


class JobsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str, throttle: int):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint
        self.throttle = throttle  # Number of seconds by which to throttle input

    def create(self, params):
        response = requests.post(
            f"{self.endpoint}/api/2.0/jobs/create",
            headers={"Authorization": "Bearer " + self.token},
            data=json.dumps(params)
        )
        time.sleep(self.throttle)
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        return response.json()

    def run_now(self, job_id):
        response = requests.post(
            f"{self.endpoint}/api/2.0/jobs/run-now",
            headers={"Authorization": f"Bearer {self.token}"},
            data=json.dumps({"job_id": job_id})
        )
        time.sleep(self.throttle)
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        return response.json()

    def delete_by_job_id(self, job_id):
        response = requests.post(
            f"{self.endpoint}/api/2.0/jobs/delete",
            headers={"Authorization": f"Bearer {self.token}"},
            data=json.dumps({"job_id": job_id})
        )
        time.sleep(self.throttle)
        assert response.status_code == 200, f"({response.status_code}): {response.text}"

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
            headers={"Authorization": f"Bearer {self.token}"}
        )
        time.sleep(self.throttle)
        assert response.status_code == 200, f"({response.status_code}): {response.text}"

        deleted = 0
        jobs = response.json()["jobs"]
        print(f"Found {len(jobs)} jobs total")

        for job_name in job_list:
            for job in jobs:
                if job_name == job["settings"]["name"]:
                    job_id = job["job_id"]

                    runs = self.client.runs().list_by_job_id(job_id)
                    print(f"Found {len(runs)} for job {job_id}")
                    delete_job = True

                    for run in runs:
                        state = run["state"]
                        if success_only and state["life_cycle_state"] != "TERMINATED":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "TERMINATED" but "{state["life_cycle_state"]}", this job must be deleted manually""")
                        if success_only and state["result_state"] != "SUCCESS":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "SUCCESS" but "{state["result_state"]}", this job must be deleted manually""")

                    if delete_job:
                        print(f""" - Deleting The job "{job_name}" ({job_id})""")
                        self.delete_by_job_id(job_id)
                        deleted += 1

        print(f"Deleted {deleted} jobs")
