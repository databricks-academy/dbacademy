# Databricks notebook source
import requests
from dbacademy.dbrest import DBAcademyRestClient


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
        json_response = response.json()
        return json_response["runs"] if "runs" in json_response else []

    def wait_for(self, run_id):
        import time

        wait = 60
        response = self.get(run_id)
        state = response["state"]["life_cycle_state"]

        if state != "TERMINATED" and state != "INTERNAL_ERROR":
            if state == "PENDING" or state == "RUNNING":
                print(f" - Run #{run_id} is {state}, checking again in {wait} seconds")
                time.sleep(wait)
            else:
                print(f" - Run #{run_id} is {state}, checking again in 5 seconds")
                time.sleep(5)

            return self.wait_for(run_id)

        return response
