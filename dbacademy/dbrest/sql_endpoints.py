# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient


class SqlEndpointsClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def start(self, endpoint_id):
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/sql/endpoints/{endpoint_id}/start")

    def stop(self, endpoint_id):
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/sql/endpoints/{endpoint_id}/stop")

    # def get(self, run_id):
    #     return self.client.execute_get_json(f"{self.endpoint}/api/2.0/jobs/runs/get?run_id={run_id}")

    # def list_by_job_id(self, job_id):
    #     json_response = self.client.execute_get_json(f"{self.endpoint}/api/2.0/jobs/runs/list?job_id={job_id}")
    #     return json_response["runs"] if "runs" in json_response else []

    # def wait_for(self, run_id):
    #     import time

    #     wait = 15
    #     response = self.get(run_id)
    #     state = response["state"]["life_cycle_state"]
    #     job_id = response.get("job_id", 0)

    #     if state != "TERMINATED" and state != "INTERNAL_ERROR":
    #         if state == "PENDING" or state == "RUNNING":
    #             print(f" - Job #{job_id}-{run_id} is {state}, checking again in {wait} seconds")
    #             time.sleep(wait)
    #         else:
    #             print(f" - Job #{job_id}-{run_id} is {state}, checking again in 5 seconds")
    #             time.sleep(5)

    #         return self.wait_for(run_id)

    #     return response
