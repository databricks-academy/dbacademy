from dbacademy.dbrest import DBAcademyRestClient
import builtins

from dbacademy.rest.common import ApiContainer


class RunsClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def get(self, run_id):
        return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/jobs/runs/get?run_id={run_id}")

    def list(self, runs=builtins.list()):
        json_response = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/jobs/runs/list?limit=1000&offset={len(runs)}")
        runs.extend(json_response.get("runs", builtins.list()))

        if not json_response.get("has_more", False): return runs
        else: return self.list(runs)

    def list_by_job_id(self, job_id, runs=builtins.list()):
        json_response = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/jobs/runs/list?limit=1000&offset={len(runs)}&job_id={job_id}")
        runs.extend(json_response.get("runs", builtins.list()))

        if not json_response.get("has_more", False): return runs
        else: return self.list_by_job_id(runs)

    def cancel(self, run_id): 
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/runs/cancel", {"run_id": run_id})

    def delete(self, run_id): 
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/runs/delete", {"run_id": run_id}, expected=[200,400])

    def wait_for(self, run_id):
        import time

        wait = 15
        response = self.get(run_id)
        state = response["state"]["life_cycle_state"]
        job_id = response.get("job_id", 0)

        if state != "TERMINATED" and state != "INTERNAL_ERROR" and state != "SKIPPED":
            if state == "PENDING" or state == "RUNNING":
                print(f" - Job #{job_id}-{run_id} is {state}, checking again in {wait} seconds")
                time.sleep(wait)
            else:
                print(f" - Job #{job_id}-{run_id} is {state}, checking again in 5 seconds")
                time.sleep(5)

            return self.wait_for(run_id)

        return response
