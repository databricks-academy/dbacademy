from dbacademy.dbrest import DBAcademyRestClient


class JobsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint

    def create(self, params):
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/jobs/create", params)

    def run_now(self, job_id:str, notebook_params:dict = None):
        payload = {
            "job_id": 
            job_id
        }
        if notebook_params is not None:
            payload["notebook_params"] = notebook_params

        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/jobs/run-now", payload)

    def delete_by_job_id(self, job_id):
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/jobs/delete", {"job_id": job_id})

    def list(self):
        response = self.client.execute_get_json(f"{self.endpoint}/api/2.0/jobs/list")
        return response.get("jobs", list())

    def delete_by_name(self, jobs, success_only):
        if type(jobs) == dict:
            job_list = list(jobs.keys())
        elif type(jobs) == list:
            job_list = jobs
        elif type(jobs) == str:
            job_list = [jobs]
        else:
            raise Exception(f"Unsupported type: {type(jobs)}")

        jobs = self.list()

        deleted = 0
        s = "s" if len(jobs) != 1 else ""
        # print(f"Found {len(jobs)} job{s} total")

        for job_name in job_list:
            for job in jobs:
                if job_name == job["settings"]["name"]:
                    job_id = job["job_id"]

                    runs = self.client.runs().list_by_job_id(job_id)
                    s = "s" if len(runs) != 1 else ""
                    # print(f"Found {len(runs)} run{s} for job {job_id}")
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
                        print(f"""Deleting job #{job_id}, "{job_name}""")
                        for run in runs:
                            run_id = run.get("run_id")
                            print(f""" - Deleting run #{run_id}""")
                            self.client.runs().delete(run_id)

                        self.delete_by_job_id(job_id)
                        deleted += 1

        print(f"Deleted {deleted} jobs")
