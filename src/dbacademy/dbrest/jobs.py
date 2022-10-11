from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class JobsClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class
        self.base_uri = f"{self.client.endpoint}/api/2.1/jobs"

    def create(self, params):
        if "notebook_task" in params:
            print("*"*80)
            print("* DEPRECATION WARNING")
            print("* You are using the Jobs 2.0 version of create as noted by the existence of the notebook_task parameter.")
            print("* Please upgrade to the 2.1 version.")
            print("*"*80)
            return self.create_2_0(params)
        else:
            return self.create_2_1(params)

    def create_2_0(self, params):
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/create", params)

    def create_2_1(self, params):
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.1/jobs/create", params)

    def run_now(self, job_id: str, notebook_params: dict = None):
        payload = {
            "job_id": job_id
        }
        if notebook_params is not None:
            payload["notebook_params"] = notebook_params

        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/run-now", payload)

    def get(self, job_id):
        return self.get_by_id(job_id)

    def get_by_id(self, job_id):
        return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/jobs/get?job_id={job_id}")

    def get_by_name(self, name: str):
        offset = 0  # Start with zero
        limit = 25  # Default maximum

        def search(jobs_list):
            job_ids = [j.get("job_id") for j in jobs_list if name == j.get("settings").get("name")]
            return (False, None) if len(job_ids) == 0 else (True, self.get_by_id(job_ids[0]))

        target_url = f"{self.client.endpoint}/api/2.1/jobs/list?limit={limit}"
        response = self.client.execute_get_json(target_url)
        jobs = response.get("jobs", list())

        found, job = search(jobs)
        if found: return job

        while response.get("has_more", False):
            offset += limit
            response = self.client.execute_get_json(f"{target_url}&offset={offset}")
            jobs = response.get("jobs", list())

            found, job = search(jobs)
            if found: return job

        return None

    def list_n(self, offset: int = 0, limit: int = 25, expand_tasks: bool = False):
        limit = min(25, limit)
        offset = max(0, offset)

        target_url = f"{self.client.endpoint}/api/2.1/jobs/list?offset={offset}&limit={limit}&expand_tasks={expand_tasks}"
        response = self.client.execute_get_json(target_url)
        return response.get("jobs", list())

    def list(self, expand_tasks: bool = False):
        offset = 0  # Start with zero
        limit = 25  # Default maximum

        target_url = f"{self.client.endpoint}/api/2.1/jobs/list?limit={limit}&expand_tasks={expand_tasks}"
        response = self.client.execute_get_json(target_url)
        all_jobs = response.get("jobs", list())

        while response.get("has_more", False):
            offset += limit
            response = self.client.execute_get_json(f"{target_url}&offset={offset}")
            all_jobs.extend(response.get("jobs", list()))

        return all_jobs

    def delete_by_job_id(self, job_id):
        print("*" * 80)
        print("* DEPRECATION WARNING")
        print("* delete_by_job_id() has been replaced by delete_by_id()")
        print("*" * 80)
        self.delete_by_id(job_id)

    def delete_by_id(self, job_id):
        self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/delete", {"job_id": job_id})

    def delete_by_name(self, job_names, success_only: bool):
        if type(job_names) == dict:
            job_names = list(job_names.keys())
        elif type(job_names) == list:
            job_names = job_names
        elif type(job_names) == str:
            job_names = [job_names]
        else:
            raise TypeError(f"Unsupported type: {type(job_names)}")

        # Get a list of all jobs
        jobs = self.list()

        s = "s" if len(jobs) != 1 else ""
        self.client.vprint(f"Found {len(jobs)} job{s}.")

        assert type(success_only) == bool, f"Expected \"success_only\" to be of type \"bool\", found \"{success_only}\"."
        self.client.vprint(f"...deleting successful jobs only: {success_only}")

        deleted = 0

        for job_name in job_names:
            for job in jobs:
                if job_name == job.get("settings").get("name"):
                    job_id = job.get("job_id")

                    runs = self.client.runs().list_by_job_id(job_id)
                    s = "s" if len(runs) != 1 else ""
                    self.client.vprint(f"Found {len(runs)} run{s} for job {job_id}")
                    delete_job = True

                    for run in runs:
                        state = run.get("state")
                        result_state = state.get("result_state", None)
                        life_cycle_state = state.get("life_cycle_state", None)

                        if success_only and life_cycle_state != "TERMINATED":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "TERMINATED" but "{life_cycle_state}", this job must be deleted manually""")
                        if success_only and result_state != "SUCCESS":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "SUCCESS" but "{result_state}", this job must be deleted manually""")

                    if delete_job:
                        self.client.vprint(f"...Deleting job #{job_id}, \"{job_name}\"")
                        for run in runs:
                            run_id = run.get("run_id")
                            self.client.vprint(f"""   - Deleting run #{run_id}""")
                            self.client.runs().delete(run_id)

                        self.delete_by_id(job_id)
                        deleted += 1

        self.client.vprint(f"...deleted {deleted} jobs")
