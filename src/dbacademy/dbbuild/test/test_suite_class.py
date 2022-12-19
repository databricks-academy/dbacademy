from dbacademy import dbgems
from ..build_utils_class import BuildUtils


class TestSuite:
    from dbacademy.dbbuild.build_config_class import BuildConfig

    TEST_TYPE_INTERACTIVE = "interactive"
    TEST_TYPE_STOCK = "stock"
    TEST_TYPE_PHOTON = "photon"
    TEST_TYPE_ML = "ml"
    TEST_TYPES = [TEST_TYPE_INTERACTIVE, TEST_TYPE_STOCK, TEST_TYPE_PHOTON, TEST_TYPE_ML]

    def __init__(self, *, build_config: BuildConfig, test_dir: str, test_type: str, keep_success: bool = False):
        from dbacademy.dbbuild.test.test_instance_class import TestInstance

        self.test_dir = test_dir
        self.build_config = build_config
        self.client = build_config.client

        self.test_rounds = dict()
        self.test_results = list()

        self.slack_thread_ts = None
        self.slack_first_message = None

        self.keep_success = keep_success

        if dbgems.is_job():
            test_type = dbgems.get_parameter("test_type", None)
        elif test_type is None:
            test_type = TestSuite.TEST_TYPE_INTERACTIVE

        assert test_type in TestSuite.TEST_TYPES, f"The test type is expected to be one of {TestSuite.TEST_TYPES}, found \"{test_type}\""

        self.test_type = test_type

        # Define each test_round first to make the next step full-proof
        for notebook in self.build_config.notebooks.values():
            self.test_rounds[notebook.test_round] = list()

        # Add each notebook to the dictionary or rounds which is a dictionary of tests
        for notebook in self.build_config.notebooks.values():
            if notebook.test_round > 0:
                # [job_name] = (notebook_path, 0, 0, ignored)
                test_instance = TestInstance(self.build_config, notebook, test_dir, self.test_type)
                self.test_rounds[notebook.test_round].append(test_instance)

                if self.client.workspace().get_status(test_instance.notebook_path) is None:
                    raise Exception(f"Notebook not found: {test_instance.notebook_path}")

        url = f"{dbgems.get_workspace_url()}#job/list/search/dbacademy.course:{build_config.build_name}"
        print(f"Test Suite: {url}")

    def get_all_job_names(self):
        job_names = list()
        for test_round in self.test_rounds:
            job_names.extend([j.job_name for j in self.test_rounds[test_round]])

        return job_names

    def reset(self):
        # Delete all jobs, even those that were successful
        self.client.jobs.delete_by_name(job_names=self.get_all_job_names(), success_only=False)
        print()

    def cleanup(self):
        if self.keep_success:
            print(f"Skipping deletion of all jobs: TestSuite.keep_success == {self.keep_success}")
        else:
            # Delete all successful jobs, keeping those jobs that failed
            self.client.jobs.delete_by_name(job_names=self.get_all_job_names(), success_only=True)

    def create_test_job(self, *, job_name: str, notebook_path: str, policy_id: str = None):
        import re
        from dbacademy.dbrest.jobs import JobConfig
        from dbacademy.dbrest.clusters import ClusterConfig

        self.build_config.spark_conf["dbacademy.smoke-test"] = "true"

        self.test_type = re.sub(r"[^a-zA-Z\d]", "-", self.test_type.lower())
        while "--" in self.test_type:
            self.test_type = self.test_type.replace("--", "-")

        job_config = JobConfig(job_name=job_name, timeout_seconds=120*60, tags={
                "dbacademy.course": self.build_config.build_name,
                "dbacademy.source": "dbacademy-smoke-test",
                "dbacademy.test-type": self.test_type
            })
        task_config = job_config.add_task(task_key="Smoke-Test", description="Executes a single notebook, hoping that the magic smoke doesn't escape")
        task_config.library.from_dict(self.build_config.libraries)
        task_config.task.notebook(notebook_path=notebook_path, source="WORKSPACE", base_parameters=self.build_config.job_arguments)

        if policy_id is not None:
            policy = self.client.cluster_policies.get_by_id(policy_id)
            assert policy is not None, f"The policy \"{policy_id}\" does not exist or you do not have permissions to use specified policy: {[p.get('name') for p in self.client.cluster_policies.list()]}"

        cluster_config = ClusterConfig(cluster_name=None,
                                       num_workers=self.build_config.workers,
                                       spark_version=self.build_config.spark_version,
                                       spark_conf=self.build_config.spark_conf,
                                       node_type_id=None,  # Expecting to have an instance pool when testing
                                       instance_pool_id=self.build_config.instance_pool_id,
                                       single_user_name=dbgems.get_username(),
                                       policy_id=policy_id,
                                       autotermination_minutes=None,
                                       spark_env_vars={"WSFS_ENABLE_WRITE_SUPPORT": "true"})
        task_config.cluster.new(cluster_config=cluster_config)

        job_id = self.client.jobs.create_from_config(job_config)
        return job_id

    def test_all_synchronously(self, test_round, fail_fast=True, service_principal: str = None, policy_id: str = None) -> bool:

        if test_round not in self.test_rounds:
            print(f"** WARNING ** There are no notebooks in round #{test_round}")
            return True

        tests = sorted(self.test_rounds[test_round], key=lambda t: t.notebook.order)

        self.send_first_message()

        what = "notebook" if len(tests) == 1 else "notebooks"
        self.send_status_update("info", f"Round #{test_round}: Testing {len(tests)} {what}  synchronously")

        print(f"Round #{test_round} test order:")
        for i, test in enumerate(tests):
            print(f"{i+1:>4}: {test.notebook.path}")
        print()

        # Assume that all tests passed
        passed = True

        for test in tests:

            if fail_fast and not passed:
                self.log_run(test, {})

                print("-" * 80)
                print(f"Skipping job, previous failure for {test.job_name}")
                print("-" * 80)

            else:
                self.send_status_update("info", f"Starting */{test.notebook.path}*")

                job_id = self.create_test_job(job_name=test.job_name,
                                              notebook_path=test.notebook_path,
                                              policy_id=policy_id)
                if service_principal:
                    sp = self.client.scim.service_principals.get_by_name(service_principal)
                    self.client.permissions.jobs.change_owner(job_id=job_id, owner_type="service_principal", owner_id=sp.get("applicationId"))

                run_id = self.client.jobs.run_now(job_id).get("run_id")

                host_name = dbgems.get_notebooks_api_endpoint() if dbgems.get_browser_host_name() is None else f"https://{dbgems.get_browser_host_name()}"
                print(f"""/{test.notebook.path}\n - {host_name}?o={dbgems.get_workspace_id()}#job/{job_id}/run/{run_id}""")

                response = self.client.runs().wait_for(run_id)
                passed = False if not self.conclude_test(test, response) else passed

        return passed

    def test_all_asynchronously(self, test_round: int, service_principal: str = None, policy_id: str = None) -> bool:

        tests = self.test_rounds[test_round]

        self.send_first_message()

        what = "notebook" if len(tests) == 1 else "notebooks"
        self.send_status_update("info", f"Round #{test_round}: Testing {len(tests)} {what}  asynchronously")

        # Launch each test
        for test in tests:
            self.send_status_update("info", f"Starting */{test.notebook.path}*")

            test.job_id = self.create_test_job(job_name=test.job_name,
                                               notebook_path=test.notebook_path,
                                               policy_id=policy_id)
            if service_principal:
                sp = self.client.scim.service_principals.get_by_name(service_principal)
                self.client.permissions.jobs.change_owner(job_id=test.job_id, owner_type="service_principal", owner_id=sp.get("applicationId"))

            test.run_id = self.client.jobs.run_now(test.job_id).get("run_id")

            print(f"""/{test.notebook.path}\n - https://{dbgems.get_browser_host_name()}?o={dbgems.get_workspace_id()}#job/{test.job_id}/run/{test.run_id}""")

        # Assume that all tests passed
        passed = True
        print(f"""\nWaiting for all test to complete:""")

        # Block until all tests completed
        for test in tests:
            self.send_status_update("info", f"Waiting for */{test.notebook.path}*")

            response = self.client.runs().wait_for(test.run_id)
            passed = False if not self.conclude_test(test, response) else passed

        return passed

    def conclude_test(self, test, response) -> bool:
        import json
        self.log_run(test, response)

        if response['state']['life_cycle_state'] == 'INTERNAL_ERROR':
            print()  # Usually a notebook-not-found
            print(json.dumps(response, indent=1))
            raise RuntimeError(response['state']['state_message'])

        result_state = response['state']['result_state']
        run_id = response.get("run_id", 0)
        job_id = response.get("job_id", 0)

        print("-" * 80)
        print(f"Job #{job_id}-{run_id} is {response['state']['life_cycle_state']} - {result_state}")
        print("-" * 80)

        return result_state != 'FAILED'

    def to_results_evaluator(self):
        from .results_evaluator import ResultsEvaluator
        return ResultsEvaluator(self.test_results, self.keep_success)

    def log_run(self, test, response):
        import time, uuid, requests, json

        job_id = response.get("job_id", 0)
        run_id = response.get("run_id", 0)

        result_state = response.get("state", {}).get("result_state", "UNKNOWN")
        if result_state == "FAILED" and test.notebook.ignored:
            result_state = "IGNORED"

        execution_duration = response.get("execution_duration", 0)
        notebook_path = response.get("task", {}).get("notebook_task", {}).get("notebook_path", "UNKNOWN")

        test_id = str(time.time()) + "-" + str(uuid.uuid1())

        payload = {
            "suite_id": self.build_config.suite_id,
            "test_id": test_id,
            "name": self.build_config.name,
            "result_state": result_state,
            "execution_duration": execution_duration,
            "cloud": self.build_config.cloud,
            "job_name": test.job_name,
            "job_id": job_id,
            "run_id": run_id,
            "notebook_path": notebook_path,
            "spark_version": self.build_config.spark_version,
            "test_type": self.test_type,
        }

        self.test_results.append(payload)

        try:
            response = requests.put("https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod/tests/smoke-tests", data=json.dumps(payload))
            assert response.status_code == 200, f"({response.status_code}): {response.text}"

        except Exception as e:
            import traceback
            message = f"{str(e)}\n{traceback.format_exc()}"
            dbgems.print_warning(title="Smoke Test Logging Failure", message=message, length=100)

        if result_state == "FAILED":
            message_type = "error"
        elif result_state == "IGNORED":
            message_type = "warn"
        else:
            message_type = "info"
        url = BuildUtils.to_job_url(job_id=job_id, run_id=run_id)
        self.send_status_update(message_type, f"*`{result_state}` /{test.notebook.path}*\n\n{url}")

    def send_first_message(self):
        if self.slack_first_message is None:
            self.send_status_update("info", f"*{self.build_config.name}*\nCloud: *{self.build_config.cloud}* | Mode: *{self.test_type}*")

    def send_status_update(self, message_type, message):
        import requests, json

        if self.slack_first_message is None:
            self.slack_first_message = message

        payload = {
            "channel": "curr-smoke-tests",
            "message": message,
            "message_type": message_type,
            "first_message": self.slack_first_message,
            "thread_ts": self.slack_thread_ts
        }

        try:
            response = requests.post("https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod/slack/client", data=json.dumps(payload))
            assert response.status_code == 200, f"({response.status_code}): {response.text}"
            self.slack_thread_ts = response.json().get("data", {}).get("thread_ts")
        except Exception as e:
            dbgems.print_warning(title="Slack Notification Failure", message=str(e), length=100)
