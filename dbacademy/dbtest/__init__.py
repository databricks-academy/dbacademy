class ResultsEvaluator:
    def __init__(self, df):

        self.failed_set = df.filter("status == 'FAILED'").collect()
        self.ignored_set = df.filter("status == 'IGNORED'").collect()
        self.success_set = df.filter("status == 'SUCCESS'").collect()

        self.cell_style = "padding: 5px; border: 1px solid black; white-space:nowrap"
        self.header_style = "padding-right:1em; border: 1px solid black; font-weight:bold; padding: 5px; background-color: F0F0F0"

    @property
    def passed(self) -> bool:
        return len(self.failed_set) == 0

    def to_html(self, print_success_links=False) -> str:
        html = "</body>"
        html += self.add_section("Failed", self.failed_set)
        html += self.add_section("Ignored", self.ignored_set)
        html += self.add_section("Success", self.success_set, print_links=print_success_links)
        html += "</body>"
        return html

    def add_row(self, style, cloud, job, version, executed, duration):
        return f"""
      <tr>
          <td style="{style}">{cloud}</td>
          <td style="{style}; width:100%">{job}</td>
          <td style="{style}">{version}</td>
          <td style="{style}">{executed}</td>
          <td style="{style}; text-align:right">{duration}</td>
      </tr>
      """

    def format_duration(self, duration):
        from math import floor
        seconds = floor(duration / 1000) % 60
        minutes = floor(duration / (1000 * 60)) % 60
        hours = floor(duration / (1000 * 60 * 60)) % 24

        if hours > 0:
            return f"{hours}h, {minutes}m, {seconds}s"
        elif minutes > 0:
            return f"{minutes}m, {seconds}s"
        else:
            return f"{seconds}s"

    def add_section(self, title, rows, print_links=True):
        html = f"""<h1>{title}</h1>"""
        if len(rows) == 0:
            html += "<p>No records found</p>"
            return html

        html += f"""<table style="border-collapse: collapse; width:100%">"""
        html += self.add_row(self.header_style, "Cloud", "Job", "Version", "Executed", "Duration")

        for row in rows:
            link = row["notebook_path"]
            if print_links:
                link = to_job_link(row["cloud"], row["job_id"], row["notebook_path"])

            html += self.add_row(self.cell_style, row["cloud"], link, row["spark_version"], row["executed_at"], self.format_duration(row["execution_duration"]))
            html += """<tbody></tbody><tbody>"""

        html += "</table>"

        return html


def to_job_url(cloud, job_id):
    aws_workspace = "https://curriculum-dev.cloud.databricks.com/?o=3551974319838082"
    gcp_workspace = "https://8422030046858219.9.gcp.databricks.com/?o=8422030046858219"
    msa_workspace = "https://westus2.azuredatabricks.net/?o=2472203627577334"

    if cloud == "AWS": return f"{aws_workspace}#job/{job_id}/run/1"
    if cloud == "GCP": return f"{gcp_workspace}#job/{job_id}/run/1"
    if cloud == "MSA": return f"{msa_workspace}#job/{job_id}/run/1"
    raise Exception(f"The cloud {cloud} is not supported")


def to_job_link(cloud, job_id, label):
    url = to_job_url(cloud, job_id)
    return f"""<a href="{url}" target="_blank">{label}</a>"""


class NotebookDef:
    def __init__(self, test_round, path, ignored, include_solution, replacements, order):
        self.test_round = test_round
        self.path = path
        self.ignored = ignored
        self.include_solution = include_solution
        self.replacements = replacements
        self.order = order


class TestConfig:
    def __init__(self,
                 name,
                 version=0,
                 spark_version=None,
                 cloud=None,
                 instance_pool=None,
                 workers=None,
                 libraries=None,
                 client=None,
                 source_dir=None,
                 source_repo=None,
                 spark_conf=None,
                 results_table=None,  # Deprecated
                 results_database=None,  # Deprecated
                 include_solutions=True,
                 ):

        import uuid, re, time
        from dbacademy import dbrest
        from dbacademy import dbgems

        self.test_type = None
        self.notebooks = None
        self.client = dbrest.DBAcademyRestClient() if client is None else client

        # The instance of this test run
        self.suite_id = str(time.time()) + "-" + str(uuid.uuid1())

        # The name of the cloud on which this tests was ran
        self.cloud = dbgems.get_cloud() if cloud is None else cloud

        # Update the name of the database results will be logged to - convert any special characters to underscores
        results_database = f"smoke_tests_{dbgems.get_cloud().lower()}" if results_database is None else results_database
        results_database = re.sub("[^a-zA-Z0-9]", "_", results_database.lower())
        # Make N passes over the database name to remove duplicate underscores
        for i in range(10): results_database = results_database.replace("__", "_")
        self.results_database = results_database

        # Default the results_table if necissary
        if results_table is None: results_table = "smoke_test_" + name.replace(" ", "_").lower()

        # Update the name of the table results will be logged to
        if "." in results_table: raise ValueError("The results_table should not include the database name")

        # Convert any special characters to underscores
        results_table = re.sub("[^a-zA-Z0-9]", "_", results_table.lower())

        # Make N passes over the table name to remove duplicate underscores
        for i in range(10): results_table = results_table.replace("__", "_")

        self.results_table = results_table

        # Course Name
        self.name = name
        assert self.name is not None, "The course's name must be specified."

        # The Distribution's version
        self.version = version
        assert self.version is not None, "The course's version must be specified."

        # The runtime you wish to test against
        self.spark_version = dbgems.get_current_spark_version() if spark_version is None else spark_version

        # We can use local-mode clusters here
        self.workers = 0 if workers is None else workers

        # The instance pool from which to obtain VMs
        self.instance_pool = dbgems.get_current_instance_pool_id(self.client) if instance_pool is None else instance_pool

        # Spark configuration parameters
        self.spark_conf = dict() if spark_conf is None else spark_conf
        if self.workers == 0:
            self.spark_conf["spark.master"] = "local[*]"

        # The libraries to be attached to the cluster
        self.libraries = [] if libraries is None else libraries

        self.source_repo = dbgems.get_notebook_dir(offset=-2) if source_repo is None else source_repo
        self.source_dir = f"{self.source_repo}/Source" if source_dir is None else source_dir

        # We don't want the folling function to fail if we are using the "default" path which 
        # may or may not exists. The implication being that this will fail if called explicitly
        self.index_notebooks(include_solutions=include_solutions, fail_fast=source_dir is not None)

    def get_distribution_name(self, version):
        distribution_name = f"{self.name}" if version is None else f"{self.name}-v{version}"
        return distribution_name.replace(" ", "-").replace(" ", "-").replace(" ", "-")

    def index_notebooks(self, include_solutions=True, fail_fast=True):
        assert self.source_dir is not None, "TestConfig.source_dir must be specified"

        self.notebooks = dict()
        entities = self.client.workspace().ls(self.source_dir, recursive=True)

        if entities is None and fail_fast is False:
            return  # The directory doesn't exist
        elif entities is None and fail_fast is True:
            raise Exception(f"The specified directory ({self.source_dir}) does not exist (fail_fast={fail_fast}).")

        entities.sort(key=lambda e: e["path"])

        for i in range(len(entities)):
            entity = entities[i]
            test_round = 2  # Default test_round for all notebooks
            include_solution = include_solutions  # Initialize to the default value
            path = entity["path"][len(self.source_dir) + 1:]  # Get the notebook's path relative too the source root

            if path.lower().startswith("version"):  # Any notebook that starts with "version" as in "Version Info" or "Version 1.2.3"
                test_round = 0  # Never test the version notebook
                include_solution = False  # Exclude from the solutions folder

            if "includes/" in path.lower():  # Any folder that ends in "includes"
                test_round = 0  # Never test notebooks in the "includes" folders

            if "includes/reset" in path.lower():  # Any reset notebook in any "includes" folder
                test_round = 1  # Add to test_round #1 before all other tests
                include_solution = False  # Exclude from the solutions folder

            if "wip" in path.lower():
                print(f"""** WARNING ** The notebook "{path}" is excluded from the build as a work in progress (WIP)""")
            else:
                # Add our notebook to the set of notebooks to be tested.
                self.notebooks[path] = NotebookDef(test_round=test_round, path=path, ignored=False, include_solution=include_solution, replacements=dict(), order=i)

    def print(self):
        print("-" * 100)
        print("Test Configuration")
        print(f"suite_id:         {self.suite_id}")
        print(f"name:             {self.name}")
        print(f"version:          {self.version}")
        print(f"spark_version:    {self.spark_version}")
        print(f"workers:          {self.workers}")
        print(f"instance_pool:    {self.instance_pool}")
        print(f"spark_conf:       {self.spark_conf}")
        print(f"cloud:            {self.cloud}")
        print(f"libraries:        {self.libraries}")
        print(f"results_database: {self.results_database}")
        print(f"results_table:    {self.results_table}")
        print(f"source_repo:      {self.source_repo}")
        print(f"source_dir:       {self.source_dir}")

        max_length = 0
        for path in self.notebooks:
            if len(path) > max_length: max_length = len(path)

        if len(self.notebooks) == 0:
            print(f"notebooks:        none")
        else:
            print(f"notebooks:        {len(self.notebooks)}")

            rounds = list(map(lambda notebook_path: self.notebooks[notebook_path].test_round, self.notebooks))
            rounds.sort()
            rounds = set(rounds)

            for test_round in rounds:
                if test_round == 0:
                    print("\nRound #0: (published but not tested)")
                else:
                    print(f"\nRound #{test_round}")

                notebook_paths = list(self.notebooks.keys())
                notebook_paths.sort()

                # for path in notebook_paths:
                for notebook in sorted(self.notebooks.values(), key=lambda n: n.order):
                    # notebook = self.notebooks[path]
                    if test_round == notebook.test_round:
                        path = notebook.path.ljust(max_length)
                        ignored = str(notebook.ignored).ljust(5)
                        replacements = str(notebook.replacements)
                        include_solution = str(notebook.include_solution).ljust(5)
                        print(f"  {notebook.order: >2}: {path}   ignored={ignored}   include_solution={include_solution}   replacements={replacements}")

        print("-" * 100)


def create_test_job(client, test_config, job_name, notebook_path):
    params = {
        "notebook_task": {
            "notebook_path": f"{notebook_path}",
        },
        "name": f"{job_name}",
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "email_notifications": {},
        "libraries": test_config.libraries,
        "new_cluster": {
            "num_workers": test_config.workers,
            "instance_pool_id": f"{test_config.instance_pool}",
            "spark_version": f"{test_config.spark_version}",
            "spark_conf": test_config.spark_conf
        }
    }
    json_response = client.jobs().create(params)
    return json_response["job_id"]


# DEPRECATED - use TestSuite instead
class SuiteBuilder:
    def __init__(self, client, course_name, test_type):
        self.client = client
        self.course_name = course_name
        self.test_type = test_type
        self.jobs = dict()

    def add(self, notebook_path, ignored=False):
        import hashlib

        if self.client.workspace().get_status(notebook_path) is None:
            raise Exception(f"Notebook not found: {notebook_path}")

        hash_code = hashlib.sha256(notebook_path.encode()).hexdigest()
        job_name = f"[TEST] {self.course_name} | {self.test_type} | {hash_code}"
        self.jobs[job_name] = (notebook_path, 0, 0, ignored)


class TestInstance:
    def __init__(self, test_config, notebook, test_dir, test_type):
        import hashlib

        self.notebook = notebook
        self.job_id = 0
        self.run_id = 0

        if notebook.include_solution:
            self.notebook_path = f"{test_dir}/Solutions/{notebook.path}"
        else:
            self.notebook_path = f"{test_dir}/{notebook.path}"

        hash_code = hashlib.sha256(self.notebook_path.encode()).hexdigest()
        self.job_name = f"[TEST] {test_config.name} | {test_type} | {hash_code}"

        # Hack to bring the test type down into the test results via the test_config
        test_config.test_type = test_type


class TestSuite:
    def __init__(self, test_config, test_dir, test_type):
        self.test_dir = test_dir
        self.test_config = test_config
        self.client = test_config.client
        self.test_type = test_type
        self.test_rounds = dict()

        self.slack_thread_ts = None
        self.slack_first_message = None

        assert test_type is not None and test_type.strip() != "", "The test type must be specified."

        # Define each test_round first to make the next step full-proof
        for notebook in test_config.notebooks.values():
            self.test_rounds[notebook.test_round] = list()

        # Add each notebook to the dictionary or rounds which is a dictionary of tests
        for notebook in test_config.notebooks.values():
            if notebook.test_round > 0:
                # [job_name] = (notebook_path, 0, 0, ignored)
                test_instance = TestInstance(test_config, notebook, test_dir, test_type)
                self.test_rounds[notebook.test_round].append(test_instance)

                if self.client.workspace().get_status(test_instance.notebook_path) is None:
                    raise Exception(f"Notebook not found: {test_instance.notebook_path}")

    def delete_all_jobs(self, success_only=False):
        for test_round in self.test_rounds:
            self.client.jobs().delete_by_name(self.test_rounds[test_round], success_only=success_only)
        print()

    def test_all_synchronously(self, test_round, fail_fast=True):
        if test_round not in self.test_rounds:
            print(f"** WARNING ** There are no notebooks in round #{test_round}")
        else:
            tests = sorted(self.test_rounds[test_round], key=lambda t: t.notebook.order)

            if self.slack_first_message is None: self.send_status_update("info", f"*{self.test_config.name}*")
            what = "notebook" if len(tests) == 1 else "notebooks"
            self.send_status_update("info", f"Round #{test_round}: Testing {len(tests)} {what}  synchronously")

            print(f"Round #{test_round} test order:")
            for test in tests:
                print(f" {test.notebook.path}")
            print()

            for test in tests:
                self.send_status_update("info", f"Starting job for */{test.notebook.path}*")

                job_id = create_test_job(self.client, self.test_config, test.job_name, test.notebook_path)
                run_id = self.client.jobs().run_now(job_id)["run_id"]

                response = self.client.runs().wait_for(run_id)
                self.conclude_test(test, response, fail_fast)

    def test_all_asynchronously(self, test_round, fail_fast=False):

        tests = self.test_rounds[test_round]

        if self.slack_first_message is None: self.send_status_update("info", f"*{self.test_config.name}*")
        what = "notebook" if len(tests) == 1 else "notebooks"
        self.send_status_update("info", f"Round #{test_round}: Testing {len(tests)} {what}  asynchronously")

        for test in tests:
            self.send_status_update("info", f"Starting */{test.notebook.path}*")

            test.job_id = create_test_job(self.client, self.test_config, test.job_name, test.notebook_path)
            test.run_id = self.client.jobs().run_now(test.job_id)["run_id"]

        for test in tests:
            self.send_status_update("info", f"Waiting for */{test.notebook.path}*")

            response = self.client.runs().wait_for(test.run_id)
            self.conclude_test(test, response, fail_fast)

    def conclude_test(self, test, response, fail_fast):
        import json
        self.log_run(test, response)

        if response['state']['life_cycle_state'] == 'INTERNAL_ERROR':
            print()  # Usually a notebook-not-found
            print(json.dumps(response, indent=1))
            raise RuntimeError(response['state']['state_message'])

        result_state = response['state']['result_state']
        run_id = response["run_id"] if "run_id" in response else 0

        print("-" * 80)
        print(f"Run #{run_id} is {response['state']['life_cycle_state']} - {result_state}")
        print("-" * 80)

        if fail_fast and result_state == 'FAILED':
            raise RuntimeError(f"{response['task']['notebook_task']['notebook_path']} failed.")

    def log_run(self, test, response):
        import traceback, time, uuid, requests, json
        from dbacademy import dbgems
        from pyspark.sql.functions import current_timestamp

        try:
            print(f"*** Adding test results to {self.test_config.results_table}")

            job_id = response["job_id"] if "job_id" in response else 0
            run_id = response["run_id"] if "run_id" in response else 0

            result_state = response["state"]["result_state"] if "state" in response and "result_state" in response["state"] else "UNKNOWN"
            if result_state == "FAILED" and test.notebook.ignored: result_state = "IGNORED"

            execution_duration = response["execution_duration"] if "execution_duration" in response else 0
            notebook_path = response["task"]["notebook_task"]["notebook_path"] if "task" in response and "notebook_task" in response["task"] and "notebook_path" in response["task"][
                "notebook_task"] else "UNKNOWN"

            test_id = str(time.time()) + "-" + str(uuid.uuid1())

            test_results = [(self.test_config.suite_id,
                             test_id,
                             self.test_config.name,
                             result_state,
                             execution_duration,
                             self.test_config.cloud,
                             test.job_name,
                             job_id,
                             run_id,
                             notebook_path,
                             self.test_config.spark_version,
                             self.test_config.test_type)]

            sc, spark, dbutils = dbgems.init_locals()

            # Append our tests results to the database
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.test_config.results_database} COMMENT 'This is the temporary, cloud-specific database for smoke tests'")
            (spark.createDataFrame(test_results)
             .toDF("suite_id", "test_id", "name", "status", "execution_duration", "cloud", "job_name", "job_id", "run_id", "notebook_path", "spark_version", "test_type")
             .withColumn("executed_at", current_timestamp())
             .write.format("csv").mode("append").saveAsTable(f"{self.test_config.results_database}.{self.test_config.results_table}"))
            print(f"*** Logged results to {self.test_config.results_table}")

            response = requests.put("https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod/tests/smoke-tests", data=json.dumps({
                "suite_id": self.test_config.suite_id,
                "test_id": test_id,
                "name": self.test_config.name,
                "result_state": result_state,
                "execution_duration": execution_duration,
                "cloud": self.test_config.cloud,
                "job_name": test.job_name,
                "job_id": job_id,
                "run_id": run_id,
                "notebook_path": notebook_path,
                "spark_version": self.test_config.spark_version,
                "test_type": self.test_config.test_type,
            }))
            assert response.status_code == 200, f"({response.status_code}): {response.text}"

            message_type = "error" if result_state in ["FAILED", "IGNORED"] else "info"
            url = to_job_url(self.test_config.cloud, job_id)
            duration = int(execution_duration/1000)
            self.send_status_update(message_type, f"`{result_state}` ({duration} sec): *{test.notebook.path}*\n{url}")

        except Exception:
            print(f"Unable to log test results.")
            traceback.print_exc()

    def send_status_update(self, message_type, message):
        import requests, json

        if self.slack_first_message is None: self.slack_first_message = message

        payload = {
            "channel": "curr-smoke-tests",
            "message": message,
            "message_type": message_type,
            "first_message": self.slack_first_message,
            "thread_ts": self.slack_thread_ts
        }

        response = requests.post("https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod/slack/client", data=json.dumps(payload))
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        self.slack_thread_ts = response.json()["data"]["thread_ts"]
