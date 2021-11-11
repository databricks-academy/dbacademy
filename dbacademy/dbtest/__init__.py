# Databricks notebook source
class ResultsEvaluator:
    def __init__(self, df):
        self.aws_workspace = "https://curriculum.cloud.databricks.com/?o=5775574114781423"
        self.gcp_workspace = "https://8422030046858219.9.gcp.databricks.com/?o=8422030046858219"
        self.msa_workspace = "https://westus2.azuredatabricks.net/?o=2472203627577334"
        
        self.failed_set = df.filter("status == 'FAILED'").collect()
        self.ignored_set = df.filter("status == 'IGNORED'").collect()
        self.success_set = df.filter("status == 'SUCCESS'").collect()

        self.cell_style = "padding: 5px; border: 1px solid black; white-space:nowrap"
        self.header_style = "padding-right:1em; border: 1px solid black; font-weight:bold; padding: 5px; background-color: F0F0F0"
        
    @property
    def passed(self) -> bool:
      return len(self.failed_set) == 0
        
    def to_html(self, print_success_links=False) -> int:
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
      seconds = floor(duration/1000)%60
      minutes = floor(duration/(1000*60))%60
      hours =   floor(duration/(1000*60*60))%24

      if hours > 0: return     f"{hours}h, {minutes}m, {seconds}s"
      elif minutes > 0: return f"{minutes}m, {seconds}s"
      else: return             f"{seconds}s"

    def add_section(self, title, rows, print_links=True):
      html = f"""<h1>{title}</h1>"""
      if len(rows) == 0:
        html += "<p>No records found</p>"
        return html

      html += f"""<table style="border-collapse: collapse; width:100%">"""
      html += self.add_row(self.header_style, "Cloud", "Job", "Version", "Executed", "Duration")

      for row in rows:
        if not print_links:
          link = row["notebook_path"]
        else:
          if row["cloud"] == "AWS": link = f"""<a href="{self.aws_workspace}#job/{row["job_id"]}/run/1" target="_blank">{row["notebook_path"]}</a>"""
          if row["cloud"] == "GCP": link = f"""<a href="{self.gcp_workspace}#job/{row["job_id"]}/run/1" target="_blank">{row["notebook_path"]}</a>"""
          if row["cloud"] == "MSA": link = f"""<a href="{self.msa_workspace}#job/{row["job_id"]}/run/1" target="_blank">{row["notebook_path"]}</a>"""

        html += self.add_row(self.cell_style, row["cloud"], link, row["spark_version"], row["executed_at"], self.format_duration(row["execution_duration"]))
        html += """<tbody></tbody><tbody>"""

      html += "</table>"

      return html


class NotebookDef:
    def __init__(self, round, path, ignored, include_solution, replacements):
      self.round = round
      self.path = path
      self.ignored = ignored
      self.include_solution = include_solution
      self.replacements = replacements


class TestConfig:
    def __init__(self, 
                 name, 
                 version,
                 spark_version = None, 
                 cloud = None,
                 instance_pool = None, 
                 workers = None, 
                 libraries = None, 
                 client = None,
                 source_dir = None,
                 spark_conf = None,
                 results_table = None,
                 results_database = None,
                 ):
      
        import uuid, re, time
        from dbacademy import dbrest
        from dbacademy import dbgems

        self.client = dbrest.DBAcademyRestClient() if client is None else client

        # The instance of this test run
        self.suite_id = str(time.time())+"-"+str(uuid.uuid1())

        # Update the name of the database results will be logged to - convert any special characters to underscores
        results_database = "test_results" if results_database is None else results_database
        results_database = re.sub("[^a-zA-Z0-9]", "_", results_database.lower())
        # Make N passes over the database name to remove duplicate underscores
        for i in range(10): results_database = results_database.replace("__", "_")
        
        # Default the results_table if necissary
        if results_table is None: results_table = "smoke_test_" + name.replace(" ", "_").lower()

        # Update the name of the table results will be logged to
        if "." in results_table: raise ValueError("The results_table should not include the database name")

        # Convert any special characters to underscores
        results_table = re.sub("[^a-zA-Z0-9]", "_", results_table.lower())

        # Make N passes over the table name to remove duplicate underscores
        for i in range(10): results_table = results_table.replace("__", "_")
            
        # Lastly, prefix the database name to the table name and set to the class attribute
        self.results_table = f"{results_database}.{results_table}_csv"

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

        # The name of the cloud on which this tests was ran
        self.cloud = dbgems.get_cloud() if cloud is None else cloud
        
        # The libraries to be attached to the cluster
        self.libraries = [] if libraries is None else libraries

        self.source_dir = f"{dbgems.get_notebook_dir(-2)}/Source" if source_dir is None else source_dir

        self.notebooks = dict()

    def get_distribution_name(self, version):
      distribution_name = f"{self.name}" if version is None else f"{self.name}-v{version}"
      return distribution_name.replace(" ", "-").replace(" ", "-").replace(" ", "-")

    def index_notebooks(self, include_solutions=True):
      assert self.source_dir is not None, "TestConfig.source_dir must be specified"
      entities = self.client.workspace().ls(self.source_dir, recursive=True)

      for entity in entities:
        path = entity["path"][len(self.source_dir)+1:]
        round = 2 # Default round for all notebooks
        round = 0 if path.lower().startswith("includes/") else round
        round = 0 if path.lower().startswith("_includes") else round
        round = 1 if path.lower() in ["includes/reset", "version info"] else round

        if "wip" in path.lower():
          print(f"""** WARNING ** The notebook "{path}" is excluded from the build as a work in progress (WIP)""")
        else:
          excluded_solutions = ["includes/reset", "version info"]
          include_solution = False if path.lower() in excluded_solutions else include_solutions
          self.notebooks[path] = NotebookDef(round=round, path=path, ignored=False, include_solution=include_solution, replacements=dict())

    def print(self):
        print("-"*100)
        print("Test Configuration")
        print(f"suite_id:       {self.suite_id}")
        print(f"name:           {self.name}")
        print(f"spark_version:  {self.spark_version}")
        print(f"workers:        {self.workers}")
        print(f"instance_pool:  {self.instance_pool}")
        print(f"spark_conf:     {self.spark_conf}")
        print(f"cloud:          {self.cloud}")
        print(f"libraries:      {self.libraries}")
        print(f"results_table:  {self.results_table}")
        print(f"source_dir:     {self.source_dir}")

        max_length = 0
        for path in self.notebooks:
          if len(path) > max_length: max_length = len(path)


        if len(self.notebooks) == 0:
          print(f"self.notebooks: none")
        else:
          print(f"self.notebooks: {len(self.notebooks)}")

          rounds = list(map(lambda path: self.notebooks[path].round,  self.notebooks))
          rounds.sort()
          rounds = set(rounds)

          for round in rounds:          
            if round == 0: print("\nRound #0: Published but not tested")
            else: print(f"\nRound #{round}")

            notebook_paths = list(self.notebooks.keys())
            notebook_paths.sort()

            for path in notebook_paths:
              notebook = self.notebooks[path]
              if round == notebook.round:
                path = notebook.path.ljust(max_length)
                ignored = str(notebook.ignored).ljust(5)
                replacements = str(notebook.replacements)
                include_solution = str(notebook.include_solution).ljust(5)
                print(f"  {path}   ignored={ignored}   include_solution={include_solution}   replacements={replacements}")

        print("-"*100)


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


def wait_for_notebooks(client, test_config, jobs, fail_fast):
    for job_name in jobs:
        notebook_path, job_id, run_id, ignored = jobs[job_name]
        print(f"Waiting for {notebook_path}")

        response = client.runs().wait_for(run_id)
        conclude_test(test_config, response, job_name, fail_fast, ignored)
        

def test_one_notebook(client, test_config, job_name, job, fail_fast=False):
  print(f"Starting job for {job[0]}")
  notebook_path, job_id, run_id, ignored = job
  test_notebook(client, test_config, job_name, notebook_path, fail_fast, ignored)
    
    
def test_notebook(client, test_config, job_name, notebook_path, fail_fast, ignored):
    job_id = create_test_job(client, test_config, job_name, notebook_path)
    run_id = client.jobs().run_now(job_id)["run_id"]

    response = client.runs().wait_for(run_id)
    conclude_test(test_config, response, job_name, fail_fast, ignored)


def test_all_notebooks(client, jobs, test_config):
    for job_name in jobs:
        notebook_path, job_id, run_id, ignored = jobs[job_name]
        
        print(f"Starting job for {notebook_path}")

        job_id = create_test_job(client, test_config, job_name, notebook_path)
        run_id = client.jobs().run_now(job_id)["run_id"]

        jobs[job_name] = (notebook_path, job_id, run_id, ignored)
        

def conclude_test(test_config, response, job_name, fail_fast, ignored):
    import json
    log_run(test_config, response, job_name, ignored)

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
        

def log_run(test_config, response, job_name, ignored):
    import traceback, time, uuid, requests, json
    from dbacademy import dbgems
    from pyspark.sql.functions import col, current_timestamp, first

    # noinspection PyBroadException
    try:
        print(f"*** Processing test results to {test_config.results_table}")
              
        job_id = response["job_id"] if "job_id" in response else 0
        run_id = response["run_id"] if "run_id" in response else 0
        
        result_state = response["state"]["result_state"] if "state" in response and "result_state" in response["state"] else "UNKNOWN"
        if result_state == "FAILED" and ignored: result_state = "IGNORED"
        
        execution_duration = response["execution_duration"] if "execution_duration" in response else 0
        notebook_path = response["task"]["notebook_task"]["notebook_path"] if "task" in response and "notebook_task" in response["task"] and "notebook_path" in response["task"][
            "notebook_task"] else "UNKNOWN"

        test_id = str(time.time())+"-"+str(uuid.uuid1())

        test_results = [(test_config.suite_id, test_id, test_config.name, result_state, execution_duration, test_config.cloud, job_name, job_id, run_id, notebook_path, test_config.spark_version)]

        sc, spark, dbutils = dbgems.init_locals()

        # Append our tests results to the database
        (spark.createDataFrame(test_results)
         .toDF("suite_id", "test_id", "name", "status", "execution_duration", "cloud", "job_name", "job_id", "run_id", "notebook_path", "spark_version")
         .withColumn("executed_at", current_timestamp())
         .write.format("csv").mode("append").saveAsTable(test_config.results_table))
        print(f"*** Logged results to {test_config.results_table}")

        response = requests.put("https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod/smoke-tests", data=json.dumps({
          "suite_id": test_config.suite_id, 
          "test_id": test_id, 
          "name": test_config.name, 
          "result_state": result_state, 
          "execution_duration": execution_duration, 
          "cloud": test_config.cloud, 
          "job_name": job_name, 
          "job_id": job_id, 
          "run_id": run_id, 
          "notebook_path": notebook_path, 
          "spark_version": test_config.spark_version
        }))
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        print(f"*** Logged results to REST endpoint")

    except Exception:
        print(f"Unable to log test results.")
        traceback.print_exc()

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

        hash = hashlib.sha256(notebook_path.encode()).hexdigest()
        job_name = f"[TEST] {self.course_name} | {self.test_type} | {hash}"
        self.jobs[job_name] = (notebook_path, 0, 0, ignored)

class TestSuite:
    def __init__(self, test_config, test_dir, test_type):
        import hashlib

        self.test_dir = test_dir
        self.test_config = test_config
        self.client = test_config.client
        self.test_type = test_type
        self.rounds = dict()

        # Define each round first to make the next step full-proof
        for notebook in test_config.notebooks:
          round = test_config.notebooks[notebook].round
          self.rounds[round] = dict()

        # Add each notebook to the dictionary or rounds which is a dictionary of tests
        for notebook in test_config.notebooks:
          round = test_config.notebooks[notebook].round
          ignored = test_config.notebooks[notebook].ignored

          if test_config.notebooks[notebook].include_solution: 
            notebook_path = f"{test_dir}/Solutions/{notebook}"
          else:
            notebook_path = f"{test_dir}/{notebook}"

          if round > 0:
            hash = hashlib.sha256(notebook_path.encode()).hexdigest()
            job_name = f"[TEST] {test_config.name} | {test_type} | {hash}"
            self.rounds[round][job_name] = (notebook_path, 0, 0, ignored)

            if self.client.workspace().get_status(notebook_path) is None:
              raise Exception(f"Notebook not found: {notebook_path}")


    def delete_all_jobs(self, success_only=False):
        for round in self.rounds:
          self.client.jobs().delete_by_name(self.rounds[round], success_only=success_only)
        
    def test_all_synchronously(self, round):
        from dbacademy import dbtest
        for job_name in self.rounds[round]:
            dbtest.test_one_notebook(self.client, self.test_config, job_name, self.rounds[round][job_name])      

    def test_all_asynchronously(self, round, fail_fast=False):
        from dbacademy import dbtest
        dbtest.test_all_notebooks(self.client, self.rounds[round], self.test_config)  
        dbtest.wait_for_notebooks(self.client, self.test_config, self.rounds[round], fail_fast=fail_fast)
