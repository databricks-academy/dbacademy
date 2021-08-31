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
        
    def to_html(self) -> int:
      html = "</body>"
      html += self.add_section("Failed", self.failed_set)
      html += self.add_section("Ignored", self.ignored_set)
      html += self.add_section("Success", self.success_set, links=False)
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

    def add_section(self, title, rows, links=True):
      html = f"""<h1>{title}</h1>"""
      if len(rows) == 0:
        html += "<p>No records found</p>"
        return html

      html += f"""<table style="border-collapse: collapse; width:100%">"""
      html += self.add_row(self.header_style, "Cloud", "Job", "Version", "Executed", "Duration")

      for row in rows:
        if not links:
          link = row["notebook_path"]
        else:
          if row["cloud"] == "AWS": link = f"""<a href="{self.aws_workspace}#job/{row["job_id"]}/run/1" target="_blank">{row["notebook_path"]}</a>"""
          if row["cloud"] == "GCP": link = f"""<a href="{self.gcp_workspace}#job/{row["job_id"]}/run/1" target="_blank">{row["notebook_path"]}</a>"""
          if row["cloud"] == "MSA": link = f"""<a href="{self.msa_workspace}#job/{row["job_id"]}/run/1" target="_blank">{row["notebook_path"]}</a>"""

        html += self.add_row(self.cell_style, row["cloud"], link, row["spark_version"], row["executed_at"], f"""{row["execution_duration"]:,d} ms""")
        html += """<tbody></tbody><tbody>"""

      html += "</table>"

      return html


class TestConfig:
    def __init__(self, 
                 name, 
                 spark_version, 
                 workers, 
                 cloud,
                 instance_pool, 
                 libraries, 
                 results_table,
                 results_database="test_results",
                 ):
      
        import uuid, re, time

        # The instance of this test run
        self.suite_id = str(time.time())+"-"+str(uuid.uuid1())

        # Update the name of the database results will be logged to - convert any special characters to underscores
        results_database = re.sub("[^a-zA-Z0-9]", "_", results_database.lower())
        # Make N passes over the database name to remove duplicate underscores
        for i in range(10): results_database = results_database.replace("__", "_")
        
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

        # The runtime you wish to test against
        self.spark_version = spark_version

        # We can use local-mode clusters here
        self.workers = workers

        # The instance pool from which to obtain VMs
        self.instance_pool = instance_pool

        # The name of the cloud on which this tests was ran
        self.cloud = cloud
        
        # The libraries to be attached to the cluster
        self.libraries = libraries

    def print(self):
        print(f"suite_id:      {self.suite_id}")
        print(f"name:          {self.name}")
        print(f"spark_version: {self.spark_version}")
        print(f"workers:       {self.workers}")
        print(f"instance_pool: {self.instance_pool}")
        print(f"cloud:         {self.cloud}")
        print(f"libraries:     {self.libraries}")
        print(f"results_table: {self.results_table}")


def create_test_job(client, test_config, job_name, notebook_path):
    spark_conf = {"spark.master": "local[*]"} if test_config.workers == 0 else dict()

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
            "spark_conf": spark_conf
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
    import traceback, time, uuid
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

    except Exception:
        print(f"Unable to log test results.")
        traceback.print_exc()

class SuiteBuilder:
    def __init__(self, client, course_name, test_type):
        self.client = client
        self.course_name = course_name
        self.test_type = test_type
        self.jobs = dict()

    def add(self, notebook_path, ignored=False):
        import hashlib
        hash = hashlib.sha256(notebook_path.encode())

        job_name = f"[TEST] {self.course_name} | {self.test_type} | {hash}"
        self.jobs[job_name] = (notebook_path, 0, 0, ignored)

