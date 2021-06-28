# Databricks notebook source
class TestConfig:
    def __init__(self, name, spark_version, workers, instance_pool, libraries, results_table, results_database="test_results"):
        import uuid, re

        # The instance of this test run
        self.suite_id = str(uuid.uuid1())

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
        self.results_table = f"{results_database}.{results_table}"


        # Course Name
        self.name = name

        # The runtime you wish to test against
        self.spark_version = spark_version

        # We can use local-mode clusters here
        self.workers = workers

        # The instance pool from which to obtain VMs
        self.instance_pool = instance_pool

        # The libraries to be attached to the cluster
        self.libraries = libraries

        # The spark table to which results will be appended
        self.results_table = results_table

    def print(self):
        print(f"suite_id:      {self.suite_id}")
        print(f"results_table: {self.results_table}")
        print(f"name:          {self.name}")
        print(f"spark_version: {self.spark_version}")
        print(f"workers:       {self.workers}")
        print(f"instance_pool: {self.instance_pool}")
        print(f"libraries:     {self.libraries}")
        print(f"results_table: {self.results_table}")


def create_test_job(test_config, job_name, notebook_path):
    from dbacademy.dbrest import DBAcademyRestClient

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
    client = DBAcademyRestClient()
    json_response = client.jobs().create(params)
    return json_response["job_id"]


def wait_for_notebooks(test_config, jobs, fail_fast):
    from dbacademy.dbrest import DBAcademyRestClient

    for job_name in jobs:
        notebook_path, job_id, run_id = jobs[job_name]
        print(f"Waiting for {notebook_path}")

        response = DBAcademyRestClient().runs().wait_for(run_id)
        conclude_test(test_config, response, job_name, fail_fast)


def test_notebook(test_config, job_name, notebook_path, fail_fast):
    from dbacademy.dbrest import DBAcademyRestClient

    job_id = create_test_job(test_config, job_name, notebook_path)
    run_id = DBAcademyRestClient().jobs().run_now(job_id)["run_id"]

    response = DBAcademyRestClient().runs().wait_for(run_id)
    conclude_test(test_config, response, job_name, fail_fast)


def test_all_notebooks(jobs, test_config):
    from dbacademy.dbrest import DBAcademyRestClient

    for job_name in jobs:
        notebook_path, job_id, run_id = jobs[job_name]
        print(f"Starting job for {notebook_path}")

        job_id = create_test_job(test_config, job_name, notebook_path)
        run_id = DBAcademyRestClient().jobs().run_now(job_id)["run_id"]

        jobs[job_name] = (notebook_path, job_id, run_id)


def conclude_test(test_config, response, job_name, fail_fast):
    import json
    log_run(test_config, response, job_name)

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


def log_run(test_config, response, job_name):
    import traceback
    from dbacademy import dbgems
    from pyspark.sql.functions import current_timestamp

    # noinspection PyBroadException
    try:
        job_id = response["job_id"] if "job_id" in response else 0
        run_id = response["run_id"] if "run_id" in response else 0
        result_state = response["state"]["result_state"] if "state" in response and "result_state" in response["state"] else "UNKNOWN"
        execution_duration = response["execution_duration"] if "execution_duration" in response else 0
        notebook_path = response["task"]["notebook_task"]["notebook_path"] if "task" in response and "notebook_task" in response["task"] and "notebook_path" in response["task"][
            "notebook_task"] else "UNKNOWN"

        test_results = [(test_config.suite_id, test_config.name, result_state, execution_duration, job_name, job_id, run_id, notebook_path, test_config.spark_version)]

        sc, spark, dbutils = dbgems.init_locals()

        (spark.createDataFrame(test_results)
         .toDF("suite_id", "name", "status", "execution_duration", "job_name", "job_id", "run_id", "notebook_path", "spark_version")
         .withColumn("executed_at", current_timestamp())
         .write
         .format("delta")
         .mode("append")
         .saveAsTable(test_config.results_table))
        print(f"Logged results to {test_config.results_table}")

    except Exception:
        print(f"Unable to log test results.")
        traceback.print_exc()
