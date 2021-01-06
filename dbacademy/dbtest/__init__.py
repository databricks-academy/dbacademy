from dbacademy.dbrest import *


class TestConfig:
    def __init__(self, sku, spark_version, workers, instance_pool, libraries, results_table):
        import uuid

        # The instance of this test run
        self.suite_id = str(uuid.uuid1()),

        # The name of the table results will be logged to
        self.results_table = "test_results.apache_spark_programming_capstone",

        # Course Name
        self.sku = sku

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


def create_test_job(test_config, job_name, notebook_path):
    spark_version = test_config.spark_version
    workers = test_config.workers
    instance_pool = test_config.instance_pool
    libraries = test_config.libraries

    spark_conf = {"spark.master": "local[*]"} if workers == 0 else dict()

    params = {
        "notebook_task": {
            "notebook_path": f"{notebook_path}",
        },
        "name": f"{job_name}",
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "email_notifications": {},
        "libraries": libraries,
        "new_cluster": {
            "num_workers": workers,
            "instance_pool_id": f"{instance_pool}",
            "spark_version": f"{spark_version}",
            "spark_conf": spark_conf
        }
    }
    client = DBAcademyRestClient()
    json_response = client.jobs().create(params)
    return json_response["job_id"]


def wait_for_run(run_id):
    import time

    wait = 60
    response = DBAcademyRestClient().runs().get(run_id)
    state = response["state"]["life_cycle_state"]

    if state != "TERMINATED" and state != "INTERNAL_ERROR":
        if state == "PENDING" or state == "RUNNING":
            print(f" - Run #{run_id} is {state}, checking again in {wait} seconds")
            time.sleep(wait)
        else:
            print(f" - Run #{run_id} is {state}, checking again in 5 seconds")
            time.sleep(5)

        return wait_for_run(run_id)

    return response


def wait_for_notebooks(test_config, jobs, fail_fast):
    for job_name in jobs:
        notebook_path, job_id, run_id = jobs[job_name]
        print(f"Waiting for {notebook_path}")

        response = wait_for_run(run_id)
        conclude_test(test_config, response, job_name, fail_fast)


def test_notebook(test_config, job_name, notebook_path, fail_fast):
    job_id = create_test_job(test_config, job_name, notebook_path)
    run_id = DBAcademyRestClient().jobs().run_now(job_id)["run_id"]

    response = wait_for_run(run_id)
    conclude_test(test_config, response, job_name, fail_fast)


def test_all_notebooks(jobs, test_config):
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
    from pyspark.sql.functions import current_timestamp

    suite_id = test_config["suite_id"]
    sku = test_config["sku"]
    spark_version = test_config["spark_version"]

    # noinspection PyBroadException
    try:
        job_id = response["job_id"] if "job_id" in response else 0
        run_id = response["run_id"] if "run_id" in response else 0
        result_state = response["state"]["result_state"] if "state" in response and "result_state" in response["state"] else "UNKNOWN"
        execution_duration = response["execution_duration"] if "execution_duration" in response else 0
        notebook_path = response["task"]["notebook_task"]["notebook_path"] if "task" in response and "notebook_task" in response["task"] and "notebook_path" in response["task"][
            "notebook_task"] else "UNKNOWN"

        test_results = [(suite_id, sku, result_state, execution_duration, job_name, job_id, run_id, notebook_path, spark_version)]
        results_table = test_config["results_table"]

        sc, spark, dbutils = dbgems.init_locals()

        (spark.createDataFrame(test_results)
         .toDF("suite_id", "sku", "status", "execution_duration", "job_name", "job_id", "run_id", "notebook_path", "spark_version")
         .withColumn("executed_at", current_timestamp())
         .write
         .format("delta")
         .mode("append")
         .saveAsTable(results_table))
        print(f"Logged results to {results_table}")

    except Exception:
        print(f"Unable to log test results.")
        traceback.print_exc()
