__all__ = ["TestJobsClient"]

import unittest


class TestJobsClient(unittest.TestCase):

    def setUp(self) -> None:
        from dbacademy.clients import darest
        from dbacademy_test.clients.darest import DBACADEMY_UNIT_TESTS

        self.__client = darest.from_token(scope=DBACADEMY_UNIT_TESTS)
        self.tearDown()

    def tearDown(self) -> None:
        pass
        jobs = self.client.jobs.list(expand_tasks=False)
        for job in jobs:
            job_id = job.get("job_id")
            self.client.jobs.delete_by_id(job_id)

    @property
    def client(self):
        return self.__client

    def test_get_by_name(self):
        orig_job = self.__create_job()
        orig_job_id = orig_job.get("job_id")
        orig_name = orig_job.get("settings", dict()).get("name")

        job = self.client.jobs.get_by_name(orig_name)
        self.assertIsNotNone(job)
        self.assertEqual(orig_job_id, job.get("job_id"))
        self.assertEqual(orig_name, orig_job.get("settings", dict()).get("name"))

    def test_get_by_id(self):
        orig_job = self.__create_job()
        orig_job_id = orig_job.get("job_id")

        job = self.client.jobs.get_by_id(orig_job_id)
        self.assertIsNotNone(job)
        self.assertEqual(orig_job_id, job.get("job_id"))

    def __create_job(self):
        from dbacademy.clients.darest.jobs_api.job_config import JobConfig
        from dbacademy.clients.darest.clusters_api.cluster_config import JobClusterConfig
        from dbacademy.common import Cloud
        from dbacademy.dbhelper import dbh_constants

        config = JobConfig(job_name="Job from Git",
                           timeout_seconds=333,
                           max_concurrent_runs=2,
                           tags={"dbacademy.unit-test": "true"})

        config.git_branch(provider="gitHub", url="https://github.com/databricks-academy/workspace-setup.git", branch="published")

        task_config = config.add_task(task_key="Workspace-Setup", description="Just a sample job", timeout_seconds=555)
        task_config.task.notebook("Workspace-Setup", source="GIT", base_parameters={
            dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_ID: "Testing 123",
            dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_DESCRIPTION: "This is a test of the Emergency Broadcast System. This is only a test.",
            dbh_constants.WORKSPACE_HELPER.PARAM_POOLS_NODE_TYPE_ID: "i3.xlarge",
            dbh_constants.WORKSPACE_HELPER.PARAM_DEFAULT_SPARK_VERSION: "11.3.x-scala2.12"
        })
        task_config.cluster.new(JobClusterConfig(cloud=Cloud.AWS,
                                                 spark_version="11.3.x-scala2.12",
                                                 node_type_id="i3.xlarge",
                                                 num_workers=0,
                                                 single_user_name="jacob.parr@databricks.com",
                                                 autotermination_minutes=None))

        job_id = self.client.jobs.create_from_config(config)
        return self.client.jobs.get_by_id(job_id)

    def test_create_git_job(self):
        import json
        from dbacademy_test.clients.darest import UNIT_TEST_SERVICE_PRINCIPLE

        job = self.__create_job()
        job_id = job.get("job_id")

        self.assertEqual(job_id, job.get("job_id"))
        self.assertEqual(UNIT_TEST_SERVICE_PRINCIPLE, job.get("creator_user_name"))
        self.assertEqual(UNIT_TEST_SERVICE_PRINCIPLE, job.get("run_as_user_name"))
        self.assertEqual(True, job.get("run_as_owner"))

        settings = job.get("settings")
        self.assertEqual("Job from Git", settings.get("name"))
        self.assertEqual(2, settings.get("max_concurrent_runs"))
        self.assertEqual("SINGLE_TASK", settings.get("format"))

        self.assertEqual({}, settings.get("email_notifications"))
        self.assertEqual({}, settings.get("webhook_notifications"))

        notebook_task = settings.get("notebook_task")
        self.assertIsNotNone(notebook_task)
        self.assertEqual("Workspace-Setup", notebook_task.get("notebook_path"))
        self.assertEqual("GIT", notebook_task.get("source"))

        base_parameters = {
            "event_id": "Testing 123",
            "event_description": "This is a test of the Emergency Broadcast System. This is only a test.",
            "pools_node_type_id": "i3.xlarge",
            "default_spark_version": "11.3.x-scala2.12"
        }
        self.assertEqual(base_parameters, notebook_task.get("base_parameters"))

        git_source = settings.get("git_source")
        self.assertEqual("https://github.com/databricks-academy/workspace-setup.git", git_source.get("git_url"))
        self.assertEqual("gitHub", git_source.get("git_provider"))
        self.assertEqual("published", git_source.get("git_branch"))

        self.assertEqual({"dbacademy.unit-test": "true"}, settings.get("tags"))

        actual_cluster = settings.get("new_cluster")
        self.assertIsNotNone(actual_cluster)
        expected_cluster = {
            "spark_version": "11.3.x-scala2.12",
            "spark_conf": {
                "spark.master": "local[*]",
                "spark.databricks.cluster.profile": "singleNode"
            },
            "aws_attributes": {
                "availability": "ON_DEMAND",
                "zone_id": "us-west-2b"
            },
            "node_type_id": "i3.xlarge",
            "custom_tags": {
                "ResourceClass": "SingleNode"
            },
            "enable_elastic_disk": False,
            "single_user_name": "jacob.parr@databricks.com",
            "data_security_mode": "SINGLE_USER",
            "num_workers": 0
        }
        if expected_cluster != actual_cluster:
            print("-"*100)
            print(json.dumps(expected_cluster, indent=4))
            print("-"*100)
            print(json.dumps(actual_cluster, indent=4))
            self.assertEqual(expected_cluster, actual_cluster)


if __name__ == "__main__":
    unittest.main()
