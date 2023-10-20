__all__ = ["TestJobsClient"]

import unittest
from dbacademy.clients import databricks

unit_test_service_principle = "d8835420-9797-45f5-897b-6d81d7f80023"


class TestJobsClient(unittest.TestCase):

    def setUp(self) -> None:
        self.__client = databricks.from_token(scope="DBACADEMY_UNIT_TESTS")
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

    def test_create_git_job(self):
        import json
        from dbacademy.clients.databricks.jobs.job_config_classes import JobConfig
        from dbacademy.clients.databricks.clusters.cluster_config_class import JobClusterConfig
        from dbacademy.dbhelper import WorkspaceHelper
        from dbacademy.common import Cloud

        config = JobConfig(job_name="Job from Git",
                           timeout_seconds=333,
                           max_concurrent_runs=2,
                           tags={"dbacademy.unit-test": "true"})

        config.git_branch(provider="gitHub", url="https://github.com/databricks-academy/workspace-setup.git", branch="published")

        task_config = config.add_task(task_key="Workspace-Setup", description="Just a sample job", timeout_seconds=555)
        task_config.task.notebook("Workspace-Setup", source="GIT", base_parameters={
            WorkspaceHelper.PARAM_EVENT_ID: "Testing 123",
            WorkspaceHelper.PARAM_EVENT_DESCRIPTION: "This is a test of the Emergency Broadcast System. This is only a test.",
            WorkspaceHelper.PARAM_POOLS_NODE_TYPE_ID: "i3.xlarge",
            WorkspaceHelper.PARAM_DEFAULT_SPARK_VERSION: "11.3.x-scala2.12"
        })
        task_config.cluster.new(JobClusterConfig(cloud=Cloud.AWS,
                                                 spark_version="11.3.x-scala2.12",
                                                 node_type_id="i3.xlarge",
                                                 num_workers=0,
                                                 single_user_name="jacob.parr@databricks.com",
                                                 autotermination_minutes=None))

        job_id = self.client.jobs.create_from_config(config)
        job = self.client.jobs.get_by_id(job_id)

        self.assertEquals(job_id, job.get("job_id"))
        self.assertEquals(unit_test_service_principle, job.get("creator_user_name"))
        self.assertEquals(unit_test_service_principle, job.get("run_as_user_name"))
        self.assertEquals(True, job.get("run_as_owner"))

        settings = job.get("settings")
        self.assertEquals("Job from Git", settings.get("name"))
        self.assertEquals(2, settings.get("max_concurrent_runs"))
        self.assertEquals("SINGLE_TASK", settings.get("format"))

        self.assertEquals({}, settings.get("email_notifications"))
        self.assertEquals({}, settings.get("webhook_notifications"))

        notebook_task = settings.get("notebook_task")
        self.assertIsNotNone(notebook_task)
        self.assertEquals("Workspace-Setup", notebook_task.get("notebook_path"))
        self.assertEquals("GIT", notebook_task.get("source"))

        base_parameters = {
            "event_id": "Testing 123",
            "event_description": "This is a test of the Emergency Broadcast System. This is only a test.",
            "pools_node_type_id": "i3.xlarge",
            "default_spark_version": "11.3.x-scala2.12"
        }
        self.assertEquals(base_parameters, notebook_task.get("base_parameters"))

        git_source = settings.get("git_source")
        self.assertEquals("https://github.com/databricks-academy/workspace-setup.git", git_source.get("git_url"))
        self.assertEquals("gitHub", git_source.get("git_provider"))
        self.assertEquals("published", git_source.get("git_branch"))

        self.assertEquals({"dbacademy.unit-test": "true"}, settings.get("tags"))

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
            self.assertEquals(expected_cluster, actual_cluster)


if __name__ == "__main__":
    unittest.main()
