import unittest

unit_test_service_principle = "d8835420-9797-45f5-897b-6d81d7f80023"


class TestJobsClient(unittest.TestCase):

    def setUp(self) -> None:
        import os
        from dbacademy.dbrest import DBAcademyRestClient

        token = os.getenv("DBACADEMY_UNIT_TESTS_API_TOKEN", None)
        endpoint = os.getenv("DBACADEMY_UNIT_TESTS_API_ENDPOINT", None)

        if token is None or endpoint is None:
            self.skipTest("Missing DBACADEMY_UNIT_TESTS_API_TOKEN or DBACADEMY_UNIT_TESTS_API_ENDPOINT environment variables")

        self.__client = DBAcademyRestClient(token=token, endpoint=endpoint)

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
        from dbacademy.dbrest.jobs import JobConfig
        from dbacademy.dbrest.clusters import ClusterConfig
        from dbacademy.dbhelper import WorkspaceHelper

        config = JobConfig(job_name="Job from Git",
                           timeout_seconds=333,
                           max_concurrent_runs=2,
                           tags={"dbacademy.unit-test": "true"})

        config.git_branch(provider="gitHub", url="https://github.com/databricks-academy/workspace-setup.git", branch="published")

        task_config = config.add_task(task_key="Workspace-Setup", description="Just a sample job", timeout_seconds=555)
        task_config.task.notebook("Workspace-Setup", source="GIT", base_parameters={
            WorkspaceHelper.PARAM_LAB_ID: "Testing 123",
            WorkspaceHelper.PARAM_DESCRIPTION: "This is a test of the Emergency Broadcast System. This is only a test.",
            WorkspaceHelper.PARAM_NODE_TYPE_ID: "i3.xlarge",
            WorkspaceHelper.PARAM_SPARK_VERSION: "11.3.x-scala2.12"
        })
        task_config.cluster.new(ClusterConfig(cluster_name=None,
                                              spark_version="11.3.x-scala2.12",
                                              node_type_id="i3.xlarge",
                                              single_user_name=unit_test_service_principle,
                                              num_workers=0,
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
        self.assertEquals({"color": "red"}, notebook_task.get("base_parameters"))

        git_source = settings.get("git_source")
        self.assertEquals("https://github.com/databricks-academy/workspace-setup.git", git_source.get("git_url"))
        self.assertEquals("gitHub", git_source.get("git_provider"))
        self.assertEquals("published", git_source.get("git_branch"))

        self.assertEquals({"dbacademy.unit-test": "true"}, settings.get("tags"))

        new_cluster = settings.get("new_cluster")
        self.assertIsNotNone(new_cluster)

        pass


if __name__ == '__main__':
    unittest.main()
