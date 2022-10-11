import unittest


class MyTestCase(unittest.TestCase):

    def test_lookup_github_tags(self):
        from dbacademy_gems import dbgems

        module = "dbacademy-gems"
        tags = dbgems.lookup_github_tags(module)
        self.assertEqual(['1.0.0', '1.1.0', '1.1.1'], tags)

        module = "dbacademy-rest"
        tags = dbgems.lookup_github_tags(module)
        self.assertEqual(['1.0.0', '1.1.0', '1.1.1'], tags)

        module = "dbacademy-helper"
        tags = dbgems.lookup_github_tags(module)
        self.assertEqual(['1.0.0', '1.1.0', '1.1.1'], tags)

    def test_dbacademy_refactor(self):
        import dbacademy.dbgems
        from dbacademy_gems import dbgems
        from inspect import getmembers

        expected_members = ['get_browser_host_name', 'get_cloud', 'get_current_instance_pool_id', 'get_current_node_type_id', 'get_current_spark_version', 'get_dbutils', 'get_job_id', 'get_notebook_dir', 'get_notebook_name', 'get_notebook_path', 'get_notebooks_api_endpoint', 'get_notebooks_api_token', 'get_parameter', 'get_session_context', 'get_spark_session', 'get_tag', 'get_tags', 'get_username', 'get_workspace_id', 'is_job', 'proof_of_life', 'deprecation_logging_enabled', 'print_warning']

        old_members = [t[0] for t in getmembers(dbacademy.dbgems) if not t[0].startswith("__")]
        new_members = [t[0] for t in getmembers(dbgems) if not t[0].startswith("__")]

        for expected in expected_members:
            self.assertTrue(expected in old_members, f"The package member {expected} was not found in dbgems: {old_members}")
            self.assertTrue(expected in new_members, f"The package member {expected} was not found in dbacademy_gems: {new_members}")

        # Add dbacademy_gems to expected as it's
        # a side effect of the implementation
        expected_members.append("dbacademy_gems")

        for old_member in old_members:
            self.assertTrue(old_member in expected_members, f"Found extra member {old_member} in dbgems: {expected_members}")

    # def test_get_parameter(self):
    #     from dbgems import get_parameter
    #
    #     param = get_parameter("whatever", "missing")
    #     self.assertEqual("moo", param)

    # def test_get_current_spark_version(self):
    #     from dbgems import get_current_spark_version
    #     version = get_current_spark_version()
    #     self.assertEqual("moo", version)


if __name__ == '__main__':
    unittest.main()
